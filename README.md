# t3rry

t3rry moves [T3 Code](https://github.com/pingdotgg/t3code) projects between
two T3 Code servers on the same host, together with their threads, history,
plans, checkpoints and attachments.

Several T3 Code servers can run on one machine, each with its own base
directory, for example one per client or organisation. T3 Code itself cannot
move a thread or project from one server to another. Its "import recent
sessions" feature rebuilds threads from provider transcripts only. It loses
titles, plans, pull request links and checkpoints, and it duplicates threads
that already exist elsewhere. t3rry moves the original threads instead.

## How it works

T3 Code keeps a server's state in `<base-dir>/userdata/statev2.sqlite` and its
uploaded files in `<base-dir>/userdata/attachments/`. For every selected
project, t3rry:

1. selects every non-deleted thread of the project, plus every thread linked
   to it by lineage: subagents, forks and context transfers;
2. appends the threads' events to the target's event log in their original
   order, and copies their projection rows, changing only the project id;
3. advances the target's projection cursor, so the server starts without
   rebuilding anything;
4. copies referenced attachment files without overwriting anything;
5. soft-deletes target threads that "import recent sessions" created for the
   same provider sessions, and for the Codex subagent sessions they spawned;
6. archives the moved threads in the source, so only one server drives each
   provider session.

Checkpoints are git refs in the shared workspace, and provider transcripts
live in the shared `~/.claude` and `~/.codex` directories, so neither needs
copying on the same host. Projects are matched by real path, so a symlinked
workspace root matches its physical path. See
[ADR-001](docs/adr/ADR-001-offline-row-level-thread-moves.md) for the design.

A move is complete by default: the target ends up with the moved threads and
without imported copies of their sessions. Codex records which session spawned
a subagent only in the first line of the subagent's rollout, so t3rry reads
that line under the Codex home. It never writes provider data. Imports that
have their own runs or a live session are always kept. See
[ADR-002](docs/adr/ADR-002-complete-moves-remove-subagent-imports.md).

## Safety model

- **Offline only.** t3rry refuses to run while either server appears to be
  running: `server-runtime.json` names a live process, or (on Linux) any
  process holds the state database open.
- **Exact schemas only.** Both databases must match a schema t3rry knows
  exactly: the migration ledger, the projection schema version, and the
  columns of every table it reads or writes. Anything else fails closed until
  t3rry adds support. `t3rry version` lists the supported schemas.
- **Plan first.** `plan` is the default command and only reads. `move` repeats
  the plan and needs `--yes`.
- **Blockers.** A move is refused when a selected thread still has unfinished
  runs, pending approvals or input requests, active provider threads,
  background tasks or queued server effects; when a thread links to a project
  outside the move; when any thread, event or row id already exists in the
  target; or when the target lacks a matching project.
- **One transaction.** The target is written inside a single
  `BEGIN IMMEDIATE` transaction after re-planning under the lock. Every table
  is checked against the planned row counts before commit.
- **Backup.** Before writing, t3rry copies the target database with
  `VACUUM INTO`, and its attachments directory, to
  `<to>/userdata/t3rry-backups/<timestamp>/` (or `--backup-dir`).
- **Repeatable.** Threads copied by an earlier run are recognised and
  skipped, and a rerun finishes any source cleanup that failed before. It
  also soft-deletes imports of the moved sessions that the target gained
  since, or that an older t3rry release left behind. A
  thread that changed in the source since then blocks the move, and so does
  moving a thread back onto the original t3rry archived when it moved it
  away: unarchive the original in T3 Code instead.
- **Backup location.** The backup directory may not lie inside the target's
  attachments directory, even through a symlink.

The source is changed only after the target commit succeeds, in its own
transaction. Its moved threads are archived exactly as T3 Code's own archive
command does, so they can be unarchived later.

## Usage

Stop both servers. With systemd user services this might look like:

```sh
systemctl --user stop t3code.service t3code-work.service
```

Check both base directories, then review the plan:

```sh
t3rry check --base-dir ~/.t3
t3rry check --base-dir ~/.t3-work
t3rry plan --from ~/.t3 --to ~/.t3-work
```

Without `--project`, every source project whose workspace also exists as a
project in the target is selected. Select projects explicitly by source or
target workspace path:

```sh
t3rry plan --from ~/.t3 --to ~/.t3-work --project ~/src/acme/api
```

If the target has no project for a workspace, the plan prints the command
that creates one, for example:

```sh
t3 project add ~/src/acme/api --base-dir ~/.t3-work
```

When the plan has no blockers, perform the move and restart both servers:

```sh
t3rry move --from ~/.t3 --to ~/.t3-work --project ~/src/acme/api --yes
systemctl --user start t3code.service t3code-work.service
```

### Options

| Option | Commands | Meaning |
| --- | --- | --- |
| `--from`, `--to` | plan, move | Source and target base directories; `~` is expanded |
| `--project <path>` | plan, move | Workspace path of a project to move; repeatable |
| `--include-deleted` | plan, move | Also move deleted threads |
| `--yes` | move | Perform the move |
| `--keep-duplicates` | move | Keep target threads imported from the same sessions |
| `--keep-subagent-imports` | move | Keep target threads imported from subagent sessions of moved threads |
| `--codex-home <dir>` | plan, move | Codex home with session rollouts; default `$CODEX_HOME` or `~/.codex` |
| `--no-archive-source` | move | Leave moved threads unarchived in the source |
| `--backup-dir <dir>` | move | Where to write the target backup |
| `--base-dir <dir>` | check | Base directory to check |

`plan` and `check` exit with status 1 when something blocks a move, and
usage errors exit with status 2.

## Limitations

- Same host only. Moving between hosts would also need path rewriting,
  provider transcripts and attachment grants; see ADR-001.
- Both servers must run the same supported T3 Code schema. New T3 Code
  releases that change storage need a t3rry update first.
- Projects are not created or moved; the target project must exist.
- Worktrees stay where they are. Threads that use worktrees under the source
  base directory keep pointing there, and the source server's worktree
  cleanup may remove them; the plan warns about such threads.
- Scheduled tasks bound to moved threads move with them and are disabled in
  the source. Tasks of deleted threads arrive disabled and stay unchanged in
  the source. Project-level scheduled tasks stay in the source.
- Threads whose legacy transcript was never opened in the source are blocked
  until it is. See [What is not copied](#what-is-not-copied).
- Only the target is backed up. Source changes are limited to archiving and
  can be undone by unarchiving.

## What is not copied

t3rry copies a thread's v2 events and its `orchestration_v2_projection_*`,
`orchestration_v2_turn_item_positions` and session binding rows, plus
scheduled tasks bound to it. It deliberately leaves these tables behind:

| Table | Why it stays |
| --- | --- |
| `provider_session_runtime` | Only "import recent sessions" reads it; t3rry reads it to find duplicate imports. |
| `orchestration_v2_events` | Unused at this schema; v2 events live in `orchestration_events`. |
| `projection_threads`, `projection_thread_messages`, `projection_thread_activities`, `projection_thread_sessions`, `projection_turns`, `projection_pending_approvals`, `projection_thread_proposed_plans`, `projection_thread_pull_requests`, `projection_state` | Legacy v1 read models. The v2 server reads them only to import v1 threads it has not imported yet, and blocks such threads. Pull request links of v2 threads live in the thread itself. |
| v1 thread events (`application_event_version = 1`) | Already converted into the thread's v2 events. |
| `orchestration_v2_thread_launch_workflows` | Not read by the server at this schema. |
| `orchestration_command_receipts`, `orchestration_v2_command_receipts` | Deduplicate retried client commands on the server that received them. |
| `orchestration_v2_effect_outbox` | Server side effects; pending or running effects block the move, finished ones are history. |
| `orchestration_v2_legacy_imports` | Tracks v1 imports in the source; a moved thread's imported history is already in its events. |
| `checkpoint_diff_blobs` | A v1 checkpoint diff cache the server no longer reads at this schema. |
| `auth_*`, `pull_request_files_viewed` | Per-server pairing, sessions and review state, not thread data. |

When t3rry cleans up a source, it marks each moved thread's original with a
`thread.archived` event whose metadata carries a source-archive marker, even
when the thread was already archived or deleted. Later moves copy that event
like any other but strip the marker, so a marker only ever means "this server
held the original", and t3rry refuses to move a thread back onto it.

## Installation

```sh
go install github.com/alcxyz/t3rry/cmd/t3rry@latest
```

or with Nix:

```sh
nix run github:alcxyz/t3rry -- plan --from ~/.t3 --to ~/.t3-work
```

## License

MIT
