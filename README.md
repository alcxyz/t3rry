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
   same provider sessions;
6. archives the moved threads in the source, so only one server drives each
   provider session.

Checkpoints are git refs in the shared workspace, and provider transcripts
live in the shared `~/.claude` and `~/.codex` directories, so neither needs
copying on the same host. Projects are matched by real path, so a symlinked
workspace root matches its physical path. See
[ADR-001](docs/adr/ADR-001-offline-row-level-thread-moves.md) for the design.

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
  skipped, and a rerun finishes any source cleanup that failed before. A
  thread that changed in the source since then blocks the move, and so does
  moving a thread straight back onto its archived original: unarchive the
  original in T3 Code instead.
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
  the source. Project-level scheduled tasks stay in the source.
- Command receipts, effect history, legacy (pre-v2) tables and checkpoint diff
  caches are not copied. Threads whose legacy transcript was never opened in
  the source are blocked until it is.
- Only the target is backed up. Source changes are limited to archiving and
  can be undone by unarchiving.

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
