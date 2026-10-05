# ADR-002: A move is complete by default, including imported subagent sessions

**Status:** Accepted
**Date:** 2026-10-05
**Applies to:** move planning, target cleanup

## Context

ADR-001 soft-deletes target threads that "import recent sessions" created for
the same provider sessions as the moved threads. A real move showed that this
leaves most of the import noise behind. Codex threads spawn subagents, and
each subagent is a separate Codex session with its own rollout. The target's
import turns every one of them into a top-level thread, titled after injected
instructions rather than a user prompt. In that move, imported subagent
sessions outnumbered the moved threads two to one. Users expect a move to
leave the target looking like the source project: the moved threads, without
copies of their sessions.

T3 Code does not record which Codex session spawned a subagent. Its import
leaves the lineage empty, and legacy threads keep only their own session id.
Codex records the parent in the `session_meta` line at the start of the
child's rollout, under `source.subagent.thread_spawn.parent_thread_id`.

A rerun of a completed move also skipped import cleanup, because cleanup
considered only threads copied in that run.

## Decision

- A move is complete by default. Besides the duplicates of ADR-001, it
  soft-deletes target imports of Codex sessions that a moved thread's session
  spawned, directly or through other subagents. It applies the same rules as
  for duplicates: only untouched imports (no runs and no live session) are
  deleted, and anything else is kept with a warning.
- t3rry reads only the first line of the candidate sessions' rollouts under
  the Codex home (`--codex-home`, default `$CODEX_HOME` or `~/.codex`). It
  never writes provider data. A missing Codex home or rollout is a warning,
  not a blocker.
- Candidates are untouched Codex imports in the moved projects' target
  projects. Sessions not spawned by a moved session, such as standalone
  `codex exec` runs, are left alone.
- Import cleanup covers every selected thread, including threads an earlier
  run already moved. `move` runs a cleanup-only target transaction, with a
  backup, when nothing else remains, so a rerun completes an older move.
- Narrower moves stay available: `--keep-duplicates` keeps same-session
  imports and `--keep-subagent-imports` keeps imported subagent sessions.

## Alternatives and consequences

- **Reading Codex's state database** (`state_<n>.sqlite`, with
  `thread_spawn_edges`): faster, but its name and schema change between Codex
  releases. The rollout `session_meta` line is Codex's persisted session
  format and is present for every session, so t3rry depends on that instead.
- **Deleting every untouched import in the moved projects:** simpler, but it
  would also remove sessions that never belonged to a moved thread.
- **Fixing T3 Code's import to skip subagent sessions:** preferable upstream,
  and complementary. It would not clean up targets that already imported
  them.
- Claude subagents are not handled: T3 Code's import has not been seen to
  create threads for them. If it does, the same rule can be extended to
  Claude's transcript metadata.
