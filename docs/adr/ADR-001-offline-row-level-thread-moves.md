# ADR-001: Move threads offline by copying events and projections

**Status:** Accepted
**Date:** 2026-10-05
**Applies to:** move planning, SQLite access, schema support

## Context

T3 Code keeps each server's state under a base directory
(`<base-dir>/userdata/statev2.sqlite` plus `attachments/`). Several servers
can run on one host, for example one per client or organisation, but T3 Code
cannot move a thread or project between them. Its "import recent sessions"
feature only rebuilds threads from Claude/Codex transcripts. It loses titles,
plans, PR links and checkpoints, and creates duplicates of native threads.

At the supported schema, `orchestration_events` is the authoritative log, but
the server never replays it. Events and their projection rows are written in
one transaction, and missing projection rows simply mean "thread not found".
Thread, event and most projection identifiers are opaque global strings that
stay valid on another server. Project identifiers differ per server.
Checkpoints are git refs in the shared workspace repository, and provider
transcripts live in the shared `~/.claude` and `~/.codex` directories.

## Decision

- t3rry moves whole projects, meaning every non-deleted thread plus its
  lineage closure, between two base directories on the same host. It works
  only while neither server is running.
- It copies rows rather than reimplementing T3 Code's projectors:
  - append the threads' v2 events to the target in source order, with new
    sequence numbers;
  - copy their projection rows verbatim, except for remapping `projectId`;
  - advance the projection cursor.
  Everything happens in one `BEGIN IMMEDIATE` transaction on the target.
  Attachments are copied without overwriting existing files.
- Source and target projects are matched by real path, so a symlinked
  workspace root matches its physical path. A missing target project is
  reported, with the `t3 project add` command that creates it.
- A move refuses to run when:
  - the migration ledger, projection schema version or written table columns
    do not exactly match a supported schema;
  - either server appears to be running, per `server-runtime.json` pid
    liveness or open handles on the database;
  - a selected thread has non-terminal work;
  - any primary key or event ID collides with the target.
  Planning is the default; writing needs an explicit flag. The target
  database and attachments are backed up before writing.
- Target threads that `import recent sessions` created for the same provider
  session are soft-deleted with a `thread.deleted` event. Moved source threads
  are archived in the source, so neither server drives the same provider
  session.
  ADR-002 extends this cleanup to imported subagent sessions and to reruns.

## Alternatives and consequences

- **Replaying events through T3 Code's projectors:** would need its
  TypeScript runtime or a Go reimplementation that tracks every upstream
  change. Copying rows keeps the tool small and exact, but ties each release
  to specific schema versions. Unknown schemas fail closed until support is
  added.
- **Exporting and importing portable bundles across hosts:** would have to
  handle path rewriting, provider transcripts and attachment grants. It is
  deferred until the same-host move is proven.
- **An upstream T3 Code feature:** would be preferable long term. t3rry is
  useful now, and its schema gate keeps it safe if upstream changes storage.
