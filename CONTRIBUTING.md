# Contributing to t3rry

## Development setup

Prerequisites: Go 1.26+. The SQLite driver is pure Go, so builds need no C
toolchain.

```bash
git clone https://github.com/alcxyz/t3rry.git
cd t3rry
go build ./cmd/t3rry
```

## Running tests

```bash
go test ./...
go vet ./...
```

Tests build temporary T3 Code databases from the schema fixture in
`internal/store/testdata`. Never point tests or development builds at a real
T3 Code base directory; copy one with `sqlite3 -readonly <db> "VACUUM INTO
'<copy>'"` while its server keeps running, or stop the server first.

## Project structure

- `cmd/t3rry/` -- command-line entry point
- `internal/buildinfo/` -- version reporting
- `internal/instance/` -- base directory paths and running-server detection
- `internal/move/` -- planning, copying, archiving and reports
- `internal/store/` -- database access and the supported schema gate
- `internal/store/schemagen/` -- generates supported schema descriptions

## Supporting a new T3 Code schema

Every supported schema is described by a fixture and generated Go code:

1. Start the new T3 Code release on an empty base directory, then capture
   `sqlite3 <db> .schema > internal/store/testdata/schema-v<N>.sql` and
   `sqlite3 <db> "SELECT migration_id, name FROM effect_sql_migrations" >
   internal/store/testdata/migrations-v<N>.txt`, where `<N>` is the highest
   migration id.
2. Add a `//go:generate go run ./schemagen -version <N>` line in
   `internal/store/store.go`, run `go generate ./internal/store`, and add the
   schema to `supportedSchemas`.
3. Review the upstream storage changes against
   [ADR-001](docs/adr/ADR-001-offline-row-level-thread-moves.md): new tables
   keyed by thread, new event types carrying `projectId`, and changes to how
   the server writes events and projections.
4. Extend the tests and run a move against copies of real databases, then
   start the new T3 Code release on the copied target.

## Making changes

1. Create a branch from `dev`
2. Make your changes
3. Add or update tests as needed
4. Run `gofmt`, `go test ./...` and `go vet ./...`
5. Open a GitHub pull request targeting `dev`
6. Squash-merge after all checks pass

CI checks formatting, generated code, build, vet, lint, race tests, the
release snapshot and the Nix build.

## Repository workflow

GitHub is t3rry's source of truth for branches, pull requests, CI and
releases. `dev` is the default development branch. Protected `main` accepts
only same-repository `dev` promotion pull requests with a new release version.

## Commit messages

Use conventional-ish prefixes: `feat:`, `fix:`, `docs:`, `chore:`,
`refactor:`.

## Releasing

Releases are automated via [GoReleaser](https://goreleaser.com/) and GitHub
Actions. The `VERSION` file is the single source of truth.

`main` and release tags use plain `x.y.z` versions. Development builds identify
their source revision: ordinary `go build` output uses `dev-<commit>[-dirty]`,
while branch-based Nix packages use `X.Y.Z-dev.<commit>[.dirty]`.

To cut a release:

1. Bump `VERSION` on `dev` to a plain release version like `0.1.0`
2. Open a same-repository pull request from `dev` to `main`
3. Wait for the promotion policy, checks, release snapshot and Nix build
4. Merge with a merge commit, not a squash; CI tags and publishes the release
5. Bump `dev` to the next development version like `0.1.1-dev`

## License

By contributing, you agree that your contributions will be licensed under the
MIT License.
