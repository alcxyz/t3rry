# t3rry repository instructions

- GitHub `origin` is authoritative for branches, pull requests, CI and releases.
- Develop from `dev` and target pull requests at `dev`. Protected `main` accepts only same-repository `dev`
  promotion pull requests with a new release version; see [CONTRIBUTING.md](CONTRIBUTING.md).
- Read the [ADR index](docs/adr/README.md) before lasting design changes. ADR-001 defines what a move copies,
  transforms and refuses.
- Never run t3rry, tests or experiments against a live T3 Code base directory. Work on copies, and keep real
  paths, thread ids, hostnames and transcript content out of commits, fixtures and docs.
- New T3 Code schemas need a captured fixture, `go generate ./internal/store` and a review of upstream storage
  changes; see "Supporting a new T3 Code schema" in [CONTRIBUTING.md](CONTRIBUTING.md).
- For Go changes, run `gofmt`, `go test ./...` and `go vet ./...`. Prefer direct Go: concrete types, ordinary
  loops, no generic declarations or range-over-func iterators.
