# git-cli-proxy

A vendor-agnostic HTTP service that serves commit-level git data (commits,
per-file changes, branches) from a disk-bounded cache of bare blobless clones,
eliminating the per-commit API calls that drive the git connectors into vendor
rate limits.

Consumed by per-vendor **nocode connectors**: repository discovery and PR
streams stay on the vendor API; commit-level streams read from this proxy.

- [DESIGN](specs/DESIGN.md) — implementation design and the nocode consumption contract
- Concept & discussion: [constructorfabric/insight#2224](https://github.com/constructorfabric/insight/issues/2224)
