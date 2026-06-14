# AGENTS.md

Guidance for future coding agents working in this repository.

## Project Notes

- This is a Go CLI for testing Xray-core outbound configs.
- Xray-core is vendored as a git submodule under `third_party/Xray-core`.
- The app intentionally uses `core.Dial` directly instead of opening a local SOCKS/HTTP proxy port.

## Xray Instance Lifecycle

- Do not run multiple Xray `core.Instance` / `Server` values concurrently in one process.
- Xray-core has process-global dialer and DNS state; `core.New` / `StartInstance` can update that state.
- Keep the lifecycle serialized: build/load config first, then `StartInstance`, test, and `Close` while holding the process-level guard.
- Parallel tester processes are safer than many concurrent Xray instances inside one process, because process-global state is isolated per OS process.

## CLI Semantics

- Timeout flags are integer milliseconds, not Go duration strings:
  - `--connect-timeout 10000`
  - `--download-timeout 30000`
- `--parallelism` controls worker goroutines, but not concurrent running Xray instances.
- Main test URL responses with HTTP 2xx or 3xx are valid. Redirects are intentionally not followed.
- Exit-IP detection should only trust successful 2xx responses, because the response body must be an IP address.

## Development

- Prefer small, focused changes in `internal/tester` and keep vendored Xray-core changes out of normal app fixes.
- Run `go test ./...` before finishing.
- If investigating concurrency behavior, try `go test -race ./...`, but note that race builds may depend on the local Go toolchain setup.
