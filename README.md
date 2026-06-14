# Xray Core Proxy Tester

A CLI wrapper for testing Xray `outbounds`. Valid outbounds are loaded into one Xray instance, and each test uses Xray’s dispatcher plus `core.Dial` with the outbound tag forced for that request. This follows the same general idea as Xray’s internal `observatory` / `burst` health checks, but skips the extra local proxy hop. After each test, the response body is closed and idle HTTP connections are closed; after the run, the shared Xray instance is shut down.

Xray-core uses process-global networking state and documents that only one `Server` instance should be running at a time. For that reason this tester starts one Xray instance per run inside a process, performs outbound tests through that instance, and then closes it. Running several tester processes still creates one running Xray instance per process.

## Xray-core

`XTLS/Xray-core` is included as a git submodule in `third_party/Xray-core` and pinned to latest release.

```bash
git submodule update --init --recursive
```

## Build and run

```bash
go build ./cmd/proxy-tester
```

Minimal URL test via stdin:

```bash
printf '[{"tag":"direct","protocol":"freedom","settings":{}}]' | \
  go run ./cmd/proxy-tester \
    --test-type url \
    --url http://example.com \
    --retries 3 \
    --exit-ip-url https://api.ipify.org \
    --connect-timeout 10000
```

Speed test:

```bash
go run ./cmd/proxy-tester \
  --test-type speed \
  --url https://example.com/file.bin \
  --outbounds-file outbounds.json \
  --download-timeout 30000 \
  --retries 2
```

## Input format

Supports either a JSON array of outbounds or an object with an `outbounds` field, compatible with the Xray outbound config format:

```json
[
  {
    "tag": "...",
    "protocol": "...",
    "settings": {}
  }
]
```

If `tag` is missing, the app assigns a stable tag such as `outbound-1`. Duplicate tags are suffixed automatically.

## Arguments

* `--test-type`: `url` or `speed`
* `--url`: test URL
* `--retries`: number of attempts per outbound
* `--exit-ip-url`: URL used to detect the exit IP; may be repeated or passed as a comma-separated list
* `--outbounds-file`: JSON file with outbounds; if omitted or set to `-`, stdin is used
* `--download-timeout`: download timeout for speed tests, in milliseconds
* `--connect-timeout`: timeout for URL tests and exit IP requests, in milliseconds
* `--geoip2-db-path`: optional path to GeoIP2/GeoLite2 City database (`.mmdb`); when set and exit IP is detected, adds `country` and `city` to result
* `--parallelism`: maximum number of worker goroutines. All workers share one Xray instance inside the process
* `--min-speed-mbps`: optional minimum speed threshold; if not reached, the reason is `speed_below_threshold`
* `--max-latency-ms`: optional maximum latency threshold; if exceeded, the reason is `latency_exceeded`
* `--allow-mux`: preserve outbound `mux` settings; by default tests disable mux in the temporary Xray config to avoid lingering mux client connections

## Output format

```json
{
  "outbound-tag": {
    "result": true,
    "speed": null,
    "latency": 123.4,
    "exit-ip": "203.0.113.10",
    "country": "US",
    "city": "Los Angeles",
    "reason": "ok"
  }
}
```

`speed` is reported in megabits per second for `speed` tests. `latency` is reported in milliseconds for `url` tests.
If GeoIP2 is not configured (or no data found), `country` / `city` are returned as `null`.
HTTP 2xx and 3xx responses from the main test URL are considered valid. Redirects are not followed; a redirect response itself is enough for the URL test to pass.
