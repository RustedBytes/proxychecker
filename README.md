# proxyprobe

[![CI](https://github.com/RustedBytes/proxychecker/actions/workflows/ci.yml/badge.svg)](https://github.com/RustedBytes/proxychecker/actions/workflows/ci.yml)

`proxyprobe` is a standalone PyO3 extension built on top of `rsloop::rust_async`
that checks many proxies concurrently and yields Python-friendly result objects
as soon as each proxy finishes.

It uses:

- `rsloop::rust_async::future_into_py(...)` to expose the async work to Python
- `wreq` for the HTTP client and proxy support
- a dedicated Tokio runtime inside the Rust worker because `wreq` runs on Tokio

## Migration

The distribution and import name are now `proxyprobe` (previously
`rsloop-rust-proxychecker` and `rsloop_rust_proxychecker`). Update your dependency
and imports; function arguments and result dictionaries are unchanged.

## Python API

```python
import proxyprobe


async def main():
    stream = await proxyprobe.check_proxies(
        proxies,
        user_agent="my-app/1.0",
        check_url="https://httpbin.org/post",
        timeout_ms=5000,
        concurrency=64,
        return_response=False,
    )

    async for result in stream:
        print(result)
```

Arguments:

- `proxies`: list of proxy URLs to test
- `user_agent`: required user agent string
- `check_url`: target URL to POST to, defaults to `https://httpbin.org/post`
- `timeout_ms`: one timeout budget used for connect, read, and total request timing
- `concurrency`: maximum number of proxy checks running at once
- `return_response`: when `True`, include the response body from `check_url` as `response_text`

Each yielded result is a dictionary like:

```python
{
    "proxy": "http://1.2.3.4:8080",
    "ok": True,
    "status": 200,
    "elapsed_ms": 412,
    "response_text": "{\"ok\":true,...}",
}
```

Failed checks yield:

```python
{
    "proxy": "socks5://1.2.3.4:1080",
    "ok": False,
    "elapsed_ms": 5001,
    "error": "...",
    "response_text": "...",  # only present when a response body was actually read
}
```

If you want a more structured object in Python, you can wrap each yielded
dictionary in a dataclass while still processing results as they arrive:

```python
from dataclasses import dataclass
import proxyprobe


@dataclass(slots=True)
class ProxyCheckResult:
    proxy: str
    ok: bool
    elapsed_ms: int
    status: int | None = None
    error: str | None = None
    response_text: str | None = None

    @classmethod
    def from_result(cls, result: dict[str, object]) -> "ProxyCheckResult":
        return cls(
            proxy=str(result.get("proxy", "")),
            ok=bool(result["ok"]),
            elapsed_ms=int(result["elapsed_ms"]),
            status=int(result["status"]) if result.get("status") is not None else None,
            error=str(result["error"]) if result.get("error") is not None else None,
            response_text=(
                str(result["response_text"])
                if result.get("response_text") is not None
                else None
            ),
        )


successful: list[ProxyCheckResult] = []
failed: list[ProxyCheckResult] = []

stream = await proxyprobe.check_proxies(proxies, user_agent="my-app/1.0")
async for result in stream:
    proxy_result = ProxyCheckResult.from_result(result)
    if proxy_result.ok:
        successful.append(proxy_result)
    else:
        failed.append(proxy_result)
```

## Rust module layout

- `api`: Python arguments, validation, and stream creation.
- `config`: shared settings and defaults.
- `checker`: proxy parsing, HTTP client, and single-proxy outcomes.
- `worker`: Tokio worker lifecycle and bounded concurrency.
- `stream`: Python queue delivery, result dictionaries, and async iteration.
- `lib`: module registration.

Build the internal Rust documentation with `cargo doc --no-deps --document-private-items`.

## Coverage

The CI check **Coverage (>=90%)** requires at least 90% line coverage across all
production Rust modules, including the Python binding and worker code. It combines
Rust unit tests with Python tests against an instrumented wheel; tests and external
dependencies are excluded from the denominator. It measures line coverage, not branch
coverage. The CI summary shows a progress bar; the `rust-coverage` artifact includes
an HTML report, JSON metrics, and a badge with the measured percentage.

To reproduce locally in an activated Python virtual environment:

```bash
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov --version 0.9.1 --locked
python -m pip install "maturin>=1.7,<2"
bash scripts/coverage.sh
```

## Supported proxy strings

The example passes the proxy string directly to `wreq`, so support follows the
proxy schemes that `wreq` accepts. In practice that includes normal HTTP/HTTPS
proxies and, with the enabled `socks` feature, SOCKS proxies as well.

## Development

Requires Python 3.10+ and Rust 1.98+. Native builds also need CMake, a C/C++
compiler, and libclang (on Ubuntu: `sudo apt-get install cmake clang libclang-dev`).

```bash
cargo fmt --check
cargo check --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
uv run python -m unittest discover -s tests -v
```

Pedantic Clippy is enabled in `Cargo.toml`. CI checks relevant pull requests and
pushes to `master`, supports manual runs, and skips documentation-only changes.
The Python API is tested on Python 3.10 and 3.15. Release notes are maintained in
[CHANGELOG.md](CHANGELOG.md); dependency compatibility changes remain under
`Unreleased` until the next version is prepared.

## Run

From the repository root:

```bash
cargo check
uv run demo.py
```
