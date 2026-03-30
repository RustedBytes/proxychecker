# Rust Proxy Checker Example

This example is a standalone PyO3 extension built on top of `rsloop::rust_async`
that checks many proxies concurrently and yields Python-friendly result objects
as soon as each proxy finishes.

It uses:

- `rsloop::rust_async::future_into_py(...)` to expose the async work to Python
- `wreq` for the HTTP client and proxy support
- a dedicated Tokio runtime inside the Rust worker because `wreq` runs on Tokio

## Python API

```python
async def main():
    stream = await rsloop_rust_proxychecker.check_proxies(
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

That means you can collect `successful` and `failed` proxies in Python while
still getting each result immediately:

```python
successful = []
failed = []

stream = await rsloop_rust_proxychecker.check_proxies(proxies, user_agent="my-app/1.0")
async for result in stream:
    if result["ok"]:
        successful.append(result)
    else:
        failed.append(result)
```

## Supported proxy strings

The example passes the proxy string directly to `wreq`, so support follows the
proxy schemes that `wreq` accepts. In practice that includes normal HTTP/HTTPS
proxies and, with the enabled `socks` feature, SOCKS proxies as well.

## Run

From the repository root:

```bash
cargo check
uv run demo.py
```
