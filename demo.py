from __future__ import annotations

import pprint
import json
from dataclasses import dataclass

import rsloop
import rsloop_rust_proxychecker


@dataclass(slots=True)
class ProxyCheckResult:
    proxy: str
    ok: bool
    elapsed_ms: int
    status: int | None = None
    error: str | None = None
    response_text: str | None = None

    @classmethod
    def from_result(cls, result: dict[str, object]) -> ProxyCheckResult:
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


def read_proxies_from_file(file_path: str) -> list[str]:
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]


async def main() -> None:
    proxies = read_proxies_from_file("proxies.txt")
    stream = await rsloop_rust_proxychecker.check_proxies(
        proxies,
        user_agent="rsloop-rust-proxychecker-demo/0.1",
        timeout_ms=3_000,
        return_response=True,
    )
    successful: list[ProxyCheckResult] = []
    failed: list[ProxyCheckResult] = []

    async for result in stream:
        proxy_result = ProxyCheckResult.from_result(result)
        pprint.pp(proxy_result)
        if proxy_result.ok:
            successful.append(proxy_result)
        else:
            failed.append(proxy_result)

    print("successful:", len(successful))
    print("failed:", len(failed))

    for result in successful:
        print(
            f"Successful proxy: {result.proxy} (status: {result.status}, elapsed: {result.elapsed_ms}ms)"
        )
        print("--- Response text ---")
        data = json.loads(result.response_text) if result.response_text else {}
        pprint.pp(data)
        print("--- End of response ---\n")


if __name__ == "__main__":
    rsloop.run(main())
