from __future__ import annotations

import pprint
from dataclasses import dataclass

import rsloop
import rsloop_rust_proxychecker


@dataclass(slots=True)
class ProxyCheckResult:
    proxy: str
    ok: bool
    data: dict[str, object]

    @classmethod
    def from_result(cls, result: dict[str, object]) -> ProxyCheckResult:
        return cls(
            proxy=str(result.get("proxy", "")),
            ok=bool(result["ok"]),
            data=result,
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


if __name__ == "__main__":
    rsloop.run(main())
