from __future__ import annotations

import pprint

import rsloop
import rsloop_rust_proxychecker


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
    successful: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []

    async for result in stream:
        pprint.pp(result)
        if result["ok"]:
            successful.append(result)
        else:
            failed.append(result)

    print("successful:", len(successful))
    print("failed:", len(failed))


if __name__ == "__main__":
    rsloop.run(main())
