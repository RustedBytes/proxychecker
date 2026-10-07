import asyncio
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import unittest

import rsloop_rust_proxychecker as checker


class ApiTests(unittest.TestCase):
    def test_empty_input_ends_stream(self):
        async def run():
            stream = await checker.check_proxies([], user_agent="test")
            self.assertEqual([item async for item in stream], [])

        asyncio.run(run())

    def test_invalid_proxy_returns_failed_outcome(self):
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                requests.append(self.path)
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args):
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            target = f"http://127.0.0.1:{server.server_port}/check"

            async def run():
                # The target succeeds if reached directly, making proxy bypass
                # observable without depending on an external HTTP service.
                for proxy in ["invalid://proxy", "ftp://127.0.0.1:8080"]:
                    with self.subTest(proxy=proxy):
                        stream = await checker.check_proxies(
                            [proxy], user_agent="test", check_url=target
                        )
                        results = [item async for item in stream]
                        self.assertEqual(len(results), 1)
                        self.assertEqual(results[0]["proxy"], proxy)
                        self.assertFalse(results[0]["ok"])
                        self.assertIn("unsupported proxy scheme", results[0]["error"])
                        self.assertNotIn("status", results[0])
                        self.assertNotIn("response_text", results[0])

            try:
                asyncio.run(run())
            finally:
                server.shutdown()
                thread.join()
        self.assertEqual(requests, [], "invalid proxy must not bypass to the target")

    def test_invalid_arguments(self):
        for kwargs in [
            {"user_agent": ""},
            {"user_agent": "test", "timeout_ms": 0},
            {"user_agent": "test", "concurrency": 0},
            {"user_agent": "test", "check_url": "not a URL"},
        ]:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                checker.check_proxies([], **kwargs)
