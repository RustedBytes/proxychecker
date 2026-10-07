import asyncio
from importlib.metadata import metadata
from contextlib import contextmanager
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import unittest

import proxyprobe as checker


class ApiTests(unittest.TestCase):
    def test_package_identity(self):
        self.assertEqual(metadata("proxyprobe")["Name"], "proxyprobe")
        self.assertEqual(checker.__name__, "proxyprobe")
        self.assertEqual(checker.PyProxyCheckStream.__module__, "proxyprobe")

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


@contextmanager
def proxy_server(status=200, truncated=False, gate=None, state=None):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if body != b"rsloop proxy checker":
                self.send_error(400, "unexpected request body")
                return
            if state is not None:
                with state["lock"]:
                    state["active"] += 1
                    state["maximum"] = max(state["maximum"], state["active"])
                    if state["active"] == 3:
                        state["started"].set()
            try:
                if gate is not None and not gate.wait(5):
                    return
                self.send_response(status)
                self.send_header("Content-Length", "100" if truncated else "6")
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(b"hello\xff")
                self.wfile.flush()
                self.close_connection = True
            finally:
                if state is not None:
                    with state["lock"]:
                        state["active"] -= 1

        def log_message(self, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}"
        finally:
            if gate is not None:
                gate.set()
            server.shutdown()
            thread.join()


class IntegrationTests(unittest.TestCase):
    def test_statuses_and_body_modes(self):
        for status in [200, 503]:
            with proxy_server(status=status) as proxy:
                for return_response in [False, True]:
                    async def run():
                        stream = await checker.check_proxies(
                            [proxy], user_agent="test", check_url="http://target.invalid/check",
                            return_response=return_response,
                        )
                        self.assertIs(stream.__aiter__(), stream)
                        results = [item async for item in stream]
                        self.assertEqual(len(results), 1)
                        result = results[0]
                        self.assertEqual(result["proxy"], proxy)
                        self.assertEqual(result["status"], status)
                        self.assertEqual(result["ok"], status == 200)
                        self.assertIsInstance(result["elapsed_ms"], int)
                        if return_response:
                            self.assertEqual(result["response_text"], "hello\ufffd")
                        else:
                            self.assertNotIn("response_text", result)
                        if status == 200:
                            self.assertNotIn("error", result)
                        else:
                            self.assertEqual(result["error"], "target returned HTTP 503")
                    with self.subTest(status=status, return_response=return_response):
                        asyncio.run(run())

    def test_truncated_body_is_failure_in_both_modes(self):
        with proxy_server(truncated=True) as proxy:
            for return_response in [False, True]:
                async def run():
                    stream = await checker.check_proxies(
                        [proxy], user_agent="test", check_url="http://target.invalid/check",
                        return_response=return_response,
                    )
                    result, = [item async for item in stream]
                    self.assertFalse(result["ok"])
                    self.assertEqual(result["status"], 200)
                    self.assertIn("response body read failed", result["error"])
                    self.assertNotIn("response_text", result)
                asyncio.run(run())

    def test_worker_setup_error_raises_runtime_error(self):
        async def run():
            stream = await checker.check_proxies([], user_agent="invalid\nheader")
            with self.assertRaisesRegex(RuntimeError, "failed to build reqwest client"):
                [item async for item in stream]
        asyncio.run(run())

    def test_requires_running_event_loop(self):
        with self.assertRaises(RuntimeError):
            checker.check_proxies([], user_agent="test")

    def test_concurrency_limit_and_complete_delivery(self):
        gate = threading.Event()
        state = {"lock": threading.Lock(), "started": threading.Event(), "active": 0, "maximum": 0}
        with proxy_server(gate=gate, state=state) as proxy:
            async def run():
                stream = await checker.check_proxies(
                    [proxy] * 6, user_agent="test", check_url="http://target.invalid/check", concurrency=3,
                )
                try:
                    self.assertTrue(await asyncio.to_thread(state["started"].wait, 5))
                    with state["lock"]:
                        self.assertEqual(state["active"], 3)
                finally:
                    gate.set()
                results = [item async for item in stream]
                self.assertEqual(len(results), 6)
                self.assertTrue(all(item["ok"] for item in results))
                self.assertLessEqual(state["maximum"], 3)
            asyncio.run(run())


class LifecycleTests(unittest.TestCase):
    def test_cancelling_iteration_closes_active_request(self):
        async def run():
            started = asyncio.Event()
            disconnected = asyncio.Event()

            async def handle(reader, writer):
                try:
                    await reader.readuntil(b"\r\n\r\n")
                    await reader.readexactly(len(b"rsloop proxy checker"))
                    started.set()
                    self.assertEqual(await reader.read(), b"")
                    disconnected.set()
                finally:
                    writer.close()
                    await writer.wait_closed()

            server = await asyncio.start_server(handle, "127.0.0.1", 0)
            async with server:
                port = server.sockets[0].getsockname()[1]
                stream = await checker.check_proxies(
                    [f"http://127.0.0.1:{port}"], user_agent="test",
                    check_url="http://target.invalid/check", timeout_ms=10000
                )
                pending = asyncio.ensure_future(anext(stream))
                await asyncio.wait_for(started.wait(), 3)
                pending.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await pending
                await asyncio.wait_for(disconnected.wait(), 3)

        asyncio.run(run())

    def test_slow_consumer_bounds_requests_and_drop_cancels(self):
        async def run():
            requests = 0
            second = asyncio.Event()
            third = asyncio.Event()

            async def handle(reader, writer):
                nonlocal requests
                try:
                    await reader.readuntil(b"\r\n\r\n")
                    await reader.readexactly(len(b"rsloop proxy checker"))
                    requests += 1
                    if requests == 2:
                        second.set()
                    if requests == 3:
                        third.set()
                    writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    await writer.drain()
                finally:
                    writer.close()
                    await writer.wait_closed()

            server = await asyncio.start_server(handle, "127.0.0.1", 0)
            async with server:
                port = server.sockets[0].getsockname()[1]
                stream = await checker.check_proxies(
                    [f"http://127.0.0.1:{port}"] * 20,
                    user_agent="test", check_url="http://target.invalid/check", concurrency=1,
                )
                await asyncio.wait_for(second.wait(), 3)
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(third.wait(), 0.2)
                self.assertTrue((await anext(stream))["ok"])
                await asyncio.wait_for(third.wait(), 3)
                del stream
                await asyncio.sleep(0.2)
                self.assertLessEqual(requests, 3)

        asyncio.run(run())
