import asyncio
import unittest

import rsloop_rust_proxychecker as checker


class ApiTests(unittest.TestCase):
    def test_empty_input_ends_stream(self):
        async def run():
            stream = await checker.check_proxies([], user_agent="test")
            self.assertEqual([item async for item in stream], [])

        asyncio.run(run())

    def test_invalid_proxy_returns_failed_outcome(self):
        async def run():
            stream = await checker.check_proxies(["invalid://proxy"], user_agent="test")
            results = [item async for item in stream]
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]["proxy"], "invalid://proxy")
            self.assertFalse(results[0]["ok"])
            self.assertIn("error", results[0])
            self.assertNotIn("status", results[0])
            self.assertNotIn("response_text", results[0])

        asyncio.run(run())

    def test_invalid_arguments(self):
        for kwargs in [
            {"user_agent": ""},
            {"user_agent": "test", "timeout_ms": 0},
            {"user_agent": "test", "concurrency": 0},
            {"user_agent": "test", "check_url": "not a URL"},
        ]:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                checker.check_proxies([], **kwargs)
