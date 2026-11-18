import asyncio
import unittest
from unittest.mock import patch

import httpx

import app.scripts.middleware_demo as demo
from app.api.sse import encode_sse


class MiddlewareTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        print_patch = patch("builtins.print")
        print_patch.start()
        self.addCleanup(print_patch.stop)

    async def asyncSetUp(self):
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=demo.app), base_url="http://test")

    async def asyncTearDown(self):
        await self.client.aclose()

    async def test_handler_sees_before_step_and_response_header_includes_after_step(self):
        response = await self.client.get("/demo/middleware", params={"label": "A"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"label": "A", "steps": ["middleware.before", "handler"]})
        self.assertEqual(response.headers["x-demo-trace"], "middleware.before -> handler -> middleware.after")
        self.assertGreaterEqual(float(response.headers["x-process-time"]), 0)

    async def test_concurrent_requests_keep_separate_labels_and_step_lists(self):
        responses = await asyncio.gather(
            self.client.get("/demo/middleware", params={"label": "A"}),
            self.client.get("/demo/middleware", params={"label": "B"}),
        )
        self.assertEqual([response.json()["label"] for response in responses], ["A", "B"])
        for response in responses:
            self.assertEqual(response.json()["steps"], ["middleware.before", "handler"])
            self.assertEqual(response.headers["x-demo-trace"], "middleware.before -> handler -> middleware.after")

    async def test_middleware_also_wraps_unmatched_routes(self):
        response = await self.client.get("/does-not-exist")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.headers["x-demo-trace"], "middleware.before -> middleware.after")

    async def test_after_hook_and_headers_are_ready_before_stream_finishes(self):
        paused = asyncio.Event()
        release = asyncio.Event()
        first_body = asyncio.Event()
        messages = []

        async def controlled_stream():
            yield encode_sse({"type": "progress", "status": "running"})
            paused.set()
            await release.wait()
            yield encode_sse({"type": "result", "data": []})

        async def receive():
            await asyncio.Event().wait()

        async def send(message):
            messages.append(message)
            if message["type"] == "http.response.body" and message.get("body"):
                first_body.set()

        path = "/demo/middleware/stream"
        scope = {
            "type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
            "http_version": "1.1", "method": "GET", "scheme": "http", "root_path": "",
            "path": path, "raw_path": path.encode(), "query_string": b"", "headers": [],
            "client": ("127.0.0.1", 12345), "server": ("test", 80),
        }
        with patch.object(demo, "generate_stream", controlled_stream):
            task = asyncio.create_task(demo.app(scope, receive, send))
            try:
                await asyncio.wait_for(paused.wait(), timeout=2)
                await asyncio.wait_for(first_body.wait(), timeout=2)
                self.assertFalse(task.done())
                headers = dict(next(item for item in messages if item["type"] == "http.response.start")["headers"])
                self.assertEqual(headers[b"x-demo-trace"], b"middleware.before -> handler -> middleware.after")
                self.assertNotIn(b'"result"', b"".join(item.get("body", b"") for item in messages))
            finally:
                release.set()
                await asyncio.wait_for(task, timeout=2)
        self.assertIn(b'"result"', b"".join(item.get("body", b"") for item in messages))


if __name__ == "__main__":
    unittest.main()
