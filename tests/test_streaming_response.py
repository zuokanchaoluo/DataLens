import asyncio
import unittest
from unittest.mock import patch

import app.scripts.streaming_demo as demo


class StreamingResponseTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.paused = asyncio.Event()
        self.release = asyncio.Event()
        self.messages = []

        async def controlled_messages():
            yield "第一条\n"
            self.paused.set()
            await self.release.wait()
            yield "第二条\n"

        generator_patch = patch.object(demo, "generate_messages", controlled_messages)
        generator_patch.start()
        self.addCleanup(generator_patch.stop)

    async def request(self, path):
        scope = {
            "type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
            "http_version": "1.1", "method": "GET", "scheme": "http", "root_path": "",
            "path": path, "raw_path": path.encode(), "query_string": b"", "headers": [],
            "client": ("127.0.0.1", 12345), "server": ("test", 80),
        }
        request_sent = False

        async def receive():
            nonlocal request_sent
            if not request_sent:
                request_sent = True
                return {"type": "http.request", "body": b"", "more_body": False}
            await asyncio.Event().wait()

        async def send(message):
            self.messages.append(message)

        await demo.app(scope, receive, send)

    def body(self):
        return b"".join(message.get("body", b"") for message in self.messages).decode("utf-8")

    async def test_streaming_sends_first_message_before_generator_finishes(self):
        task = asyncio.create_task(self.request("/demo/stream"))
        try:
            await asyncio.wait_for(self.paused.wait(), timeout=2)
            self.assertFalse(task.done())
            self.assertEqual(self.body(), "第一条\n")
            self.assertEqual(self.messages[0]["status"], 200)
            headers = dict(self.messages[0]["headers"])
            self.assertEqual(headers[b"content-type"], b"text/plain; charset=utf-8")
        finally:
            self.release.set()
            await asyncio.wait_for(task, timeout=2)
        self.assertEqual(self.body(), "第一条\n第二条\n")
        self.assertFalse(self.messages[-1]["more_body"])

    async def test_normal_response_waits_for_generator_to_finish(self):
        task = asyncio.create_task(self.request("/demo/normal"))
        try:
            await asyncio.wait_for(self.paused.wait(), timeout=2)
            self.assertFalse(task.done())
            self.assertEqual(self.messages, [])
        finally:
            self.release.set()
            await asyncio.wait_for(task, timeout=2)
        self.assertEqual(self.body(), "第一条\n第二条\n")


if __name__ == "__main__":
    unittest.main()
