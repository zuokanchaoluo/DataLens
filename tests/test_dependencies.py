import asyncio
import unittest
from unittest.mock import patch

import httpx
from fastapi import Depends, FastAPI

import app.scripts.dependencies_demo as demo
from app.api.sse import encode_sse


class DependencyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.resources = []
        self.original_client = httpx.AsyncClient

        def track_client(*args, **kwargs):
            resource = self.original_client(*args, **kwargs)
            self.resources.append(resource)
            return resource

        for item in (patch("builtins.print"), patch.object(demo.httpx, "AsyncClient", side_effect=track_client)):
            item.start()
            self.addCleanup(item.stop)

    async def asyncSetUp(self):
        self.client = self.original_client(transport=httpx.ASGITransport(app=demo.app), base_url="http://test")

    async def asyncTearDown(self):
        await self.client.aclose()

    async def test_sub_dependencies_share_one_resource_and_release_it_after_response(self):
        response = await self.client.post("/demo/dependencies", json={"query": " 统计销售额 "})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"query": "统计销售额", "resource_open": True, "same_client": True})
        self.assertEqual(len(self.resources), 1)
        self.assertTrue(self.resources[0].is_closed)

    async def test_new_request_gets_a_new_resource(self):
        for question in ("问题A", "问题B"):
            response = await self.client.post("/demo/dependencies", json={"query": question})
            self.assertEqual(response.json()["query"], question)
            self.assertTrue(self.resources[-1].is_closed)
        self.assertEqual(len(self.resources), 2)
        self.assertIsNot(self.resources[0], self.resources[1])

    async def test_concurrent_requests_have_separate_resources(self):
        responses = await asyncio.gather(
            self.client.post("/demo/dependencies", json={"query": "问题A"}),
            self.client.post("/demo/dependencies", json={"query": "问题B"}),
        )
        self.assertEqual([response.json()["query"] for response in responses], ["问题A", "问题B"])
        self.assertTrue(all(response.json()["resource_open"] and response.json()["same_client"] for response in responses))
        self.assertEqual(len(self.resources), 2)
        self.assertIsNot(self.resources[0], self.resources[1])
        self.assertTrue(all(resource.is_closed for resource in self.resources))

    async def test_invalid_body_returns_422_and_any_created_resource_is_released(self):
        response = await self.client.post("/demo/dependencies", json={"query": " "})
        self.assertEqual(response.status_code, 422)
        self.assertTrue(all(resource.is_closed for resource in self.resources))

    async def test_route_failure_releases_the_yield_dependency(self):
        application = FastAPI()

        @application.get("/fail")
        async def fail(client=Depends(demo.get_demo_client, scope="request")):
            raise RuntimeError("example failure")

        async with self.original_client(
            transport=httpx.ASGITransport(app=application, raise_app_exceptions=False), base_url="http://test"
        ) as client:
            response = await client.get("/fail")
        self.assertEqual(response.status_code, 500)
        self.assertEqual(len(self.resources), 1)
        self.assertTrue(self.resources[0].is_closed)

    async def test_request_scope_keeps_resource_open_until_stream_finishes(self):
        paused = asyncio.Event()
        release = asyncio.Event()
        first_body = asyncio.Event()
        messages = []

        async def controlled_stream(client):
            yield encode_sse({"type": "progress", "resource_open": not client.is_closed})
            paused.set()
            await release.wait()
            self.assertFalse(client.is_closed)
            yield encode_sse({"type": "result", "data": []})

        async def receive():
            await asyncio.Event().wait()

        async def send(message):
            messages.append(message)
            if message["type"] == "http.response.body" and message.get("body"):
                first_body.set()

        path = "/demo/dependencies/stream"
        scope = {
            "type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
            "http_version": "1.1", "method": "GET", "scheme": "http", "root_path": "",
            "path": path, "raw_path": path.encode(), "query_string": b"", "headers": [],
            "client": ("127.0.0.1", 12345), "server": ("test", 80),
        }
        with patch.object(demo, "generate_stream", controlled_stream):
            task = asyncio.create_task(demo.app(scope, receive, send))
            try:
                await asyncio.wait_for(paused.wait(), timeout=5)
                await asyncio.wait_for(first_body.wait(), timeout=5)
                self.assertEqual(len(self.resources), 1)
                self.assertFalse(self.resources[0].is_closed)
                self.assertFalse(task.done())
            finally:
                release.set()
                await asyncio.wait_for(task, timeout=5)
        self.assertTrue(self.resources[0].is_closed)


if __name__ == "__main__":
    unittest.main()
