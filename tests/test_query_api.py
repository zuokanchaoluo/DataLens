import asyncio
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import httpx
from fastapi import Depends

with patch.dict(os.environ, {"OPENAI_API_KEY": "offline-test-key"}):
    import app.api.dependencies as dependencies
    import app.services.query_service as service_module
    from main import app

from app.repositories.es.value_es_repository import ValueEsRepository
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository
from app.repositories.qdrant.column_qdrant_repository import ColumnQdrantRepository
from app.repositories.qdrant.metric_qdrant_repository import MetricQdrantRepository


class TrackingSession:
    """不连接数据库，记录请求会话的进入和退出。"""

    def __init__(self, database):
        self.database = database
        self.closed = False
        self.commit = AsyncMock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True


def read_events(content):
    frames = content.decode("utf-8").split("\n\n")
    if frames[-1] != "" or any(not frame.startswith("data: ") for frame in frames[:-1]):
        raise AssertionError("Invalid SSE framing")
    return [json.loads(frame[6:]) for frame in frames[:-1]]


class QueryAPITests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.sessions = []
        self.contexts = []
        self.inputs = []
        self.fail_workflow = False
        self.embedding_client = object()
        self.qdrant_client = object()
        self.es_client = object()
        self.managers = {}
        factories = {}
        for database in ("meta", "dw"):
            def factory(database=database):
                session = TrackingSession(database)
                self.sessions.append(session)
                return session

            factories[database] = Mock(side_effect=factory)

        resources = {
            "embedding_client_manager": {"client": self.embedding_client},
            "qdrant_client_manager": {"client": self.qdrant_client},
            "es_client_manager": {"client": self.es_client},
            "meta_mysql_client_manager": {"session_factory": factories["meta"]},
            "dw_mysql_client_manager": {"session_factory": factories["dw"]},
        }
        for name, fields in resources.items():
            manager = SimpleNamespace(**fields, init=Mock(), close=AsyncMock())
            self.managers[name] = manager
            manager_patch = patch.object(dependencies, name, manager)
            manager_patch.start()
            self.addCleanup(manager_patch.stop)

        for item in (patch.object(service_module, "graph", SimpleNamespace(astream=self.workflow)),
                     patch.object(service_module, "logger")):
            item.start()
            self.addCleanup(item.stop)

    async def asyncSetUp(self):
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test", trust_env=False,
        )

    async def asyncTearDown(self):
        await self.client.aclose()

    async def workflow(self, *, input, context, **kwargs):
        self.inputs.append(input)
        self.contexts.append(context)
        self.assert_sessions_open(context)
        yield "custom", {"type": "progress", "step": "生成SQL", "status": "running"}
        await asyncio.sleep(0)
        self.assert_sessions_open(context)
        if self.fail_workflow:
            raise RuntimeError("private connection error")
        rows = [] if input["query"] == "空结果" else [{"question": input["query"]}]
        result = {"columns": ["question"], "rows": rows, "row_count": len(rows), "truncated": False}
        yield "custom", {"type": "result", "data": rows, "columns": result["columns"],
                         "row_count": result["row_count"], "truncated": result["truncated"]}
        yield "values", {**input, "error": None, "sql_result": result}

    def assert_sessions_open(self, context):
        for key in ("meta_mysql_repository", "dw_mysql_repository"):
            self.assertFalse(context[key].session.closed)

    def assert_sessions_closed(self):
        self.assertTrue(all(session.closed for session in self.sessions))
        for session in self.sessions:
            session.commit.assert_not_awaited()

    async def test_route_runs_real_dependency_chain_and_returns_sse(self):
        response = await self.client.post("/api/query", json={"query": " 统计销售额 "})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "text/event-stream; charset=utf-8")
        self.assertEqual(response.headers["cache-control"], "no-cache")
        self.assertEqual(read_events(response.content), [
            {"type": "progress", "step": "生成SQL", "status": "running"},
            {"type": "result", "data": [{"question": "统计销售额"}],
             "columns": ["question"], "row_count": 1, "truncated": False},
        ])
        context = self.contexts[0]
        self.assertIs(context["embedding_client"], self.embedding_client)
        for key, cls, client in (
            ("column_qdrant_repository", ColumnQdrantRepository, self.qdrant_client),
            ("metric_qdrant_repository", MetricQdrantRepository, self.qdrant_client),
            ("value_es_repository", ValueEsRepository, self.es_client),
        ):
            self.assertIsInstance(context[key], cls)
            self.assertIs(context[key].client, client)
        self.assertIsInstance(context["meta_mysql_repository"], MetaMySQLRepository)
        self.assertIsInstance(context["dw_mysql_repository"], DWMySQLRepository)
        self.assertEqual([session.database for session in self.sessions], ["meta", "dw"])
        self.assert_sessions_closed()
        for manager in self.managers.values():
            manager.init.assert_not_called()
            manager.close.assert_not_awaited()

    async def test_openapi_documents_only_query_input_and_sse_response(self):
        response = await self.client.get("/openapi.json")
        self.assertEqual(response.status_code, 200)
        schema = response.json()
        operation = schema["paths"]["/api/query"]["post"]
        self.assertNotIn("parameters", operation)
        self.assertEqual(set(operation["responses"]["200"]["content"]), {"text/event-stream"})
        request_schema = schema["components"]["schemas"]["QuerySchema"]
        self.assertEqual(request_schema["required"], ["query"])
        self.assertEqual(set(request_schema["properties"]), {"query"})
        self.assertFalse(request_schema["additionalProperties"])
        self.assertEqual(self.sessions, [])

    async def test_invalid_requests_never_run_workflow_and_release_created_sessions(self):
        for payload in ({}, {"query": " "}, {"query": 123}, {"query": "问题", "sql": "SELECT 1"}):
            with self.subTest(payload=payload):
                response = await self.client.post("/api/query", json=payload)
                self.assertEqual(response.status_code, 422)
                self.assertEqual(self.contexts, [])
                self.assert_sessions_closed()

    async def test_empty_result_remains_success_over_http(self):
        response = await self.client.post("/api/query", json={"query": "空结果"})
        self.assertEqual(read_events(response.content)[-1], {
            "type": "result", "data": [], "columns": ["question"], "row_count": 0, "truncated": False,
        })
        self.assert_sessions_closed()

    async def test_concurrent_requests_have_separate_services_repositories_and_sessions(self):
        with patch.object(dependencies, "QueryService", wraps=service_module.QueryService) as constructor:
            first, second = await asyncio.gather(
                self.client.post("/api/query", json={"query": "问题一"}),
                self.client.post("/api/query", json={"query": "问题二"}),
            )
        self.assertEqual(constructor.call_count, 2)
        self.assertEqual(read_events(first.content)[-1]["data"], [{"question": "问题一"}])
        self.assertEqual(read_events(second.content)[-1]["data"], [{"question": "问题二"}])
        self.assertIsNot(self.inputs[0], self.inputs[1])
        self.assertIsNot(self.contexts[0], self.contexts[1])
        for key in ("meta_mysql_repository", "dw_mysql_repository"):
            self.assertIsNot(self.contexts[0][key], self.contexts[1][key])
            self.assertIsNot(self.contexts[0][key].session, self.contexts[1][key].session)
        self.assertEqual(len(self.sessions), 4)
        self.assert_sessions_closed()

    async def test_shared_client_not_initialized_returns_safe_503_without_stream(self):
        for name in ("embedding_client_manager", "qdrant_client_manager", "es_client_manager"):
            with self.subTest(manager=name), patch.object(self.managers[name], "client", None):
                response = await self.client.post("/api/query", json={"query": "问题"})
                self.assertEqual(response.status_code, 503)
                self.assertEqual(response.json(), {"type": "error", "message": "查询服务尚未就绪，请稍后重试。"})
                self.assertEqual(self.contexts, [])
                self.assert_sessions_closed()

    async def test_missing_session_factory_releases_any_preceding_session(self):
        for name in ("meta_mysql_client_manager", "dw_mysql_client_manager"):
            with self.subTest(manager=name), patch.object(self.managers[name], "session_factory", None):
                response = await self.client.post("/api/query", json={"query": "问题"})
                self.assertEqual(response.status_code, 503)
                self.assertEqual(self.contexts, [])
                self.assert_sessions_closed()
        self.assertEqual(len(self.sessions), 1)

    async def test_workflow_failure_returns_sse_error_and_releases_both_sessions(self):
        self.fail_workflow = True
        response = await self.client.post("/api/query", json={"query": "问题"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(read_events(response.content)[-1], {
            "type": "error", "message": service_module.QUERY_ERROR_MESSAGE,
        })
        self.assertNotIn("private", response.text)
        self.assertEqual(len(self.sessions), 2)
        self.assert_sessions_closed()

    async def test_request_dependency_cache_reuses_same_service_and_sessions(self):
        calls = []

        async def get_same_service(
            first=Depends(dependencies.get_query_service),
            second=Depends(dependencies.get_query_service),
        ):
            calls.append((first, second))
            return first

        # 应用级依赖与路由共同引用同一个服务依赖，验证请求内缓存。
        from fastapi import FastAPI
        from app.api.routers.query_router import query_router

        application = FastAPI(dependencies=[Depends(get_same_service)])
        application.include_router(query_router)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test", trust_env=False,
        ) as client:
            await client.post("/api/query", json={"query": "问题"})
        self.assertEqual(len(calls), 1)
        self.assertIs(calls[0][0], calls[0][1])
        self.assertIs(calls[0][0].meta_mysql_repository, self.contexts[0]["meta_mysql_repository"])
        self.assertEqual(len(self.sessions), 2)
        self.assert_sessions_closed()

    async def test_sessions_remain_open_through_last_body_chunk_then_close(self):
        paused, release = asyncio.Event(), asyncio.Event()
        body = json.dumps({"query": "流式会话检查"}, ensure_ascii=False).encode()
        messages = []
        body_received = False

        async def receive():
            nonlocal body_received
            if not body_received:
                body_received = True
                return {"type": "http.request", "body": body, "more_body": False}
            await asyncio.Event().wait()

        async def send(message):
            messages.append(message)
            # 连最终 more_body=False 的发送时刻，会话也应保持开启。
            if message["type"] == "http.response.body":
                self.assertEqual(len(self.sessions), 2)
                self.assertTrue(all(not session.closed for session in self.sessions))
                if message.get("body") and not paused.is_set():
                    paused.set()
                    await release.wait()

        path = "/api/query"
        scope = {
            "type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
            "http_version": "1.1", "method": "POST", "scheme": "http", "root_path": "",
            "path": path, "raw_path": path.encode(), "query_string": b"",
            "headers": [(b"content-type", b"application/json")],
            "client": ("127.0.0.1", 12345), "server": ("test", 80),
        }
        task = asyncio.create_task(app(scope, receive, send))
        try:
            await asyncio.wait_for(paused.wait(), timeout=5)
            self.assertTrue(all(not session.closed for session in self.sessions))
            self.assertFalse(task.done())
        finally:
            release.set()
            await asyncio.wait_for(task, timeout=5)
        self.assert_sessions_closed()
        wire = b"".join(message.get("body", b"") for message in messages
                        if message["type"] == "http.response.body")
        self.assertEqual([event["type"] for event in read_events(wire)], ["progress", "result"])


if __name__ == "__main__":
    unittest.main()
