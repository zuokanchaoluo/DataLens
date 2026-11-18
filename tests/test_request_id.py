import asyncio
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from langgraph.graph import END, START, StateGraph
from langgraph.runtime import Runtime

with patch.dict(os.environ, {"OPENAI_API_KEY": "offline-test-key"}):
    import app.api.dependencies as dependencies
    import app.services.query_service as service_module
    from main import app

from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState
from app.core.context import REQUEST_ID_HEADER, request_id_ctx_var
from app.core.log import log_format, logger
from app.core.request_id import RequestIdMiddleware
from test_app_lifespan import LifecycleHarness, TrackingSession


class RequestIdTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.records = []
        self.rendered = []

        def capture(message):
            record = message.record
            self.rendered.append(str(message))
            self.records.append({"request_id": record["extra"]["request_id"],
                                 "phase": record["extra"].get("phase"),
                                 "question": record["extra"].get("question"),
                                 "message": record["message"], "name": record["name"]})

        sink = logger.add(capture, level="TRACE", format=log_format, colorize=False)
        self.addCleanup(logger.remove, sink)

    def probe(self, phase, question=None):
        logger.bind(phase=phase, question=question).trace("请求标识验证")

    def assert_id(self, value):
        self.assertRegex(value, r"^[0-9a-f]{32}$")


class RequestIdMiddlewareTests(RequestIdTestCase):
    async def test_http_id_matches_state_context_and_one_response_header(self):
        observed, sent = [], []

        async def downstream(scope, receive, send):
            observed.append((scope["state"]["request_id"], request_id_ctx_var.get()))
            await send({"type": "http.response.start", "status": 200,
                        "headers": [(b"x-request-id", b"old"), (b"x-request-id", b"duplicate"),
                                    (b"set-cookie", b"a=1"), (b"set-cookie", b"b=2")]})
            await send({"type": "http.response.body", "body": b"unchanged", "more_body": False})

        async def receive():
            self.fail("request_id 中间件不应主动读取请求体")

        async def send(message):
            sent.append(message)

        token = request_id_ctx_var.set("parent-context")
        try:
            scope = {"type": "http", "headers": [(b"x-request-id", b"client-value")], "state": {"other": 1}}
            await RequestIdMiddleware(downstream)(scope, receive, send)
            self.assertEqual(request_id_ctx_var.get(), "parent-context")
        finally:
            request_id_ctx_var.reset(token)
        request_id = observed[0][0]
        self.assert_id(request_id)
        self.assertEqual(observed, [(request_id, request_id)])
        self.assertEqual(scope["state"]["other"], 1)
        self.assertEqual([value for key, value in sent[0]["headers"] if key == b"x-request-id"], [request_id.encode()])
        self.assertEqual([value for key, value in sent[0]["headers"] if key == b"set-cookie"], [b"a=1", b"b=2"])
        self.assertEqual(sent[-1]["body"], b"unchanged")

    async def test_exception_and_cancellation_restore_previous_token(self):
        for error in (RuntimeError("offline failure"), asyncio.CancelledError()):
            with self.subTest(error=type(error).__name__):
                async def downstream(scope, receive, send):
                    self.assert_id(request_id_ctx_var.get())
                    raise error

                token = request_id_ctx_var.set("parent-context")
                try:
                    with self.assertRaises(type(error)):
                        await RequestIdMiddleware(downstream)({"type": "http"}, None, None)
                    self.assertEqual(request_id_ctx_var.get(), "parent-context")
                finally:
                    request_id_ctx_var.reset(token)

    async def test_lifespan_and_websocket_pass_through_without_creating_http_context(self):
        for scope_type in ("lifespan", "websocket"):
            with self.subTest(scope_type=scope_type):
                scope = {"type": scope_type}
                calls = []

                async def downstream(current_scope, receive, send):
                    calls.append((current_scope, request_id_ctx_var.get()))

                token = request_id_ctx_var.set("outer")
                try:
                    await RequestIdMiddleware(downstream)(scope, None, None)
                    self.assertEqual(calls, [(scope, "outer")])
                    self.assertNotIn("state", scope)
                finally:
                    request_id_ctx_var.reset(token)

    async def test_log_patch_has_default_and_reads_current_context_per_record(self):
        self.assertEqual(request_id_ctx_var.get(), "-")
        self.probe("outside")
        token = request_id_ctx_var.set("inside")
        try:
            logger.bind(request_id="stale", phase="inside").trace("日志格式验证")
        finally:
            request_id_ctx_var.reset(token)
        self.probe("after")
        self.assertEqual([record["request_id"] for record in self.records], ["-", "inside", "-"])
        self.assertIn("request_id=inside", self.rendered[1])
        self.assertIn("request_id=-", self.rendered[2])


class RequestIdAPITests(RequestIdTestCase):
    def setUp(self):
        super().setUp()
        self.harness = self.enterContext(LifecycleHarness())
        self.waiting = asyncio.Event()
        self.contexts = {}
        self.enterContext(patch.object(service_module, "graph", SimpleNamespace(astream=self.workflow)))
        original_exit = TrackingSession.__aexit__

        async def session_exit(session, *args):
            await asyncio.sleep(0)
            self.probe("session.close." + session.engine.name, getattr(session, "question", None))
            await original_exit(session, *args)

        self.enterContext(patch.object(TrackingSession, "__aexit__", session_exit))

    async def asyncSetUp(self):
        lifecycle = app.router.lifespan_context(app)
        await lifecycle.__aenter__()
        self.addAsyncCleanup(lifecycle.__aexit__, None, None, None)
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                        base_url="http://test", trust_env=False)
        self.addAsyncCleanup(self.client.aclose)

    async def workflow(self, *, input, context, **kwargs):
        question = input["query"]
        self.contexts[question] = context
        for key in ("meta_mysql_repository", "dw_mysql_repository"):
            context[key].session.question = question
        self.probe("workflow.enter", question)
        try:
            yield "custom", {"type": "progress", "step": "请求追踪验证", "status": "running"}
            await asyncio.sleep(0)
            if question in ("断开", "取消"):
                self.waiting.set()
                await asyncio.Event().wait()
            await asyncio.to_thread(self.probe, "thread", question)
            self.probe("workflow.resume", question)
            if question == "流内失败":
                raise RuntimeError("offline workflow error")
            result = {"columns": ["question"], "rows": [{"question": question}], "row_count": 1, "truncated": False}
            yield "custom", {"type": "result", "data": result["rows"], "columns": result["columns"],
                             "row_count": 1, "truncated": False}
            yield "values", {"error": None, "sql_result": result}
        finally:
            await asyncio.sleep(0.01)
            self.probe("workflow.cleanup", question)

    def assert_question_records(self, question, request_id, phases):
        records = [record for record in self.records if record["question"] == question]
        self.assertTrue(records)
        self.assertEqual({record["request_id"] for record in records}, {request_id})
        self.assertTrue(set(phases).issubset({record["phase"] for record in records}))

    async def test_concurrent_sse_requests_keep_ids_in_workflow_threads_and_session_cleanup(self):
        first, second = await asyncio.gather(*[
            self.client.post("/api/query", json={"query": question}) for question in ("请求甲", "请求乙")
        ])
        identifiers = [response.headers[REQUEST_ID_HEADER] for response in (first, second)]
        self.assertNotEqual(*identifiers)
        for response, question, request_id in zip((first, second), ("请求甲", "请求乙"), identifiers):
            self.assertEqual(response.status_code, 200)
            self.assert_id(request_id)
            self.assertEqual(len(response.headers.get_list(REQUEST_ID_HEADER)), 1)
            self.assert_question_records(question, request_id,
                                         ("workflow.enter", "workflow.resume", "thread", "workflow.cleanup",
                                          "session.close.meta", "session.close.dw"))
        self.assertEqual(request_id_ctx_var.get(), "-")
        self.assertTrue(all(session.closed for session in self.harness.sessions))

    async def test_client_supplied_request_id_is_replaced_by_new_server_id_each_time(self):
        responses = []
        for _ in range(2):
            responses.append(await self.client.post("/api/query", json={"query": "重复问题"},
                                                    headers={REQUEST_ID_HEADER: "client-value"}))
        identifiers = [response.headers[REQUEST_ID_HEADER] for response in responses]
        for request_id in identifiers:
            self.assert_id(request_id)
            self.assertNotEqual(request_id, "client-value")
        self.assertNotEqual(*identifiers)

    async def test_real_langgraph_child_tasks_keep_same_id_as_response(self):
        async def node(state, runtime: Runtime[DataAgentContext]):
            question = state["query"]
            for key in ("meta_mysql_repository", "dw_mysql_repository"):
                runtime.context[key].session.question = question
            self.probe("node.enter", question)
            try:
                await asyncio.sleep(0)
                runtime.stream_writer({"type": "progress", "step": "真实节点追踪", "status": "running"})
                result = {"columns": ["value"], "rows": [], "row_count": 0, "truncated": False}
                runtime.stream_writer({"type": "result", "data": [], "columns": ["value"], "row_count": 0, "truncated": False})
                return {"error": None, "sql_result": result}
            finally:
                await asyncio.sleep(0.01)
                self.probe("node.cleanup", question)

        builder = StateGraph(DataAgentState, context_schema=DataAgentContext)
        builder.add_node("check", node)
        builder.add_edge(START, "check")
        builder.add_edge("check", END)
        with patch.object(service_module, "graph", builder.compile()):
            responses = await asyncio.gather(*[
                self.client.post("/api/query", json={"query": question}) for question in ("图甲", "图乙")
            ])
        for question, response in zip(("图甲", "图乙"), responses):
            request_id = response.headers[REQUEST_ID_HEADER]
            self.assert_id(request_id)
            self.assert_question_records(question, request_id, ("node.enter", "node.cleanup", "session.close.meta", "session.close.dw"))

    async def test_validation_and_unavailable_errors_have_ids_and_restore_parent_context(self):
        token = request_id_ctx_var.set("parent")
        try:
            invalid = await self.client.post("/api/query", json={"query": " "})
            self.assertEqual(invalid.status_code, 422)
            self.assert_id(invalid.headers[REQUEST_ID_HEADER])
            self.assertEqual(request_id_ctx_var.get(), "parent")
            with patch.object(self.harness.managers["embedding"], "client", None):
                unavailable = await self.client.post("/api/query", json={"query": "问题"})
            self.assertEqual(unavailable.status_code, 503)
            request_id = unavailable.headers[REQUEST_ID_HEADER]
            self.assert_id(request_id)
            error_records = [record for record in self.records if record["name"] == "app.api.exception_handlers"]
            self.assertTrue(error_records)
            self.assertEqual(error_records[-1]["request_id"], request_id)
            self.assertEqual(request_id_ctx_var.get(), "parent")
        finally:
            request_id_ctx_var.reset(token)

    async def test_unexpected_500_restores_id_for_outer_error_handler_and_header(self):
        token = request_id_ctx_var.set("parent")
        try:
            with patch.object(dependencies, "QueryService", side_effect=RuntimeError("offline-private-value")):
                response = await self.client.post("/api/query", json={"query": "合法问题"})
            self.assertEqual(request_id_ctx_var.get(), "parent")
        finally:
            request_id_ctx_var.reset(token)
        self.assertEqual(response.status_code, 500)
        request_id = response.headers[REQUEST_ID_HEADER]
        self.assert_id(request_id)
        self.assertEqual(len(response.headers.get_list(REQUEST_ID_HEADER)), 1)
        self.assertNotIn("offline-private-value", response.text)
        handler_logs = [record for record in self.records if record["name"] == "app.api.exception_handlers"]
        self.assertEqual(handler_logs[-1]["request_id"], request_id)
        session_logs = [record for record in self.records if record["phase"] and record["phase"].startswith("session.close")]
        self.assertEqual(len(session_logs), 2)
        self.assertEqual({record["request_id"] for record in session_logs}, {request_id})

    async def test_stream_error_log_and_cleanup_match_response_id(self):
        response = await self.client.post("/api/query", json={"query": "流内失败"})
        request_id = response.headers[REQUEST_ID_HEADER]
        self.assertEqual(response.status_code, 200)
        self.assertIn('"type": "error"', response.text)
        self.assert_question_records("流内失败", request_id, ("workflow.cleanup", "session.close.meta", "session.close.dw"))
        service_logs = [record for record in self.records if record["name"] == "app.services.query_service"]
        self.assertTrue(service_logs)
        self.assertEqual(service_logs[-1]["request_id"], request_id)

    async def test_docs_and_framework_errors_also_get_request_id(self):
        responses = [await self.client.get(path) for path in ("/docs", "/openapi.json", "/missing", "/api/query")]
        self.assertEqual([response.status_code for response in responses], [200, 200, 404, 405])
        identifiers = [response.headers[REQUEST_ID_HEADER] for response in responses]
        self.assertEqual(len(set(identifiers)), 4)
        for request_id in identifiers:
            self.assert_id(request_id)
        self.assertEqual(request_id_ctx_var.get(), "-")

    def raw_request(self, question, *, disconnect=False):
        messages, after_context = [], []
        incoming = asyncio.Queue()
        incoming.put_nowait({"type": "http.request", "body": json.dumps({"query": question}).encode(), "more_body": False})

        async def send(message):
            messages.append(message)
            if disconnect and message["type"] == "http.response.body" and message.get("body"):
                incoming.put_nowait({"type": "http.disconnect"})

        path = "/api/query"
        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
                 "http_version": "1.1", "method": "POST", "scheme": "http", "root_path": "",
                 "path": path, "raw_path": path.encode(), "query_string": b"",
                 "headers": [(b"content-type", b"application/json")],
                 "client": ("127.0.0.1", 12345), "server": ("test", 80)}

        async def run():
            try:
                await app(scope, incoming.get, send)
            finally:
                after_context.append(request_id_ctx_var.get())

        return asyncio.create_task(run()), messages, after_context

    def response_id(self, messages):
        start = next(message for message in messages if message["type"] == "http.response.start")
        return dict(start["headers"])[b"x-request-id"].decode()

    async def test_disconnect_cleanup_keeps_id_while_another_request_runs(self):
        task, messages, after = self.raw_request("断开", disconnect=True)
        _, normal = await asyncio.gather(task, self.client.post("/api/query", json={"query": "并行正常请求"}))
        disconnected_id = self.response_id(messages)
        normal_id = normal.headers[REQUEST_ID_HEADER]
        self.assertNotEqual(disconnected_id, normal_id)
        self.assert_question_records("断开", disconnected_id, ("workflow.cleanup", "session.close.meta", "session.close.dw"))
        self.assert_question_records("并行正常请求", normal_id, ("workflow.cleanup", "session.close.meta", "session.close.dw"))
        self.assertEqual(after, ["-"])
        self.assertTrue(all(session.closed for session in self.harness.sessions))

    async def test_external_cancellation_keeps_cleanup_id_and_resets_token(self):
        token = request_id_ctx_var.set("parent")
        try:
            task, messages, after = self.raw_request("取消")
            await asyncio.wait_for(self.waiting.wait(), timeout=3)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=3)
            self.assertEqual(request_id_ctx_var.get(), "parent")
        finally:
            request_id_ctx_var.reset(token)
        request_id = self.response_id(messages)
        self.assert_question_records("取消", request_id, ("workflow.cleanup", "session.close.meta", "session.close.dw"))
        self.assertEqual(after, ["parent"])
        self.assertTrue(all(session.closed for session in self.harness.sessions))


if __name__ == "__main__":
    unittest.main()
