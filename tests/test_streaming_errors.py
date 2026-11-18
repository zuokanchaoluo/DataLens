import asyncio
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import anyio
from langgraph.graph import END, START, StateGraph
from langgraph.runtime import Runtime
from langchain_core.runnables import RunnableLambda
from starlette.requests import ClientDisconnect

with patch.dict(os.environ, {"OPENAI_API_KEY": "offline-test-key"}):
    import app.agent.graph as graph_module
    import app.agent.nodes.correct_sql as correction_node
    import app.agent.nodes.run_sql as execution_node
    import app.agent.nodes.validate_sql as validation_node
    import app.api.exception_handlers as handlers
    import app.services.query_service as service_module

from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState
from app.core.sql_validation import SQLValidationError
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository
from test_app_lifespan import LifecycleHarness, TrackingSession, app


def progress(step="流内验证", status="running"):
    return {"type": "progress", "step": step, "status": status}


class StreamingErrorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.harness = self.enterContext(LifecycleHarness())
        self.logger = self.enterContext(patch.object(service_module, "logger"))
        self.enterContext(patch.object(handlers, "logger"))
        self.workflows = []
        self.service_streams = []
        self.cleanup = []
        self.contexts = []
        original_query = service_module.QueryService.query

        def query(service, question):
            stream = original_query(service, question)
            self.service_streams.append(stream)
            return stream

        self.enterContext(patch.object(service_module.QueryService, "query", query))

    async def asyncSetUp(self):
        lifecycle = app.router.lifespan_context(app)
        await lifecycle.__aenter__()
        self.addAsyncCleanup(lifecycle.__aexit__, None, None, None)
        self.addAsyncCleanup(self.close_leftover_streams)

    async def close_leftover_streams(self):
        # 回归失败时也显式收尾；断言前不依赖 GC 或事件循环退出清理。
        for stream in self.service_streams + self.workflows:
            await stream.aclose()

    def install_workflow(self, workflow):
        def astream(**kwargs):
            stream = workflow(**kwargs)
            self.workflows.append(stream)
            return stream

        self.enterContext(patch.object(service_module, "graph", SimpleNamespace(astream=astream)))

    def make_request(self, *, version="2.4", question="问题", on_send=None, cancel_scope=None):
        messages = []
        incoming = asyncio.Queue()
        self.incoming = incoming
        incoming.put_nowait({"type": "http.request", "body": json.dumps({"query": question}).encode(),
                             "more_body": False})

        async def send(message):
            messages.append(message)
            if on_send is not None:
                await on_send(message, incoming)

        path = "/api/query"
        scope = {
            "type": "http", "asgi": {"version": "3.0", "spec_version": version},
            "http_version": "1.1", "method": "POST", "scheme": "http", "root_path": "",
            "path": path, "raw_path": path.encode(), "query_string": b"",
            "headers": [(b"content-type", b"application/json")],
            "client": ("127.0.0.1", 12345), "server": ("test", 80),
        }
        async def run():
            if cancel_scope is None:
                await app(scope, incoming.get, send)
            else:
                with cancel_scope:
                    await app(scope, incoming.get, send)

        self.tasks_before = set(asyncio.all_tasks())
        return asyncio.create_task(run()), messages

    def events(self, messages):
        body = b"".join(message.get("body", b"") for message in messages
                        if message["type"] == "http.response.body")
        return [json.loads(frame[6:]) for frame in body.decode().split("\n\n") if frame]

    def assert_released(self):
        self.assertTrue(all(session.closed for session in self.harness.sessions))
        self.assertTrue(all(not resource.closed for resource in self.harness.resources))
        self.assertFalse([task for task in asyncio.all_tasks()
                          if task not in self.tasks_before and task is not asyncio.current_task() and not task.done()])

    async def test_send_failure_closes_paused_generators_before_sessions(self):
        async def workflow(**kwargs):
            try:
                yield "custom", progress()
                self.fail("发送失败后不能继续工作流")
            finally:
                self.assertTrue(all(not session.closed for session in self.harness.sessions))
                await asyncio.sleep(0.01)
                self.cleanup.append("workflow")

        async def send(message, incoming):
            if message["type"] == "http.response.body" and message.get("body"):
                raise OSError("offline closed socket")

        self.install_workflow(workflow)
        task, messages = self.make_request(on_send=send)
        try:
            await asyncio.wait_for(task, timeout=3)
        except ClientDisconnect:
            pass
        self.assertEqual(self.cleanup, ["workflow"])
        self.assertTrue(all(stream.ag_frame is None for stream in self.service_streams + self.workflows))
        self.assert_released()

    async def test_disconnect_during_workflow_wait_cancels_it_for_asgi_24(self):
        async def workflow(**kwargs):
            try:
                yield "custom", progress()
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0.01)
                self.cleanup.append("workflow")

        async def send(message, incoming):
            if message["type"] == "http.response.body" and message.get("body"):
                incoming.put_nowait({"type": "http.disconnect"})

        self.install_workflow(workflow)
        task, messages = self.make_request(on_send=send)
        await asyncio.wait_for(task, timeout=3)
        self.assertEqual(self.cleanup, ["workflow"])
        self.assertEqual(self.events(messages), [progress()])
        self.assert_released()

    async def test_real_langgraph_node_finishes_async_cleanup_before_sessions_close(self):
        async def node(state, runtime: Runtime[DataAgentContext]):
            try:
                runtime.stream_writer(progress())
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0.02)
                for key in ("meta_mysql_repository", "dw_mysql_repository"):
                    self.assertFalse(runtime.context[key].session.closed)
                self.cleanup.append("node")
            return {}

        builder = StateGraph(DataAgentState, context_schema=DataAgentContext)
        builder.add_node("wait", node)
        builder.add_edge(START, "wait")
        builder.add_edge("wait", END)
        self.enterContext(patch.object(service_module, "graph", builder.compile()))

        async def send(message, incoming):
            if message["type"] == "http.response.body" and message.get("body"):
                incoming.put_nowait({"type": "http.disconnect"})

        task, messages = self.make_request(version="2.3", on_send=send)
        await asyncio.wait_for(task, timeout=3)
        self.assertEqual(self.cleanup, ["node"])
        self.assertEqual(self.events(messages), [progress()])
        self.assert_released()

    async def test_anyio_cancel_scope_allows_async_workflow_and_session_cleanup(self):
        waiting = asyncio.Event()
        original_exit = TrackingSession.__aexit__

        async def slow_exit(session, *args):
            await asyncio.sleep(0.01)
            await original_exit(session, *args)

        async def workflow(**kwargs):
            try:
                yield "custom", progress()
                waiting.set()
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0.01)
                self.assertTrue(all(not session.closed for session in self.harness.sessions))
                self.cleanup.append("workflow")

        self.install_workflow(workflow)
        with patch.object(TrackingSession, "__aexit__", slow_exit):
            scope = anyio.CancelScope()
            task, messages = self.make_request(cancel_scope=scope)
            await asyncio.wait_for(waiting.wait(), timeout=3)
            scope.cancel()
            await asyncio.wait_for(task, timeout=3)
        self.assertEqual(self.cleanup, ["workflow"])
        self.assertEqual(self.events(messages), [progress()])
        self.assert_released()

    async def test_disconnect_before_first_event_sends_no_terminal_event(self):
        async def workflow(**kwargs):
            try:
                await asyncio.Event().wait()
                yield "custom", progress()
            finally:
                await asyncio.sleep(0.01)
                self.cleanup.append("workflow")

        async def send(message, incoming):
            if message["type"] == "http.response.start":
                incoming.put_nowait({"type": "http.disconnect"})

        self.install_workflow(workflow)
        task, messages = self.make_request(on_send=send)
        await asyncio.wait_for(task, timeout=3)
        self.assertEqual(self.cleanup, ["workflow"])
        self.assertEqual(self.events(messages), [])
        self.assert_released()

    async def test_request_task_cancellation_during_generation_and_send_closes_stream(self):
        for phase in ("generation", "send"):
            with self.subTest(phase=phase):
                waiting = asyncio.Event()

                async def workflow(**kwargs):
                    try:
                        yield "custom", progress()
                        waiting.set()
                        await asyncio.Event().wait()
                    finally:
                        await asyncio.sleep(0.01)
                        self.cleanup.append(phase)

                async def send(message, incoming):
                    if phase == "send" and message["type"] == "http.response.body" and message.get("body"):
                        waiting.set()
                        await asyncio.Event().wait()

                self.install_workflow(workflow)
                task, messages = self.make_request(on_send=send)
                await asyncio.wait_for(waiting.wait(), timeout=3)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(task, timeout=3)
                self.assertEqual(self.events(messages), [progress()])
                self.assertEqual(self.cleanup[-1], phase)
                self.assert_released()

    async def test_cleanup_failure_on_disconnect_does_not_yield_an_error(self):
        async def workflow(**kwargs):
            try:
                yield "custom", progress()
            finally:
                await asyncio.sleep(0.01)
                self.cleanup.append("workflow")
                raise RuntimeError("offline-private-cleanup-value")

        async def send(message, incoming):
            if message["type"] == "http.response.body" and message.get("body"):
                raise OSError("offline disconnected")

        self.install_workflow(workflow)
        task, messages = self.make_request(on_send=send)
        await asyncio.wait_for(task, timeout=3)
        self.assertEqual(self.events(messages), [progress()])
        self.assertEqual(self.cleanup, ["workflow"])
        self.assertNotIn("offline-private-cleanup-value", str(self.logger.mock_calls))
        self.assert_released()

    async def test_disconnect_during_workflow_cleanup_waits_for_that_cleanup(self):
        cleaning = asyncio.Event()

        async def workflow(**kwargs):
            try:
                yield "custom", progress(step=object())
            finally:
                cleaning.set()
                await asyncio.sleep(0.03)
                self.cleanup.append("workflow")

        self.install_workflow(workflow)
        task, messages = self.make_request()
        await asyncio.wait_for(cleaning.wait(), timeout=3)
        self.incoming.put_nowait({"type": "http.disconnect"})
        await asyncio.wait_for(task, timeout=3)
        self.assertEqual(self.cleanup, ["workflow"])
        self.assertEqual(self.events(messages), [])
        self.assert_released()

    async def test_runtime_failure_after_progress_yields_one_safe_terminal_error(self):
        async def workflow(**kwargs):
            try:
                yield "custom", progress()
                raise RuntimeError("offline-private-failure")
            finally:
                self.cleanup.append("workflow")

        self.install_workflow(workflow)
        task, messages = self.make_request()
        await asyncio.wait_for(task, timeout=3)
        events = self.events(messages)
        self.assertEqual(events, [progress(), {"type": "error", "message": service_module.QUERY_ERROR_MESSAGE}])
        self.assertEqual([message["status"] for message in messages if message["type"] == "http.response.start"], [200])
        self.assertEqual(self.cleanup, ["workflow"])
        self.assertNotIn("offline-private-failure", json.dumps(events))
        self.assert_released()

    async def test_custom_terminal_error_stops_workflow_and_is_sanitized(self):
        async def workflow(**kwargs):
            try:
                yield "custom", {"type": "error", "message": "offline-private-error"}
                self.fail("终止事件之后不应继续消费工作流")
            finally:
                self.cleanup.append("workflow")

        self.install_workflow(workflow)
        task, messages = self.make_request()
        await asyncio.wait_for(task, timeout=3)
        self.assertEqual(self.events(messages), [{"type": "error", "message": service_module.QUERY_ERROR_MESSAGE}])
        self.assertEqual(self.cleanup, ["workflow"])
        self.assert_released()

    async def test_result_then_workflow_failure_emits_only_terminal_error(self):
        async def workflow(**kwargs):
            yield "custom", {"type": "result", "data": [], "columns": ["value"], "row_count": 0, "truncated": False}
            raise RuntimeError("offline failure after result")

        self.install_workflow(workflow)
        task, messages = self.make_request()
        await asyncio.wait_for(task, timeout=3)
        self.assertEqual(self.events(messages), [{"type": "error", "message": service_module.QUERY_ERROR_MESSAGE}])
        self.assert_released()

    async def test_encoding_and_cleanup_failure_still_emit_only_one_terminal_error(self):
        async def workflow(**kwargs):
            try:
                yield "custom", progress(step=object())
            finally:
                self.cleanup.append("workflow")
                raise RuntimeError("offline cleanup failure")

        self.install_workflow(workflow)
        task, messages = self.make_request()
        await asyncio.wait_for(task, timeout=3)
        self.assertEqual(self.events(messages), [{"type": "error", "message": service_module.QUERY_ERROR_MESSAGE}])
        self.assertEqual(self.cleanup, ["workflow"])
        self.assert_released()

    async def test_recoverable_progress_error_and_duplicate_results_keep_one_success(self):
        async def workflow(**kwargs):
            yield "custom", progress("校验SQL", "error")
            yield "custom", progress("校正SQL", "success")
            event = {"type": "result", "data": [], "columns": ["value"], "row_count": 0, "truncated": False}
            yield "custom", event
            yield "custom", event
            yield "values", {"error": None, "sql_result": {"rows": [], "columns": ["value"], "row_count": 0, "truncated": False}}

        self.install_workflow(workflow)
        task, messages = self.make_request()
        await asyncio.wait_for(task, timeout=3)
        self.assertEqual([event["type"] for event in self.events(messages)], ["progress", "progress", "result"])
        self.assertEqual(self.events(messages)[-1]["columns"], ["value"])
        self.assert_released()

    async def test_disconnect_after_terminal_result_does_not_create_another_terminal_event(self):
        async def workflow(**kwargs):
            yield "custom", {"type": "result", "data": [], "columns": ["value"], "row_count": 0, "truncated": False}
            yield "values", {"error": None, "sql_result": {"rows": [], "columns": ["value"], "row_count": 0, "truncated": False}}

        async def send(message, incoming):
            if message["type"] == "http.response.body" and message.get("body"):
                incoming.put_nowait({"type": "http.disconnect"})
                await asyncio.sleep(0)

        self.install_workflow(workflow)
        task, messages = self.make_request(on_send=send)
        await asyncio.wait_for(task, timeout=3)
        self.assertEqual([event["type"] for event in self.events(messages)], ["result"])
        self.assert_released()

    async def test_real_correction_limit_yields_one_error_and_never_executes_sql(self):
        sql = "SELECT missing_value FROM demo_numbers"

        async def seed(state):
            return {"sql": sql, "error": None, "sql_correction_attempts": 0, "sql_result": None,
                    "table_infos": [{"name": "demo_numbers", "columns": [{"name": "sample_value"}]}],
                    "metric_infos": [], "date_info": {}, "db_info": {}}

        builder = StateGraph(DataAgentState, context_schema=DataAgentContext)
        nodes = {"seed": seed, "validate_sql": validation_node.validate_sql,
                 "correct_sql": correction_node.correct_sql, "run_sql": execution_node.run_sql}
        for name, function in nodes.items():
            builder.add_node(name, function)
        builder.add_edge(START, "seed")
        builder.add_edge("seed", "validate_sql")
        builder.add_edge("run_sql", END)
        for source in ("validate_sql", "correct_sql"):
            for branch in graph_module.graph_builder.branches[source].values():
                builder.add_conditional_edges(source, branch.path, path_map=branch.ends)

        model = Mock(return_value=sql)
        with (patch.object(service_module, "graph", builder.compile()),
              patch.object(correction_node, "llm", RunnableLambda(model)),
              patch.object(validation_node, "logger"), patch.object(correction_node, "logger"),
              patch.object(DWMySQLRepository, "validate_sql", AsyncMock(side_effect=SQLValidationError("offline invalid SQL"))) as validate,
              patch.object(DWMySQLRepository, "execute_sql", AsyncMock()) as execute):
            task, messages = self.make_request()
            await asyncio.wait_for(task, timeout=3)
        terminal = [event for event in self.events(messages) if event["type"] in ("result", "error")]
        self.assertEqual(terminal, [{"type": "error", "message": service_module.QUERY_ERROR_MESSAGE}])
        self.assertEqual(model.call_count, correction_node.MAX_SQL_CORRECTIONS)
        self.assertEqual(validate.await_count, correction_node.MAX_SQL_CORRECTIONS + 1)
        execute.assert_not_awaited()
        self.assert_released()


if __name__ == "__main__":
    unittest.main()
