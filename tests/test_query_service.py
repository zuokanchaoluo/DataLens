import asyncio
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, START, StateGraph
from langgraph.runtime import Runtime

with patch.dict(os.environ, {"OPENAI_API_KEY": "offline-test-key"}):
    import app.agent.graph as graph_module
    import app.agent.nodes.correct_sql as correction_node
    import app.agent.nodes.run_sql as execution_node
    import app.agent.nodes.validate_sql as validation_node
    import app.services.query_service as service_module
from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState
from app.core.sql_validation import SQLValidationError


def sql_result(rows=None):
    rows = [{"total_amount": "41099.50"}] if rows is None else rows
    return {"columns": ["total_amount"], "rows": rows,
            "row_count": len(rows), "truncated": False}


def result_event(result):
    return {"type": "result", "data": result["rows"], "columns": result["columns"],
            "row_count": result["row_count"], "truncated": result["truncated"]}


def progress(step="执行SQL", status="running"):
    return {"type": "progress", "step": step, "status": status}


def decode(frame):
    if not frame.startswith("data: ") or not frame.endswith("\n\n"):
        raise AssertionError("Invalid SSE frame")
    return json.loads(frame[6:-2])


class QueryServiceTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.dependencies = {key: object() for key in DataAgentContext.__annotations__}
        self.service = service_module.QueryService(**self.dependencies)
        self.result = sql_result()
        self.final_state = {"error": None, "sql_result": self.result}
        logger_patch = patch.object(service_module, "logger")
        self.logger = logger_patch.start()
        self.addCleanup(logger_patch.stop)

    def install_stream(self, script):
        async def stream(**kwargs):
            for item in script:
                if isinstance(item, BaseException):
                    raise item
                yield item

        fake_graph = SimpleNamespace(astream=Mock(side_effect=stream))
        graph_patch = patch.object(service_module, "graph", fake_graph)
        graph_patch.start()
        self.addCleanup(graph_patch.stop)
        return fake_graph

    async def collect(self, query="统计华北地区的销售总额"):
        return [decode(frame) async for frame in self.service.query(query)]

    def assert_error(self, events):
        self.assertEqual(events[-1], {"type": "error", "message": service_module.QUERY_ERROR_MESSAGE})
        self.assertEqual(sum(event["type"] == "error" for event in events), 1)
        self.assertFalse(any(event["type"] == "result" for event in events))


class QueryServiceTests(QueryServiceTestCase):
    async def test_lazy_start_progress_order_and_one_result_without_internal_state(self):
        internal_state = {**self.final_state, "retrieved_column_infos": [object()]}
        fake_graph = self.install_stream([
            ("values", {"query": "示例"}),
            ("custom", {**progress(), "internal": object()}),
            ("custom", progress(status="success")),
            ("custom", {**result_event(self.result), "internal": object()}),
            ("values", internal_state),
        ])
        stream = self.service.query("示例")
        fake_graph.astream.assert_not_called()
        events = [decode(frame) async for frame in stream]
        self.assertEqual(events, [progress(), progress(status="success"), result_event(self.result)])
        arguments = fake_graph.astream.call_args.kwargs
        self.assertEqual(arguments["input"], {"query": "示例"})
        self.assertEqual(arguments["context"], self.dependencies)
        self.assertEqual(arguments["stream_mode"], ["custom", "values"])
        self.assertEqual(arguments["version"], "v1")

    async def test_legacy_progress_and_unknown_custom_events(self):
        self.install_stream([
            ("custom", "抽取关键词"), ("custom", "召回字段信息"),
            ("custom", "内部调试信息"), ("custom", {"type": "debug", "secret": object()}),
            ("custom", [object()]),
            ("custom", result_event(self.result)), ("values", self.final_state),
        ])
        events = await self.collect()
        self.assertEqual(events, [progress("抽取关键词"), progress("召回字段信息"), result_event(self.result)])

    async def test_empty_result_preserves_columns_and_truncation_metadata(self):
        result = sql_result([])
        self.install_stream([("custom", result_event(result)),
                             ("values", {"error": None, "sql_result": result})])
        self.assertEqual(await self.collect(), [result_event(result)])

    async def test_recoverable_progress_error_does_not_end_query(self):
        failed = progress("校验SQL", "error")
        repaired = progress("校正SQL", "success")
        self.install_stream([
            ("custom", failed), ("values", {"error": "中间错误", "sql_result": None}),
            ("custom", repaired), ("custom", result_event(self.result)),
            ("values", self.final_state),
        ])
        self.assertEqual(await self.collect(), [failed, repaired, result_event(self.result)])

    async def test_final_state_error_ends_with_safe_error_even_without_exception(self):
        self.install_stream([("custom", progress("校正SQL", "error")),
                             ("values", {"error": "已达上限，敏感原始错误", "sql_result": None})])
        self.assert_error(await self.collect())

    async def test_runtime_exceptions_become_one_safe_error(self):
        for prefix in ([], [("custom", progress())]):
            with self.subTest(prefix=prefix):
                self.install_stream(prefix + [RuntimeError("password=private-value")])
                events = await self.collect()
                self.assert_error(events)
                self.assertNotIn("private-value", json.dumps(events))
                self.assertNotIn("private-value", str(self.logger.mock_calls))

    async def test_missing_final_state_result_event_or_execution_result_is_failure(self):
        scripts = [[], [("values", self.final_state)], [("custom", result_event(self.result))],
                   [("custom", result_event(self.result)), ("values", {"error": None, "sql_result": None})]]
        for script in scripts:
            with self.subTest(script=script):
                self.install_stream(script)
                self.assert_error(await self.collect())

    async def test_result_waits_for_successful_end_and_duplicates_are_not_repeated(self):
        self.install_stream([("custom", result_event(self.result)),
                             ("custom", result_event(self.result)), ("values", self.final_state)])
        self.assertEqual(await self.collect(), [result_event(self.result)])

    async def test_result_followed_by_final_error_or_exception_is_not_sent(self):
        endings = [[("values", {"error": "失败", "sql_result": None})], [RuntimeError("失败")]]
        for ending in endings:
            with self.subTest(ending=ending):
                self.install_stream([("custom", result_event(self.result))] + ending)
                self.assert_error(await self.collect())

    async def test_non_json_progress_or_result_becomes_safe_error(self):
        for item in (progress(object()), {**result_event(self.result), "data": [{"value": float("nan")}]}):
            with self.subTest(item=item):
                self.install_stream([("custom", item), ("values", self.final_state)])
                self.assert_error(await self.collect())

    async def test_cancellation_propagates_and_closes_workflow_stream(self):
        closed = []

        async def stream(**kwargs):
            try:
                yield "custom", progress()
                raise asyncio.CancelledError()
            finally:
                closed.append(True)

        with patch.object(service_module, "graph", SimpleNamespace(astream=stream)):
            generator = self.service.query("示例")
            self.assertEqual(decode(await anext(generator)), progress())
            with self.assertRaises(asyncio.CancelledError):
                await anext(generator)
        self.assertEqual(closed, [True])
        self.logger.error.assert_not_called()

    async def test_consumer_closing_generator_closes_workflow_stream(self):
        closed = []

        async def stream(**kwargs):
            try:
                yield "custom", progress()
                self.fail("关闭流后不应继续工作流")
            finally:
                closed.append(True)

        with patch.object(service_module, "graph", SimpleNamespace(astream=stream)):
            generator = self.service.query("示例")
            await anext(generator)
            await generator.aclose()
        self.assertEqual(closed, [True])

    async def test_overlapping_queries_have_independent_state_context_and_results(self):
        inputs, contexts = [], []

        async def stream(**kwargs):
            state, context = kwargs["input"], kwargs["context"]
            inputs.append(state)
            contexts.append(context)
            yield "custom", progress()
            await asyncio.sleep(0)
            result = sql_result([{"total_amount": state["query"]}])
            yield "custom", result_event(result)
            yield "values", {"error": None, "sql_result": result}

        with patch.object(service_module, "graph", SimpleNamespace(astream=stream)):
            first, second = await asyncio.gather(self.collect("问题一"), self.collect("问题二"))
        self.assertIsNot(inputs[0], inputs[1])
        self.assertIsNot(contexts[0], contexts[1])
        for context in contexts:
            for key, dependency in self.dependencies.items():
                self.assertIs(context[key], dependency)
        self.assertEqual(first[-1]["data"], [{"total_amount": "问题一"}])
        self.assertEqual(second[-1]["data"], [{"total_amount": "问题二"}])


class QueryServiceWorkflowTests(QueryServiceTestCase):
    async def run_workflow(self, *, invalid=False, repair=False, rows=None):
        good_sql = "SELECT SUM(sample_value) AS total_amount FROM demo_numbers"
        bad_sql = "SELECT SUM(missing_value) FROM demo_numbers"
        result = sql_result(rows)

        async def validate(sql):
            if sql == bad_sql:
                raise SQLValidationError("未知字段 missing_value")
            return sql

        repository = SimpleNamespace(validate_sql=AsyncMock(side_effect=validate),
                                     execute_sql=AsyncMock(return_value=result))
        self.service.dw_mysql_repository = repository

        async def seed(state, runtime: Runtime[DataAgentContext]):
            runtime.stream_writer("抽取关键词")
            return {"sql": bad_sql if invalid else good_sql, "error": None,
                    "sql_correction_attempts": 0, "sql_result": None,
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
        # 复用正式工作流的分支规则，确认正常 END 的失败也能被服务识别。
        for source in ("validate_sql", "correct_sql"):
            for branch in graph_module.graph_builder.branches[source].values():
                builder.add_conditional_edges(source, branch.path, path_map=branch.ends)

        with (patch.object(service_module, "graph", builder.compile()),
              patch.object(correction_node, "llm", RunnableLambda(lambda _: good_sql if repair else bad_sql)),
              patch.object(validation_node, "logger"), patch.object(correction_node, "logger"),
              patch.object(execution_node, "logger")):
            events = await self.collect()
        return events, repository

    async def test_real_langgraph_stream_and_execution_node_preserve_empty_result(self):
        events, repository = await self.run_workflow(rows=[])
        self.assertEqual(events[0], progress("抽取关键词"))
        self.assertEqual(events[-1], result_event(sql_result([])))
        self.assertEqual(sum(event["type"] == "result" for event in events), 1)
        repository.execute_sql.assert_awaited_once()

    async def test_real_correction_can_recover_after_validation_error(self):
        events, repository = await self.run_workflow(invalid=True, repair=True)
        self.assertIn(progress("校验SQL", "error"), events)
        self.assertEqual(events[-1]["type"], "result")
        self.assertEqual(repository.validate_sql.await_count, 2)
        repository.execute_sql.assert_awaited_once()

    async def test_real_correction_exhaustion_sends_terminal_error_without_execution(self):
        events, repository = await self.run_workflow(invalid=True)
        self.assert_error(events)
        self.assertEqual(repository.validate_sql.await_count, correction_node.MAX_SQL_CORRECTIONS + 1)
        repository.execute_sql.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
