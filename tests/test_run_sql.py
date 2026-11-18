import asyncio
import json
import os
import unittest
from copy import deepcopy
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, START, StateGraph
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

with patch.dict(os.environ, {"OPENAI_API_KEY": "offline-test-key"}):
    import app.agent.graph as graph_module
    import app.agent.nodes.correct_sql as correction_node
    import app.agent.nodes.generate_sql as generate_node
    import app.agent.nodes.run_sql as execution_node
    import app.agent.nodes.validate_sql as validation_node
from app.core.sql_result import build_sql_result
from app.core.sql_validation import SQLValidationError
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository


class SQLResultTests(unittest.TestCase):
    def test_native_values_are_json_safe_and_decimal_precision_is_preserved(self):
        columns = ["amount", "date", "datetime", "time", "duration", "binary", "optional", "count", "flag", "ratio"]
        values = [Decimal("123456789012345.67"), date(2026, 11, 17), datetime(2026, 11, 17, 10, 30),
                  time(10, 30), timedelta(hours=26), b"\x00\xff", None, 3, True, 1.5]
        result = build_sql_result(columns, [values], 1000)
        row = result["rows"][0]
        self.assertEqual(row, {"amount": "123456789012345.67", "date": "2026-11-17",
                               "datetime": "2026-11-17T10:30:00", "time": "10:30:00",
                               "duration": "1 day, 2:00:00", "binary": "0x00ff", "optional": None,
                               "count": 3, "flag": True, "ratio": 1.5})
        self.assertEqual(json.loads(json.dumps(result, allow_nan=False)), result)

    def test_duplicate_column_names_do_not_silently_overwrite_values(self):
        with self.assertRaisesRegex(ValueError, "重复列名"):
            build_sql_result(["amount", "amount"], [[1, 2]], 1000)

    def test_unsupported_values_and_nonfinite_floats_are_reported(self):
        for value in (object(), float("inf"), float("nan")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                build_sql_result(["value"], [[value]], 1000)


class DWSQLExecutionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # Actual SQLAlchemy statements/results; only the async bridge is adapted for SQLite.
        self.engine = create_engine("sqlite://")
        self.connection = self.engine.connect()
        self.connection.exec_driver_sql("CREATE TABLE demo_numbers (sample_value INTEGER)")
        self.connection.exec_driver_sql("INSERT INTO demo_numbers VALUES (1), (2), (3)")
        self.statements = []
        self.results = []

        def execute(statement, **kwargs):
            self.statements.append((statement, kwargs))
            result = self.connection.exec_driver_sql(statement, **kwargs)
            self.results.append(result)
            return result

        self.sync_connection = SimpleNamespace(exec_driver_sql=execute)
        self.async_connection = SimpleNamespace(run_sync=AsyncMock(side_effect=lambda fn: fn(self.sync_connection)))
        self.session = SimpleNamespace(connection=AsyncMock(return_value=self.async_connection))
        self.repository = DWMySQLRepository(self.session)

    def tearDown(self):
        self.connection.close()
        self.engine.dispose()

    async def test_query_returns_named_rows_and_closes_cursor(self):
        result = await self.repository.execute_sql(
            "SELECT sample_value FROM demo_numbers ORDER BY sample_value"
        )
        self.assertEqual(result, {"columns": ["sample_value"],
                                  "rows": [{"sample_value": 1}, {"sample_value": 2}, {"sample_value": 3}],
                                  "row_count": 3, "truncated": False})
        self.assertTrue(self.results[-1].closed)

    async def test_aggregate_is_returned_as_one_row(self):
        result = await self.repository.execute_sql("SELECT SUM(sample_value) AS total FROM demo_numbers")
        self.assertEqual(result["rows"], [{"total": 6}])
        self.assertEqual(result["row_count"], 1)

    async def test_empty_query_result_preserves_column_metadata(self):
        result = await self.repository.execute_sql("SELECT sample_value FROM demo_numbers WHERE 1 = 0")
        self.assertEqual(result, {"columns": ["sample_value"], "rows": [], "row_count": 0, "truncated": False})

    async def test_null_aggregate_is_not_confused_with_no_rows(self):
        result = await self.repository.execute_sql("SELECT SUM(sample_value) AS total FROM demo_numbers WHERE 1 = 0")
        self.assertEqual(result["rows"], [{"total": None}])
        self.assertEqual(result["row_count"], 1)

    async def test_row_cap_reports_truncation_without_modifying_sql(self):
        sql = "SELECT sample_value FROM demo_numbers ORDER BY sample_value"
        result = await self.repository.execute_sql(sql, max_rows=2)
        self.assertEqual(result["rows"], [{"sample_value": 1}, {"sample_value": 2}])
        self.assertEqual(result["row_count"], 2)
        self.assertTrue(result["truncated"])
        self.assertEqual(self.statements[-1][0], sql)
        self.assertTrue(self.statements[-1][1]["execution_options"]["stream_results"])
        self.assertTrue(self.results[-1].closed)

    async def test_exact_cap_and_existing_limit_do_not_report_truncation(self):
        for sql, cap, count in (("SELECT sample_value FROM demo_numbers", 3, 3),
                                ("SELECT sample_value FROM demo_numbers LIMIT 1", 2, 1)):
            with self.subTest(sql=sql):
                result = await self.repository.execute_sql(sql, max_rows=cap)
                self.assertFalse(result["truncated"])
                self.assertEqual(result["row_count"], count)

    async def test_colon_and_percent_literals_are_preserved(self):
        result = await self.repository.execute_sql("SELECT ':name; %Y-%m' AS label")
        self.assertEqual(result["rows"], [{"label": ":name; %Y-%m"}])
        self.assertTrue(self.statements[-1][1]["execution_options"]["no_parameters"])

    async def test_invalid_or_unsafe_sql_never_opens_a_connection(self):
        for sql in ("DELETE FROM demo_numbers", "SELECT 1; SELECT 2", "SELECT SLEEP(1)"):
            with self.subTest(sql=sql), self.assertRaises(SQLValidationError):
                await self.repository.execute_sql(sql)
        self.session.connection.assert_not_awaited()
        self.assertEqual(self.statements, [])

    async def test_invalid_row_cap_is_rejected_before_database_access(self):
        for cap in (0, -1, True, "2"):
            with self.subTest(cap=cap), self.assertRaises(ValueError):
                await self.repository.execute_sql("SELECT 1", max_rows=cap)
        self.session.connection.assert_not_awaited()

    async def test_mapping_failure_still_closes_cursor(self):
        with self.assertRaisesRegex(ValueError, "重复列名"):
            await self.repository.execute_sql("SELECT 1 AS duplicate, 2 AS duplicate")
        self.assertTrue(self.results[-1].closed)

    async def test_database_execution_error_propagates(self):
        with self.assertRaises(OperationalError):
            await self.repository.execute_sql("SELECT * FROM nonexistent_table")


class RunSQLTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.sql = "SELECT SUM(sample_value) AS total FROM demo_numbers"
        self.result = {"columns": ["total"], "rows": [{"total": "6.00"}], "row_count": 1, "truncated": False}
        self.repository = SimpleNamespace(execute_sql=AsyncMock(return_value=self.result))
        self.events = []
        self.runtime = SimpleNamespace(
            stream_writer=self.events.append, context={"dw_mysql_repository": self.repository}
        )
        self.state = {"sql": self.sql, "validated_sql": self.sql, "error": None}
        logger_patch = patch.object(execution_node, "logger")
        logger_patch.start()
        self.addCleanup(logger_patch.stop)

    async def test_executes_exact_validated_sql_saves_result_and_emits_data(self):
        before = deepcopy(self.state)
        update = await execution_node.run_sql(self.state, self.runtime)
        self.repository.execute_sql.assert_awaited_once_with(self.sql)
        self.assertEqual(update, {"sql_result": self.result, "error": None})
        self.assertEqual(self.state, before)
        self.assertEqual(self.events[-1], {"type": "result", "data": self.result["rows"],
                                          "columns": ["total"], "row_count": 1, "truncated": False})
        self.assertEqual([event["status"] for event in self.events if event["type"] == "progress"],
                         ["running", "success"])

    async def test_unvalidated_changed_or_failed_sql_does_not_reach_repository(self):
        for sql, validated, error in ((self.sql, None, None), (self.sql, "", None),
                                      (self.sql, "SELECT 1", None), (self.sql, self.sql, "failed")):
            with self.subTest(validated=validated, error=error):
                self.state.update(sql=sql, validated_sql=validated, error=error)
                with self.assertRaisesRegex(ValueError, "禁止执行"):
                    await execution_node.run_sql(self.state, self.runtime)
        self.repository.execute_sql.assert_not_awaited()
        self.assertFalse(any(event["type"] == "result" for event in self.events))

    async def test_empty_and_truncated_results_are_successes_with_metadata(self):
        for result in ({"columns": ["total"], "rows": [], "row_count": 0, "truncated": False},
                       {"columns": ["total"], "rows": [{"total": 1}], "row_count": 1, "truncated": True}):
            with self.subTest(result=result):
                self.repository.execute_sql.return_value = result
                update = await execution_node.run_sql(self.state, self.runtime)
                self.assertEqual(update["sql_result"], result)
                self.assertEqual(self.events[-1]["truncated"], result["truncated"])

    async def test_database_error_propagates_without_result_event(self):
        self.repository.execute_sql.side_effect = RuntimeError("database unavailable")
        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            await execution_node.run_sql(self.state, self.runtime)
        self.assertEqual(self.events[-1]["status"], "error")
        self.assertFalse(any(event["type"] == "result" for event in self.events))

    async def test_timeout_cancels_work_and_does_not_emit_result(self):
        cancelled = []

        async def slow(_):
            try:
                await asyncio.sleep(1)
            finally:
                cancelled.append(True)

        self.repository.execute_sql.side_effect = slow
        with patch.object(execution_node, "EXECUTION_TIMEOUT_SECONDS", 0.01):
            with self.assertRaises(TimeoutError):
                await execution_node.run_sql(self.state, self.runtime)
        self.assertEqual(cancelled, [True])
        self.assertEqual(self.events[-1]["status"], "error")
        self.assertFalse(any(event["type"] == "result" for event in self.events))


class ExecutionWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.sql = "SELECT SUM(sample_value) AS total FROM demo_numbers"
        self.result = {"columns": ["total"], "rows": [{"total": 6}], "row_count": 1, "truncated": False}
        self.repository = SimpleNamespace(validate_sql=AsyncMock(return_value=self.sql),
                                          execute_sql=AsyncMock(return_value=self.result))
        self.state = {
            "query": "统计示例数值总和", "metric_infos": [],
            "table_infos": [{"name": "demo_numbers", "role": "fact", "description": "示例表",
                             "columns": [{"name": "sample_value", "type": "int", "role": "measure",
                                          "description": "示例数值", "examples": [], "alias": []}]}],
            "date_info": {"date": "2026-11-17", "weekday": "星期二", "quarter": "Q4", "timezone": "Asia/Shanghai"},
            "db_info": {"dialect": "mysql", "version": "8.0.46"},
            "sql_correction_attempts": 2,
            "sql_result": {"columns": ["old"], "rows": [{"old": 99}], "row_count": 1, "truncated": False},
        }
        for node in (generate_node, validation_node, correction_node, execution_node):
            logger_patch = patch.object(node, "logger")
            logger_patch.start()
            self.addCleanup(logger_patch.stop)

    def build_graph(self):
        builder = StateGraph(execution_node.DataAgentState,
                             context_schema=execution_node.DataAgentContext)
        nodes = {"generate_sql": generate_node.generate_sql, "validate_sql": validation_node.validate_sql,
                 "correct_sql": correction_node.correct_sql, "run_sql": execution_node.run_sql}
        for name, function in nodes.items():
            builder.add_node(name, function)
        builder.add_edge(START, "generate_sql")
        for source, target in graph_module.graph_builder.edges:
            if source in nodes and (target in nodes or target == END):
                builder.add_edge(source, target)
        for source in ("validate_sql", "correct_sql"):
            for branch in graph_module.graph_builder.branches[source].values():
                builder.add_conditional_edges(source, branch.path, path_map=branch.ends)
        return builder.compile()

    async def invoke(self):
        with patch.object(generate_node, "llm", RunnableLambda(lambda _: self.sql)):
            return await self.build_graph().ainvoke(
                self.state, context={"dw_mysql_repository": self.repository}
            )

    async def test_generation_validation_execution_saves_result_and_resets_old_state(self):
        trace = []

        async def validate(sql):
            trace.append("validate")
            return sql

        async def execute(sql):
            trace.append("execute")
            return self.result

        self.repository.validate_sql.side_effect = validate
        self.repository.execute_sql.side_effect = execute
        result = await self.invoke()
        self.assertEqual(trace, ["validate", "execute"])
        self.assertEqual(result["sql_result"], self.result)
        self.assertEqual(result["validated_sql"], self.sql)
        self.assertEqual(result["sql_correction_attempts"], 0)

    async def test_correction_is_revalidated_and_only_fixed_sql_is_executed(self):
        corrected = "SELECT SUM(sample_value) AS total FROM demo_numbers WHERE sample_value > 0"
        self.repository.validate_sql.side_effect = [SQLValidationError("example error"), corrected]
        with patch.object(correction_node, "llm", RunnableLambda(lambda _: corrected)):
            result = await self.invoke()
        self.repository.execute_sql.assert_awaited_once_with(corrected)
        self.assertEqual(result["sql_correction_attempts"], 1)
        self.assertEqual(result["sql_result"], self.result)

    async def test_exhausted_correction_clears_old_result_and_never_executes(self):
        self.repository.validate_sql.side_effect = SQLValidationError("example error")
        with patch.object(correction_node, "llm", RunnableLambda(lambda _: self.sql)):
            result = await self.invoke()
        self.repository.execute_sql.assert_not_awaited()
        self.assertIn("已达上限", result["error"])
        self.assertIsNone(result["sql_result"])

    async def test_execution_failure_does_not_start_another_correction_loop(self):
        self.repository.execute_sql.side_effect = RuntimeError("execution failed")
        with patch.object(correction_node, "llm", None):
            with self.assertRaisesRegex(RuntimeError, "execution failed"):
                await self.invoke()
        self.repository.validate_sql.assert_awaited_once()
        self.repository.execute_sql.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
