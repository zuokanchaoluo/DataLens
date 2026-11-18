import asyncio
import os
import unittest
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, START, StateGraph
from sqlalchemy.exc import OperationalError, ProgrammingError

with patch.dict(os.environ, {"OPENAI_API_KEY": "offline-test-key"}):
    import app.agent.graph as graph_module
    import app.agent.nodes.generate_sql as generate_node
    import app.agent.nodes.validate_sql as validation_node
from app.core.sql_validation import SQLValidationError, normalize_read_only_sql
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository


class SQLStructureTests(unittest.TestCase):
    def test_supports_aggregation_joins_cte_subqueries_and_union(self):
        queries = [
            "SELECT SUM(o.order_amount) FROM fact_order o JOIN dim_date d ON o.date_id=d.date_id WHERE d.year=2025",
            "WITH totals AS (SELECT SUM(order_amount) AS amount FROM fact_order) SELECT amount FROM totals",
            "SELECT * FROM (SELECT order_amount FROM fact_order) AS orders",
            "SELECT 1 AS n UNION ALL SELECT 2 AS n",
            "SELECT DATE_FORMAT(CURRENT_DATE(), '%Y-%m'), IFNULL(SUM(order_amount), 0) FROM fact_order",
        ]
        for sql in queries:
            with self.subTest(sql=sql):
                normalized = normalize_read_only_sql(sql)
                self.assertEqual(normalize_read_only_sql(normalized), normalized)

    def test_semicolons_and_sql_keywords_in_strings_are_data(self):
        self.assertEqual(normalize_read_only_sql("SELECT 'DELETE; :name' AS label;"),
                         "SELECT 'DELETE; :name' AS label")

    def test_ordinary_comments_and_trailing_comment_are_removed(self):
        self.assertEqual(normalize_read_only_sql("/* description */ SELECT 1; -- trailing comment"),
                         "SELECT 1")

    def test_empty_malformed_or_multiple_statements_are_rejected(self):
        for sql in (None, "", "  ", "-- comment only", "SELECT FROM", "SELECT 1; SELECT 2"):
            with self.subTest(sql=sql), self.assertRaises(SQLValidationError):
                normalize_read_only_sql(sql)

    def test_write_and_management_commands_are_rejected(self):
        for sql in ("DELETE FROM fact_order", "UPDATE fact_order SET order_amount=0",
                    "INSERT INTO fact_order VALUES (1)", "DROP TABLE fact_order",
                    "SHOW TABLES", "EXPLAIN SELECT 1", "CALL demo()",
                    "WITH deleted AS (DELETE FROM fact_order) SELECT * FROM deleted"):
            with self.subTest(sql=sql), self.assertRaises(SQLValidationError):
                normalize_read_only_sql(sql)

    def test_output_locking_variables_parameters_and_hints_are_rejected(self):
        for sql in ("SELECT 1 INTO @answer", "SELECT 1 INTO OUTFILE '/tmp/example.txt'",
                    "SELECT * FROM fact_order FOR UPDATE",
                    "SELECT * FROM fact_order LOCK IN SHARE MODE",
                    "SELECT @answer := 1", "SELECT @answer", "SELECT @@version",
                    "SELECT ?", "SELECT :amount", "SELECT /*+ MAX_EXECUTION_TIME(1000) */ 1"):
            with self.subTest(sql=sql), self.assertRaises(SQLValidationError):
                normalize_read_only_sql(sql)

    def test_unknown_and_side_effect_functions_are_rejected(self):
        for sql in ("SELECT demo_function(1)", "SELECT SLEEP(1)",
                    "SELECT BENCHMARK(10, 1+1)", "SELECT GET_LOCK('example', 1)",
                    "SELECT LOAD_FILE('/tmp/example.txt')", "SELECT demo.abs(1)"):
            with self.subTest(sql=sql), self.assertRaises(SQLValidationError):
                normalize_read_only_sql(sql)

    def test_executable_comments_cannot_change_query_semantics(self):
        for sql in ("SELECT 1 /*!50000 UNION SELECT 2 */",
                    "SELECT 1; /*!50000 DELETE FROM fact_order */",
                    "SELECT 1 /*M!100100 UNION SELECT 2 */"):
            with self.subTest(sql=sql), self.assertRaisesRegex(SQLValidationError, "可执行注释"):
                normalize_read_only_sql(sql)


class DWSQLValidationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.result = SimpleNamespace(close=Mock())
        self.connection = SimpleNamespace(exec_driver_sql=AsyncMock(return_value=self.result))
        self.session = SimpleNamespace(
            get_bind=Mock(return_value=SimpleNamespace(dialect=SimpleNamespace(name="mysql"))),
            connection=AsyncMock(return_value=self.connection),
        )
        self.repository = DWMySQLRepository(self.session)

    async def test_only_explains_normalized_query_and_closes_plan_result(self):
        sql = await self.repository.validate_sql("select sum(order_amount) from fact_order;")
        self.assertEqual(sql, "SELECT SUM(order_amount) FROM fact_order")
        self.connection.exec_driver_sql.assert_awaited_once_with(
            "EXPLAIN FORMAT=JSON SELECT SUM(order_amount) FROM fact_order",
            execution_options={"no_parameters": True},
        )
        self.result.close.assert_called_once_with()

    async def test_colon_and_percent_literals_are_not_interpreted_as_parameters(self):
        await self.repository.validate_sql("SELECT ':name;DELETE %Y-%m' AS label")
        self.connection.exec_driver_sql.assert_awaited_once_with(
            "EXPLAIN FORMAT=JSON SELECT ':name;DELETE %Y-%m' AS label",
            execution_options={"no_parameters": True},
        )

    async def test_invalid_sql_never_opens_a_database_connection(self):
        with self.assertRaises(SQLValidationError):
            await self.repository.validate_sql("DELETE FROM fact_order")
        self.session.connection.assert_not_awaited()

    async def test_missing_table_column_and_syntax_errors_can_be_corrected(self):
        for exception_type, code in ((ProgrammingError, 1146), (OperationalError, 1054),
                                     (ProgrammingError, 1064), (OperationalError, 1055)):
            with self.subTest(code=code):
                error = exception_type("EXPLAIN", None, Exception(code, "example SQL error"))
                self.connection.exec_driver_sql.side_effect = error
                with self.assertRaisesRegex(SQLValidationError, str(code)) as caught:
                    await self.repository.validate_sql("SELECT amount FROM demo")
                self.assertIs(caught.exception.__cause__, error)

    async def test_connection_permission_and_unknown_errors_are_not_sql_corrections(self):
        for code, invalidated in ((2003, False), (2013, True), (1142, False), (9999, False),
                                  (1054, True)):
            with self.subTest(code=code, invalidated=invalidated):
                error = OperationalError("EXPLAIN", None, Exception(code, "service error"),
                                         connection_invalidated=invalidated)
                self.connection.exec_driver_sql.side_effect = error
                with self.assertRaises(OperationalError) as caught:
                    await self.repository.validate_sql("SELECT 1")
                self.assertIs(caught.exception, error)

    async def test_unsupported_database_is_not_treated_as_a_query_error(self):
        self.session.get_bind.return_value.dialect.name = "sqlite"
        with self.assertRaisesRegex(RuntimeError, "仅支持 MySQL"):
            await self.repository.validate_sql("SELECT 1")
        self.session.connection.assert_not_awaited()


class ValidateSQLNodeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.sql = "SELECT SUM(order_amount) FROM fact_order"
        self.repository = SimpleNamespace(validate_sql=AsyncMock(return_value=self.sql))
        self.events = []
        self.runtime = SimpleNamespace(
            context={"dw_mysql_repository": self.repository}, stream_writer=self.events.append
        )
        self.state = {"sql": self.sql + ";", "error": "old error", "validated_sql": "old SQL"}
        self.state["sql_result"] = {"columns": ["old"], "rows": [{"old": 1}], "row_count": 1, "truncated": False}
        for node in (validation_node, generate_node):
            logger_patch = patch.object(node, "logger")
            logger_patch.start()
            self.addCleanup(logger_patch.stop)

    async def test_success_saves_the_exact_validated_text_and_clears_error(self):
        before = deepcopy(self.state)
        result = await validation_node.validate_sql(self.state, self.runtime)
        self.assertEqual(result, {"sql": self.sql, "validated_sql": self.sql, "error": None, "sql_result": None})
        self.repository.validate_sql.assert_awaited_once_with(self.state["sql"])
        self.assertEqual(self.state, before)
        self.assertEqual([event["status"] for event in self.events], ["running", "success"])

    async def test_sql_error_preserves_candidate_for_correction_and_removes_old_marker(self):
        self.repository.validate_sql.side_effect = SQLValidationError("unknown column amount")
        before = deepcopy(self.state)
        result = await validation_node.validate_sql(self.state, self.runtime)
        self.assertEqual(result, {"validated_sql": None, "error": "unknown column amount", "sql_result": None})
        self.assertEqual(self.state, before)
        self.assertEqual(self.events[-1]["status"], "error")

    async def test_service_failure_propagates_instead_of_requesting_sql_correction(self):
        self.repository.validate_sql.side_effect = RuntimeError("database unavailable")
        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            await validation_node.validate_sql(self.state, self.runtime)
        self.assertEqual(self.events[-1]["status"], "error")

    async def test_deadline_cancels_database_work_and_propagates_timeout(self):
        cancelled = []

        async def slow_validation(_):
            try:
                await asyncio.sleep(1)
            finally:
                cancelled.append(True)

        self.repository.validate_sql.side_effect = slow_validation
        with patch.object(validation_node, "VALIDATION_TIMEOUT_SECONDS", 0.01):
            with self.assertRaises(TimeoutError):
                await validation_node.validate_sql(self.state, self.runtime)
        self.assertEqual(cancelled, [True])
        self.assertEqual(self.events[-1]["status"], "error")

    def build_branch_graph(self, visited):
        builder = StateGraph(validation_node.DataAgentState,
                             context_schema=validation_node.DataAgentContext)
        builder.add_node("validate_sql", validation_node.validate_sql)

        async def execute(state):
            self.assertIsNone(state["error"])
            self.assertEqual(state["sql"], state["validated_sql"])
            visited.append("run_sql")
            return {}

        async def correct(state):
            self.assertIsNotNone(state["error"])
            self.assertIsNone(state["validated_sql"])
            visited.append("correct_sql")
            return {}

        builder.add_node("run_sql", execute)
        builder.add_node("correct_sql", correct)
        builder.add_edge(START, "validate_sql")
        # Isolate validation routing; the correction loop is covered in test_correct_sql.
        route = next(iter(graph_module.graph_builder.branches["validate_sql"].values()))
        builder.add_conditional_edges("validate_sql", route.path, path_map=route.ends)
        builder.add_edge("correct_sql", END)
        for source, target in graph_module.graph_builder.edges:
            if source == "run_sql":
                builder.add_edge(source, target)
        return builder.compile()

    async def test_workflow_success_reaches_execution_with_the_same_sql(self):
        visited = []
        result = await self.build_branch_graph(visited).ainvoke(self.state, context=self.runtime.context)
        self.assertEqual(visited, ["run_sql"])
        self.assertEqual(result["sql"], self.sql)

    async def test_workflow_rejects_sql_without_reaching_execution(self):
        visited = []
        self.repository.validate_sql.side_effect = SQLValidationError("unknown table")
        result = await self.build_branch_graph(visited).ainvoke(self.state, context=self.runtime.context)
        self.assertEqual(visited, ["correct_sql"])
        self.assertEqual(result["sql"], self.state["sql"])
        self.assertEqual(result["error"], "unknown table")

    async def test_workflow_service_failure_reaches_neither_later_node(self):
        visited = []
        self.repository.validate_sql.side_effect = RuntimeError("connection failed")
        with self.assertRaisesRegex(RuntimeError, "connection failed"):
            await self.build_branch_graph(visited).ainvoke(self.state, context=self.runtime.context)
        self.assertEqual(visited, [])

    async def test_generated_sql_passes_through_real_repository_validation(self):
        result = SimpleNamespace(close=Mock())
        connection = SimpleNamespace(exec_driver_sql=AsyncMock(return_value=result))
        session = SimpleNamespace(
            get_bind=lambda: SimpleNamespace(dialect=SimpleNamespace(name="mysql")),
            connection=AsyncMock(return_value=connection),
        )
        builder = StateGraph(validation_node.DataAgentState,
                             context_schema=validation_node.DataAgentContext)
        builder.add_node("generate_sql", generate_node.generate_sql)
        builder.add_node("validate_sql", validation_node.validate_sql)
        builder.add_edge(START, "generate_sql")
        builder.add_edge("generate_sql", "validate_sql")
        builder.add_edge("validate_sql", END)
        state = {
            "query": "统计示例数值的总和", "metric_infos": [],
            "table_infos": [{"name": "demo_numbers", "role": "fact", "description": "示例表",
                             "columns": [{"name": "sample_value", "type": "int", "role": "measure",
                                          "description": "示例数值", "examples": [], "alias": []}]}],
            "date_info": {"date": "2026-11-17", "weekday": "星期二", "quarter": "Q4", "timezone": "Asia/Shanghai"},
            "db_info": {"dialect": "mysql", "version": "8.0.46"},
        }
        with patch.object(generate_node, "llm", RunnableLambda(lambda _:
                          "```sql\nselect sum(sample_value) from demo_numbers;\n```")):
            output = await builder.compile().ainvoke(
                state, context={"dw_mysql_repository": DWMySQLRepository(session)}
            )
        self.assertEqual(output["sql"], "SELECT SUM(sample_value) FROM demo_numbers")
        self.assertEqual(output["validated_sql"], output["sql"])
        self.assertIsNone(output["error"])
        connection.exec_driver_sql.assert_awaited_once_with(
            "EXPLAIN FORMAT=JSON SELECT SUM(sample_value) FROM demo_numbers",
            execution_options={"no_parameters": True},
        )


if __name__ == "__main__":
    unittest.main()
