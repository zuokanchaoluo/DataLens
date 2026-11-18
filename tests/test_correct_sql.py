import os
import unittest
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, START, StateGraph
from sqlalchemy.exc import OperationalError

with patch.dict(os.environ, {"OPENAI_API_KEY": "offline-test-key"}):
    import app.agent.graph as graph_module
    import app.agent.nodes.correct_sql as correction_node
    import app.agent.nodes.generate_sql as generate_node
    import app.agent.nodes.validate_sql as validation_node
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository


def example_state():
    return {
        "query": "统计去年的示例数值总和",
        "table_infos": [{"name": "demo_numbers", "role": "fact", "description": "示例表",
                         "columns": [{"name": "sample_value", "type": "int", "role": "measure",
                                      "description": "示例数值", "examples": [], "alias": []},
                                     {"name": "sample_year", "type": "int", "role": "dimension",
                                      "description": "示例年份", "examples": [], "alias": []}]}],
        "metric_infos": [{"name": "示例合计", "description": "示例数值求和",
                          "relevant_columns": ["demo_numbers.sample_value"], "alias": []}],
        "date_info": {"date": "2026-11-17", "weekday": "星期二", "quarter": "Q4", "timezone": "Asia/Shanghai"},
        "db_info": {"dialect": "mysql", "version": "8.0.46"},
        "sql": "SELECT SUM(missing_value) FROM demo_numbers WHERE sample_year = 2025",
        "validated_sql": None,
        "error": "MySQL SQL 校验失败（1054）: Unknown column 'missing_value'",
        "sql_correction_attempts": 0,
    }


CORRECT_SQL = "SELECT SUM(sample_value) FROM demo_numbers WHERE sample_year = 2025"


class CorrectSQLTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.state = example_state()
        self.events = []
        self.runtime = SimpleNamespace(context={}, stream_writer=self.events.append)
        logger_patch = patch.object(correction_node, "logger")
        logger_patch.start()
        self.addCleanup(logger_patch.stop)

    async def invoke(self, response):
        with patch.object(correction_node, "llm", RunnableLambda(lambda _: response)):
            return await correction_node.correct_sql(self.state, self.runtime)

    async def test_new_candidate_clears_old_validation_and_preserves_input(self):
        self.state["validated_sql"] = "stale SQL"
        self.state["sql_result"] = {"columns": ["old"], "rows": [{"old": 1}], "row_count": 1, "truncated": False}
        before = deepcopy(self.state)
        result = await self.invoke(CORRECT_SQL)
        self.assertEqual(result, {"sql": CORRECT_SQL, "validated_sql": None,
                                  "error": None, "sql_correction_attempts": 1, "sql_result": None})
        self.assertEqual(self.state, before)
        self.assertEqual([event["status"] for event in self.events], ["running", "success"])

    async def test_second_correction_can_still_produce_a_candidate(self):
        self.state["sql_correction_attempts"] = 1
        result = await self.invoke(CORRECT_SQL)
        self.assertEqual(result["sql_correction_attempts"], 2)
        self.assertEqual(result["sql"], CORRECT_SQL)

    async def test_missing_counter_is_treated_as_first_correction(self):
        self.state.pop("sql_correction_attempts")
        result = await self.invoke(CORRECT_SQL)
        self.assertEqual(result["sql_correction_attempts"], 1)

    async def test_prompt_contains_all_seven_inputs_and_keeps_literal_braces(self):
        prompts = []
        self.state["error"] += " {example: value}"

        def respond(prompt):
            prompts.append(prompt.to_string())
            return CORRECT_SQL

        with patch.object(correction_node, "llm", RunnableLambda(respond)):
            await correction_node.correct_sql(self.state, self.runtime)
        text = prompts[0]
        for value in (self.state["query"], self.state["sql"], self.state["error"],
                      "示例表", "demo_numbers.sample_value", "示例数值求和",
                      "2026-11-17", "Asia/Shanghai", "mysql", "8.0.46"):
            self.assertIn(value, text)
        for key in ("query", "table_infos", "metric_infos", "date_info", "db_info", "sql", "error"):
            self.assertNotIn("{" + key + "}", text)

    async def test_ai_message_and_one_markdown_wrapper_are_cleaned(self):
        result = await self.invoke(AIMessage(content=f"```sql\n{CORRECT_SQL};\n```"))
        self.assertEqual(result["sql"], CORRECT_SQL + ";")

    async def test_no_metric_is_valid_for_an_ordinary_query(self):
        self.state["metric_infos"] = []
        result = await self.invoke(CORRECT_SQL)
        self.assertEqual(result["sql"], CORRECT_SQL)

    async def test_absent_error_stops_before_model_call(self):
        for error in (None, "", "   "):
            with self.subTest(error=error), patch.object(correction_node, "llm", None):
                self.state["error"] = error
                with self.assertRaisesRegex(ValueError, "校验错误信息"):
                    await correction_node.correct_sql(self.state, self.runtime)
                self.assertEqual(self.events[-1]["status"], "error")

    async def test_missing_question_sql_or_tables_stops_before_model_call(self):
        for key, value in (("query", " "), ("sql", " "), ("table_infos", []),
                           ("table_infos", [{"name": "demo_numbers", "columns": []}])):
            with self.subTest(key=key, value=value), patch.object(correction_node, "llm", None):
                self.state = example_state()
                self.state[key] = value
                with self.assertRaises(ValueError):
                    await correction_node.correct_sql(self.state, self.runtime)

    async def test_invalid_correction_counter_is_rejected(self):
        for attempts in (-1, "1", True):
            with self.subTest(attempts=attempts), patch.object(correction_node, "llm", None):
                self.state["sql_correction_attempts"] = attempts
                with self.assertRaisesRegex(ValueError, "非负整数"):
                    await correction_node.correct_sql(self.state, self.runtime)

    async def test_exhaustion_keeps_last_error_and_does_not_call_model(self):
        self.state["sql_correction_attempts"] = correction_node.MAX_SQL_CORRECTIONS
        self.state["validated_sql"] = "stale SQL"
        before = deepcopy(self.state)
        with patch.object(correction_node, "llm", None):
            result = await correction_node.correct_sql(self.state, self.runtime)
        self.assertIsNone(result["validated_sql"])
        self.assertIn("已达上限", result["error"])
        self.assertIn(self.state["error"], result["error"])
        self.assertNotIn("sql", result)
        self.assertEqual(self.state, before)
        self.assertEqual(self.events[-1]["status"], "error")

    async def test_empty_or_malformed_model_output_propagates_error(self):
        for output in ("", "  ", "```sql\nSELECT 1", "```python\nSELECT 1\n```"):
            with self.subTest(output=output):
                before = deepcopy(self.state)
                with self.assertRaises(ValueError):
                    await self.invoke(output)
                self.assertEqual(self.state, before)
                self.assertEqual(self.events[-1]["status"], "error")

    async def test_candidate_is_not_marked_valid_even_when_it_looks_like_sql(self):
        # The next validation node, rather than this model node, rejects unsafe output.
        result = await self.invoke("DELETE FROM demo_numbers")
        self.assertIsNone(result["validated_sql"])
        self.assertEqual(result["sql"], "DELETE FROM demo_numbers")

    async def test_model_failure_propagates_without_changing_state(self):
        before = deepcopy(self.state)

        async def fail(_):
            raise RuntimeError("model unavailable")

        with patch.object(correction_node, "llm", RunnableLambda(fail)):
            with self.assertRaisesRegex(RuntimeError, "model unavailable"):
                await correction_node.correct_sql(self.state, self.runtime)
        self.assertEqual(self.state, before)
        self.assertEqual(self.events[-1]["status"], "error")


class CorrectionWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.state = example_state()
        self.prompts = []
        self.commands = []
        self.executed = []
        self.events = []

        async def explain(command, **kwargs):
            self.commands.append(command)
            if "missing_value" in command:
                raise OperationalError(command, None, Exception(1054, "Unknown column in: " + command))
            return SimpleNamespace(close=Mock())

        self.connection = SimpleNamespace(exec_driver_sql=AsyncMock(side_effect=explain))
        self.session = SimpleNamespace(
            get_bind=lambda: SimpleNamespace(dialect=SimpleNamespace(name="mysql")),
            connection=AsyncMock(return_value=self.connection),
        )
        self.repository = DWMySQLRepository(self.session)
        for node in (correction_node, validation_node, generate_node):
            logger_patch = patch.object(node, "logger")
            logger_patch.start()
            self.addCleanup(logger_patch.stop)

    def build_graph(self, include_generation=False):
        builder = StateGraph(correction_node.DataAgentState,
                             context_schema=correction_node.DataAgentContext)
        nodes = {"validate_sql": validation_node.validate_sql,
                 "correct_sql": correction_node.correct_sql}
        if include_generation:
            nodes["generate_sql"] = generate_node.generate_sql

        async def execute(state):
            self.assertIsNone(state["error"])
            self.assertEqual(state["sql"], state["validated_sql"])
            self.executed.append(state["sql"])
            return {}

        nodes["run_sql"] = execute
        for name, function in nodes.items():
            builder.add_node(name, function)
        builder.add_edge(START, "generate_sql" if include_generation else "validate_sql")
        # Use actual production edges/routers, replacing only the execution node and services.
        for source, target in graph_module.graph_builder.edges:
            if source in nodes and (target in nodes or target == END):
                builder.add_edge(source, target)
        for source in ("validate_sql", "correct_sql"):
            for branch in graph_module.graph_builder.branches[source].values():
                builder.add_conditional_edges(source, branch.path, path_map=branch.ends)
        return builder.compile()

    async def invoke(self, responses, include_generation=False):
        response_iterator = iter(responses)

        def respond(prompt):
            self.prompts.append(prompt.to_string())
            return next(response_iterator)

        with patch.object(correction_node, "llm", RunnableLambda(respond)):
            result = None
            async for mode, chunk in self.build_graph(include_generation).astream(
                self.state, context={"dw_mysql_repository": self.repository},
                stream_mode=["values", "custom"],
            ):
                if mode == "values":
                    result = chunk
                else:
                    self.events.append(chunk)
            return result

    async def test_valid_initial_query_skips_model_correction(self):
        self.state["sql"] = CORRECT_SQL
        result = await self.invoke([])
        self.assertEqual(self.prompts, [])
        self.assertEqual(len(self.commands), 1)
        self.assertEqual(self.executed, [CORRECT_SQL])
        self.assertEqual(result["sql_correction_attempts"], 0)

    async def test_one_repair_is_revalidated_before_execution(self):
        before = deepcopy(self.state)
        result = await self.invoke([f"```sql\n{CORRECT_SQL};\n```"])
        self.assertEqual(len(self.prompts), 1)
        self.assertEqual(len(self.commands), 2)
        self.assertEqual(self.commands[-1], "EXPLAIN FORMAT=JSON " + CORRECT_SQL)
        self.assertEqual(self.executed, [CORRECT_SQL])
        self.assertEqual(result["validated_sql"], CORRECT_SQL)
        self.assertEqual(result["sql_correction_attempts"], 1)
        self.assertEqual(result["table_infos"], before["table_infos"])
        self.assertEqual(self.state, before)

    async def test_second_repair_uses_latest_candidate_and_latest_validation_error(self):
        second_sql = self.state["sql"].replace("missing_value", "another_missing_value")
        result = await self.invoke([second_sql, CORRECT_SQL])
        self.assertEqual(len(self.prompts), 2)
        self.assertIn(second_sql, self.prompts[1])
        self.assertIn("Unknown column in: EXPLAIN FORMAT=JSON " + second_sql, self.prompts[1])
        self.assertEqual(len(self.commands), 3)
        self.assertEqual(result["sql_correction_attempts"], 2)
        self.assertIsNone(result["error"])
        self.assertEqual(self.executed, [CORRECT_SQL])

    async def test_repeated_invalid_sql_exhausts_limit_without_execution(self):
        result = await self.invoke([self.state["sql"]] * correction_node.MAX_SQL_CORRECTIONS)
        self.assertEqual(len(self.prompts), correction_node.MAX_SQL_CORRECTIONS)
        self.assertEqual(len(self.commands), correction_node.MAX_SQL_CORRECTIONS + 1)
        self.assertEqual(self.executed, [])
        self.assertEqual(result["sql_correction_attempts"], correction_node.MAX_SQL_CORRECTIONS)
        self.assertIsNone(result["validated_sql"])
        self.assertIn("已达上限", result["error"])
        self.assertIn("1054", result["error"])
        self.assertEqual(self.events[-1]["status"], "error")

    async def test_unsafe_correction_is_rejected_before_database_or_execution(self):
        result = await self.invoke(["DELETE FROM demo_numbers", CORRECT_SQL])
        self.assertIn("只允许 SELECT", self.prompts[1])
        # The unsafe middle candidate never reaches even EXPLAIN.
        self.assertEqual(len(self.commands), 2)
        self.assertTrue(all(command.startswith("EXPLAIN FORMAT=JSON SELECT") for command in self.commands))
        self.assertEqual(self.executed, [CORRECT_SQL])
        self.assertEqual(result["sql_correction_attempts"], 2)

    async def test_database_service_failure_does_not_trigger_model_correction(self):
        self.connection.exec_driver_sql.side_effect = OperationalError(
            "EXPLAIN", None, Exception(2003, "database unavailable")
        )
        with self.assertRaises(OperationalError):
            await self.invoke([])
        self.assertEqual(self.prompts, [])
        self.assertEqual(self.executed, [])

    async def test_new_generation_resets_previous_counter_before_repair(self):
        self.state["sql_correction_attempts"] = correction_node.MAX_SQL_CORRECTIONS
        self.state["validated_sql"] = "stale SQL"
        with patch.object(generate_node, "llm", RunnableLambda(lambda _: self.state["sql"])):
            result = await self.invoke([CORRECT_SQL], include_generation=True)
        self.assertEqual(len(self.prompts), 1)
        self.assertEqual(result["sql_correction_attempts"], 1)
        self.assertEqual(self.executed, [CORRECT_SQL])


if __name__ == "__main__":
    unittest.main()
