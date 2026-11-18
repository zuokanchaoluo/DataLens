import os
import unittest
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, START, StateGraph

with patch.dict(os.environ, {"OPENAI_API_KEY": "offline-test-key"}):
    import app.agent.nodes.generate_sql as sql_node
    import app.agent.nodes.add_extra_context as context_node
    import app.agent.nodes.filter_metric as metric_node
    import app.agent.nodes.filter_table as table_node


class GenerateSQLTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.sql = (
            "SELECT SUM(o.order_amount) AS total_amount\n"
            "FROM fact_order AS o\n"
            "JOIN dim_date AS d ON o.date_id = d.date_id\n"
            "WHERE d.year = 2025;"
        )
        self.state = {
            "query": "统计去年的销售总额",
            "table_infos": [
                {"name": "fact_order", "role": "fact", "description": "订单事实表", "columns": [
                    self.column("order_amount", "measure", "float"),
                    self.column("date_id", "foreign_key", "varchar(20)"),
                ]},
                {"name": "dim_date", "role": "dim", "description": "日期维度表", "columns": [
                    self.column("date_id", "primary_key", "varchar(20)"),
                    self.column("year", "dimension", "int"),
                ]},
            ],
            "metric_infos": [{"name": "GMV", "description": "订单成交金额总和",
                              "relevant_columns": ["fact_order.order_amount"], "alias": ["销售额"]}],
            "date_info": {"date": "2026-11-17", "weekday": "星期二", "quarter": "Q4",
                          "timezone": "Asia/Shanghai"},
            "db_info": {"dialect": "mysql", "version": "8.0.46"},
        }
        self.events = []
        self.runtime = SimpleNamespace(stream_writer=self.events.append)
        for node in (sql_node, context_node, metric_node, table_node):
            logger_patch = patch.object(node, "logger")
            logger_patch.start()
            self.addCleanup(logger_patch.stop)

    @staticmethod
    def column(name, role, type):
        return {"name": name, "role": role, "type": type,
                "description": f"{name}说明", "examples": [], "alias": []}

    async def invoke(self, response):
        with patch.object(sql_node, "llm", RunnableLambda(lambda _: response)):
            return await sql_node.generate_sql(self.state, self.runtime)

    async def test_generates_text_clears_old_error_and_keeps_input_unchanged(self):
        self.state["error"] = "previous error"
        self.state["validated_sql"] = "SELECT 1"
        self.state["sql_correction_attempts"] = 2
        self.state["sql_result"] = {"columns": ["old"], "rows": [{"old": 1}], "row_count": 1, "truncated": False}
        before = deepcopy(self.state)
        result = await self.invoke(" \n" + self.sql + "\n ")
        self.assertEqual(result, {"sql": self.sql, "validated_sql": None, "error": None,
                                  "sql_correction_attempts": 0, "sql_result": None})
        self.assertEqual(self.state, before)
        self.assertEqual([event["status"] for event in self.events], ["running", "success"])

    async def test_ai_message_text_blocks_are_parsed(self):
        result = await self.invoke(AIMessage(content=[{"type": "text", "text": self.sql}]))
        self.assertEqual(result["sql"], self.sql)

    async def test_removes_one_complete_markdown_wrapper(self):
        for header in ("```sql", "```SQL", "```"):
            with self.subTest(header=header):
                result = await self.invoke(f"{header}\n{self.sql}\n```")
                self.assertEqual(result["sql"], self.sql)

    async def test_preserves_sql_body_line_endings_and_literal_content(self):
        sql = "SELECT 'first\r\nsecond;```' AS marker;"
        result = await self.invoke(f"```sql\r\n{sql}\r\n```")
        self.assertEqual(result["sql"], sql)

    async def test_empty_model_output_is_rejected(self):
        for response in ("", " \n ", "```sql\n\n```"):
            with self.subTest(response=response):
                with self.assertRaisesRegex(ValueError, "SQL 文本"):
                    await self.invoke(response)
                self.assertEqual(self.events[-1]["status"], "error")

    async def test_incomplete_multiple_or_wrong_language_code_blocks_are_rejected(self):
        for response in ("```sql\nSELECT 1", "```python\nSELECT 1\n```",
                         "```sql\nSELECT 1\n```\n```sql\nSELECT 2\n```"):
            with self.subTest(response=response):
                with self.assertRaisesRegex(ValueError, "代码块"):
                    await self.invoke(response)
                self.assertEqual(self.events[-1]["status"], "error")

    async def test_no_tables_or_empty_column_lists_fail_before_model_call(self):
        for tables in ([], [{"name": "fact_order", "columns": []}]):
            with self.subTest(tables=tables), patch.object(sql_node, "llm", None):
                self.state["table_infos"] = tables
                with self.assertRaisesRegex(ValueError, "候选表字段"):
                    await sql_node.generate_sql(self.state, self.runtime)

    async def test_empty_question_fails_before_model_call(self):
        self.state["query"] = "   "
        with patch.object(sql_node, "llm", None):
            with self.assertRaisesRegex(ValueError, "用户问题不能为空"):
                await sql_node.generate_sql(self.state, self.runtime)

    async def test_missing_metric_dependencies_fail_before_model_call(self):
        for reference in ("fact_order.missing_column", "missing_table.order_amount"):
            with self.subTest(reference=reference), patch.object(sql_node, "llm", None):
                self.state["metric_infos"][0]["relevant_columns"] = [reference]
                with self.assertRaisesRegex(ValueError, "指标 GMV 依赖的字段"):
                    await sql_node.generate_sql(self.state, self.runtime)
                self.assertEqual(self.events[-1]["status"], "error")

    async def test_no_metric_definition_is_valid_for_an_ordinary_query(self):
        self.state["metric_infos"] = []
        result = await self.invoke("SELECT year FROM dim_date;")
        self.assertEqual(result["sql"], "SELECT year FROM dim_date;")

    async def test_prompt_contains_all_five_inputs(self):
        prompts = []

        def respond(prompt):
            prompts.append(prompt.to_string())
            return self.sql

        with patch.object(sql_node, "llm", RunnableLambda(respond)):
            await sql_node.generate_sql(self.state, self.runtime)
        text = prompts[0]
        for value in (self.state["query"], "订单事实表", "订单成交金额总和",
                      "fact_order.order_amount", "2026-11-17", "Asia/Shanghai", "mysql", "8.0.46"):
            self.assertIn(value, text)
        for variable in ("query", "table_infos", "metric_infos", "date_info", "db_info"):
            self.assertNotIn("{" + variable + "}", text)

    async def test_model_failure_is_reported_and_propagated(self):
        async def fail(_):
            raise RuntimeError("model unavailable")

        with patch.object(sql_node, "llm", RunnableLambda(fail)):
            with self.assertRaisesRegex(RuntimeError, "model unavailable"):
                await sql_node.generate_sql(self.state, self.runtime)
        self.assertEqual(self.events[-1]["status"], "error")

    async def test_graph_saves_sql_without_touching_database_tools(self):
        builder = StateGraph(sql_node.DataAgentState, context_schema=sql_node.DataAgentContext)
        builder.add_node("generate_sql", sql_node.generate_sql)
        builder.add_edge(START, "generate_sql")
        builder.add_edge("generate_sql", END)
        with patch.object(sql_node, "llm", RunnableLambda(lambda _: self.sql)):
            result = await builder.compile().ainvoke(self.state, context={})
        self.assertEqual(result["sql"], self.sql)
        self.assertIsNone(result["error"])
        self.assertEqual(result["table_infos"], self.state["table_infos"])

    async def test_filter_and_date_context_outputs_feed_sql_generation(self):
        builder = StateGraph(sql_node.DataAgentState, context_schema=sql_node.DataAgentContext)
        builder.add_node("filter_table", table_node.filter_table)
        builder.add_node("filter_metric", metric_node.filter_metric)
        builder.add_node("add_extra_context", context_node.add_extra_context)
        builder.add_node("generate_sql", sql_node.generate_sql)
        builder.add_edge(START, "filter_table")
        builder.add_edge(START, "filter_metric")
        builder.add_edge(["filter_table", "filter_metric"], "add_extra_context")
        builder.add_edge("add_extra_context", "generate_sql")
        builder.add_edge("generate_sql", END)
        input_state = {key: self.state[key] for key in ("query", "table_infos", "metric_infos")}
        repository = SimpleNamespace(get_db_info=AsyncMock(return_value=self.state["db_info"]))
        with patch.object(table_node, "llm", RunnableLambda(lambda _:
                 '{"fact_order": ["order_amount", "date_id"], "dim_date": ["date_id", "year"]}'
             )), patch.object(metric_node, "llm", RunnableLambda(lambda _: '["GMV"]')), \
             patch.object(sql_node, "llm", RunnableLambda(lambda _: self.sql)), \
             patch.object(context_node, "datetime") as clock:
            clock.now.return_value = datetime(2026, 11, 17, tzinfo=context_node.BEIJING_TIMEZONE)
            result = await builder.compile().ainvoke(
                input_state, context={"dw_mysql_repository": repository}
            )
        self.assertEqual(result["sql"], self.sql)
        self.assertEqual(result["metric_infos"][0]["name"], "GMV")
        self.assertEqual(result["date_info"]["date"], "2026-11-17")
        repository.get_db_info.assert_awaited_once_with()


if __name__ == "__main__":
    unittest.main()
