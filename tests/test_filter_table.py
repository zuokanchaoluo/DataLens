import os
import unittest
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, START, StateGraph

with patch.dict(os.environ, {"OPENAI_API_KEY": "offline-test-key"}):
    import app.agent.nodes.filter_table as filter_node
    import app.agent.nodes.merge_retrieved_info as merge_node
from app.entities.column_info import ColumnInfo
from app.entities.table_info import TableInfo


class FilterTableTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tables = [
            {"name": "fact_order", "role": "fact", "description": "订单事实表", "columns": [
                self.column("order_amount", "measure"),
                self.column("order_id", "primary_key"),
                self.column("region_id", "foreign_key"),
                self.column("remark", "dimension"),
            ]},
            {"name": "dim_region", "role": "dim", "description": "地区维度表", "columns": [
                self.column("region_name", "dimension", ["华东", "华北"]),
                self.column("region_id", "primary_key"),
                self.column("country", "dimension"),
            ]},
            {"name": "dim_product", "role": "dim", "description": "商品维度表", "columns": [
                self.column("product_name", "dimension"),
                self.column("product_id", "primary_key"),
            ]},
        ]
        self.state = {
            "query": "统计华北地区的销售总额",
            "table_infos": self.tables,
            "metric_infos": [{"name": "GMV", "description": "销售额",
                              "relevant_columns": ["fact_order.order_amount"], "alias": []}],
        }
        self.events = []
        self.runtime = SimpleNamespace(stream_writer=self.events.append)
        for node in (filter_node, merge_node):
            logger_patch = patch.object(node, "logger")
            logger_patch.start()
            self.addCleanup(logger_patch.stop)

    @staticmethod
    def column(name, role, examples=None):
        return {"name": name, "type": "varchar(50)", "role": role,
                "examples": examples or [], "description": f"{name}说明", "alias": [name]}

    async def invoke(self, response, state=None):
        with patch.object(filter_node, "llm", RunnableLambda(lambda _: response)):
            return await filter_node.filter_table(
                self.state if state is None else state, self.runtime
            )

    async def test_single_table_selection_keeps_complete_original_metadata(self):
        result = await self.invoke('{"fact_order": ["order_amount"]}')
        self.assertEqual(result["table_infos"], [{
            **self.tables[0], "columns": [self.tables[0]["columns"][0]]
        }])
        self.assertEqual([event["status"] for event in self.events], ["running", "success"])

    async def test_multiple_tables_restore_only_candidate_key_columns(self):
        result = await self.invoke(
            '{"fact_order": ["order_amount"], "dim_region": ["region_name"]}'
        )
        tables = {table["name"]: table for table in result["table_infos"]}
        self.assertEqual(set(tables), {"fact_order", "dim_region"})
        self.assertEqual({c["name"] for c in tables["fact_order"]["columns"]},
                         {"order_amount", "order_id", "region_id"})
        self.assertEqual({c["name"] for c in tables["dim_region"]["columns"]},
                         {"region_name", "region_id"})

    async def test_keeps_input_order_and_deduplicates_selected_names(self):
        result = await self.invoke(
            '{"dim_region": ["region_id", "region_name"], '
            '"fact_order": ["region_id", "order_amount", "order_amount"]}'
        )
        self.assertEqual([table["name"] for table in result["table_infos"]],
                         ["fact_order", "dim_region"])
        self.assertEqual([c["name"] for c in result["table_infos"][0]["columns"]],
                         ["order_amount", "order_id", "region_id"])

    async def test_empty_candidates_skip_the_model(self):
        with patch.object(filter_node, "llm", None):
            result = await filter_node.filter_table(
                {"query": "销售额", "table_infos": []}, self.runtime
            )
        self.assertEqual(result, {"table_infos": []})
        self.assertEqual(self.events[-1]["status"], "success")

    async def test_empty_model_selection_returns_no_tables(self):
        self.assertEqual(await self.invoke('{}'), {"table_infos": []})

    async def test_unknown_tables_and_fields_are_rejected(self):
        for response in ('{"unknown_table": ["id"]}',
                         '{"fact_order": ["invented_column"]}'):
            with self.subTest(response=response):
                with self.assertRaisesRegex(ValueError, "候选"):
                    await self.invoke(response)
                self.assertEqual(self.events[-1]["status"], "error")

    async def test_wrong_output_shapes_are_rejected(self):
        for response in ('[]', 'null', '{"fact_order": []}',
                         '{"fact_order": "order_amount"}', '{"fact_order": [7]}'):
            with self.subTest(response=response):
                with self.assertRaises(ValueError):
                    await self.invoke(response)
                self.assertEqual(self.events[-1]["status"], "error")

    async def test_malformed_json_is_reported(self):
        with self.assertRaises(ValueError):
            await self.invoke('not JSON')
        self.assertEqual(self.events[-1]["status"], "error")

    async def test_model_failure_is_propagated(self):
        async def fail(_):
            raise RuntimeError("model unavailable")

        with patch.object(filter_node, "llm", RunnableLambda(fail)):
            with self.assertRaisesRegex(RuntimeError, "model unavailable"):
                await filter_node.filter_table(self.state, self.runtime)
        self.assertEqual(self.events[-1]["status"], "error")

    async def test_selection_does_not_mutate_or_share_nested_input_data(self):
        self.tables[0]["columns"][0]["examples"] = [{"sample": [12.5]}]
        before = deepcopy(self.state)
        result = await self.invoke('{"fact_order": ["order_amount"]}')
        self.assertEqual(self.state, before)
        output_column = result["table_infos"][0]["columns"][0]
        output_column["examples"][0]["sample"].append(99)
        output_column["alias"].append("新别名")
        self.assertEqual(self.state, before)

    async def test_real_prompt_formats_json_example_and_chinese_context(self):
        formatted_prompts = []

        def respond(prompt):
            formatted_prompts.append(prompt.to_string())
            return '{"fact_order": ["order_amount"]}'

        with patch.object(filter_node, "llm", RunnableLambda(respond)):
            await filter_node.filter_table(self.state, self.runtime)
        text = formatted_prompts[0]
        self.assertIn('"表名1":["字段1"', text)
        self.assertIn("统计华北地区的销售总额", text)
        self.assertIn("地区维度表", text)
        self.assertIn("华北", text)
        self.assertNotIn("{table_infos}", text)

    async def test_graph_updates_table_state_and_preserves_metrics(self):
        builder = StateGraph(filter_node.DataAgentState)
        builder.add_node("filter_table", filter_node.filter_table)
        builder.add_edge(START, "filter_table")
        builder.add_edge("filter_table", END)
        with patch.object(filter_node, "llm", RunnableLambda(
            lambda _: '{"fact_order": ["order_amount"]}'
        )):
            result = await builder.compile().ainvoke(self.state)
        self.assertEqual(len(result["table_infos"]), 1)
        self.assertEqual(result["metric_infos"], self.state["metric_infos"])
        self.assertEqual(len(self.state["table_infos"]), 3)

    async def test_merge_output_can_be_filtered_without_losing_join_candidates(self):
        columns = [ColumnInfo(
            id=f"{table['name']}_{column['name']}", table_id=table["name"], **column
        ) for table in self.tables for column in table["columns"]]
        tables = {table["name"]: TableInfo(table["name"], table["name"],
                                          table["role"], table["description"])
                  for table in self.tables}
        repository = SimpleNamespace(
            get_table_info_by_id=AsyncMock(side_effect=lambda name: tables[name]),
            get_key_columns_by_table_id=AsyncMock(side_effect=lambda name: [
                column for column in columns if column.table_id == name
                and column.role in ("primary_key", "foreign_key")
            ]),
        )
        builder = StateGraph(filter_node.DataAgentState,
                             context_schema=filter_node.DataAgentContext)
        builder.add_node("merge", merge_node.merge_retrieved_info)
        builder.add_node("filter", filter_node.filter_table)
        builder.add_edge(START, "merge")
        builder.add_edge("merge", "filter")
        builder.add_edge("filter", END)
        with patch.object(filter_node, "llm", RunnableLambda(lambda _:
            '{"fact_order": ["order_amount"], "dim_region": ["region_name"]}'
        )):
            result = await builder.compile().ainvoke({
                "query": self.state["query"], "retrieved_column_infos": columns,
                "retrieved_metric_infos": [], "retrieved_value_infos": [],
            }, context={"meta_mysql_repository": repository})
        self.assertEqual([table["name"] for table in result["table_infos"]],
                         ["fact_order", "dim_region"])
        self.assertTrue(all("region_id" in [c["name"] for c in table["columns"]]
                            for table in result["table_infos"]))


if __name__ == "__main__":
    unittest.main()
