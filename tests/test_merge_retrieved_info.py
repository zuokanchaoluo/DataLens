import os
import unittest
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, call, patch

from langgraph.graph import END, START, StateGraph

# The shared configuration may be imported before the model-node tests.
# Resolve it with a dummy key so test discovery never needs real credentials.
with patch.dict(os.environ, {"OPENAI_API_KEY": "offline-test-key"}):
    import app.agent.nodes.merge_retrieved_info as merge_node
from app.entities.column_info import ColumnInfo
from app.entities.metric_info import MetricInfo
from app.entities.table_info import TableInfo
from app.entities.value_info import ValueInfo


class MergeRetrievedInfoTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.amount = self.column("fact_order", "order_amount", "measure", [12.5])
        self.region_name = self.column("dim_region", "region_name", "dimension", ["华东"])
        self.order_id = self.column("fact_order", "order_id", "primary_key", [])
        self.order_region = self.column("fact_order", "region_id", "foreign_key", [])
        self.region_id = self.column("dim_region", "region_id", "primary_key", [])
        self.metric = MetricInfo(
            "GMV", "GMV", "订单成交金额总和", ["fact_order.order_amount"], ["销售额"]
        )
        self.value = ValueInfo("dim_region.region_name.华北", "华北", "dim_region.region_name")
        self.lookup = {
            "fact_order.order_amount": self.amount,
            "dim_region.region_name": self.region_name,
        }
        self.tables = {
            "fact_order": TableInfo("fact_order", "fact_order", "fact", "订单事实表"),
            "dim_region": TableInfo("dim_region", "dim_region", "dim", "地区维度表"),
        }
        self.keys = {
            "fact_order": [self.order_id, self.order_region],
            "dim_region": [self.region_id],
        }
        self.repository = SimpleNamespace(
            get_column_info_by_id=AsyncMock(side_effect=lambda key: self.lookup.get(key)),
            get_table_info_by_id=AsyncMock(side_effect=lambda key: self.tables.get(key)),
            get_key_columns_by_table_id=AsyncMock(side_effect=lambda key: self.keys.get(key, [])),
        )
        self.events = []
        self.runtime = SimpleNamespace(
            context={"meta_mysql_repository": self.repository},
            stream_writer=self.events.append,
        )
        logger_patch = patch.object(merge_node, "logger")
        logger_patch.start()
        self.addCleanup(logger_patch.stop)

    @staticmethod
    def column(table, name, role, examples):
        return ColumnInfo(f"{table}_{name}", name, "varchar(50)", role,
                          examples, f"{name}说明", [name], table)

    def state(self, columns=None, metrics=None, values=None):
        return {
            "query": "统计华北地区的销售总额",
            "retrieved_column_infos": columns or [],
            "retrieved_metric_infos": metrics or [],
            "retrieved_value_infos": values or [],
        }

    async def test_merges_three_sources_and_adds_table_descriptions_and_keys(self):
        state = self.state([self.amount], [self.metric, self.metric], [self.value, self.value])
        before = deepcopy(state)
        result = await merge_node.merge_retrieved_info(state, self.runtime)
        tables = {table["name"]: table for table in result["table_infos"]}
        self.assertEqual(set(tables), {"fact_order", "dim_region"})
        self.assertEqual(tables["fact_order"]["description"], "订单事实表")
        self.assertEqual({c["name"] for c in tables["fact_order"]["columns"]},
                         {"order_amount", "order_id", "region_id"})
        region_columns = {c["name"]: c for c in tables["dim_region"]["columns"]}
        self.assertEqual(region_columns["region_name"]["examples"], ["华东", "华北"])
        self.assertIn("region_id", region_columns)
        self.assertEqual(result["metric_infos"][0]["relevant_columns"], ["fact_order.order_amount"])
        self.assertEqual(len(result["metric_infos"]), 1)
        self.repository.get_column_info_by_id.assert_awaited_once_with("dim_region.region_name")
        self.assertEqual(state, before)
        self.assertEqual(self.region_name.examples, ["华东"])
        self.assertEqual([event["status"] for event in self.events], ["running", "success"])

    async def test_metric_reference_adds_a_column_missing_from_field_recall(self):
        result = await merge_node.merge_retrieved_info(self.state(metrics=[self.metric]), self.runtime)
        self.repository.get_column_info_by_id.assert_awaited_once_with("fact_order.order_amount")
        self.assertIn("order_amount", [c["name"] for c in result["table_infos"][0]["columns"]])

    async def test_legacy_and_dotted_ids_do_not_duplicate_fields_or_references(self):
        canonical = deepcopy(self.amount)
        canonical.id = "fact_order.order_amount"
        metric = deepcopy(self.metric)
        metric.relevant_columns = [self.amount.id, canonical.id, canonical.id]
        result = await merge_node.merge_retrieved_info(
            self.state([self.amount, canonical, self.order_id], [metric]), self.runtime
        )
        names = [column["name"] for column in result["table_infos"][0]["columns"]]
        self.assertEqual(names.count("order_amount"), 1)
        self.assertEqual(names.count("order_id"), 1)
        self.assertEqual(result["metric_infos"][0]["relevant_columns"], [canonical.id])
        self.repository.get_column_info_by_id.assert_not_awaited()

    async def test_nested_examples_and_aliases_are_not_shared_with_inputs(self):
        self.amount.examples = [{"sample": [1]}]
        state = self.state([self.amount])
        result = await merge_node.merge_retrieved_info(state, self.runtime)
        output = result["table_infos"][0]["columns"][0]
        output["examples"][0]["sample"].append(2)
        output["alias"].append("new alias")
        self.assertEqual(self.amount.examples, [{"sample": [1]}])
        self.assertEqual(self.amount.alias, ["order_amount"])

    async def test_value_can_populate_an_empty_examples_list(self):
        self.region_name.examples = None
        result = await merge_node.merge_retrieved_info(self.state(values=[self.value]), self.runtime)
        self.assertEqual(result["table_infos"][0]["columns"][0]["examples"], ["华北"])
        self.assertIsNone(self.region_name.examples)

    async def test_empty_recall_results_need_no_database_queries(self):
        result = await merge_node.merge_retrieved_info(self.state(), self.runtime)
        self.assertEqual(result, {"table_infos": [], "metric_infos": []})
        self.repository.get_column_info_by_id.assert_not_awaited()
        self.repository.get_table_info_by_id.assert_not_awaited()
        self.repository.get_key_columns_by_table_id.assert_not_awaited()

    async def test_missing_column_or_table_fails_with_metadata_error(self):
        for kind in ("column", "table"):
            with self.subTest(kind=kind):
                if kind == "column":
                    self.lookup.clear()
                    state = self.state(metrics=[self.metric])
                else:
                    self.tables.clear()
                    state = self.state(columns=[self.amount])
                with self.assertRaisesRegex(ValueError, "元数据库中找不到"):
                    await merge_node.merge_retrieved_info(state, self.runtime)
                self.assertEqual(self.events[-1]["status"], "error")

    async def test_database_failure_is_not_reported_as_an_empty_success(self):
        self.repository.get_table_info_by_id.side_effect = RuntimeError("database unavailable")
        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            await merge_node.merge_retrieved_info(self.state([self.amount]), self.runtime)
        self.assertEqual(self.events[-1]["status"], "error")

    async def test_key_column_from_wrong_table_is_rejected(self):
        self.keys["fact_order"] = [self.region_id]
        with self.assertRaisesRegex(ValueError, "所属表"):
            await merge_node.merge_retrieved_info(self.state([self.amount]), self.runtime)

    async def test_graph_waits_for_all_three_branches_even_with_an_extra_step(self):
        builder = StateGraph(merge_node.DataAgentState, context_schema=merge_node.DataAgentContext)
        builder.add_node("recall_column", lambda state: {"retrieved_column_infos": [self.amount]})
        builder.add_node("prepare_metric", lambda state: {})
        builder.add_node("recall_metric", lambda state: {"retrieved_metric_infos": [self.metric]})
        builder.add_node("recall_value", lambda state: {"retrieved_value_infos": [self.value]})
        builder.add_node("merge_retrieved_info", merge_node.merge_retrieved_info)
        for name in ("recall_column", "prepare_metric", "recall_value"):
            builder.add_edge(START, name)
        builder.add_edge("prepare_metric", "recall_metric")
        builder.add_edge(["recall_column", "recall_metric", "recall_value"], "merge_retrieved_info")
        builder.add_edge("merge_retrieved_info", END)
        result = await builder.compile().ainvoke(
            {"query": "统计华北地区的销售总额"},
            context={**self.runtime.context, "column_qdrant_repository": None,
                     "metric_qdrant_repository": None, "value_es_repository": None,
                     "embedding_client": None},
        )
        self.assertEqual(len(result["table_infos"]), 2)
        self.assertEqual(result["metric_infos"][0]["name"], "GMV")
        self.assertEqual(self.repository.get_table_info_by_id.await_args_list,
                         [call("fact_order"), call("dim_region")])


if __name__ == "__main__":
    unittest.main()
