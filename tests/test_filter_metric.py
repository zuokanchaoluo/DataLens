import os
import unittest
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import patch

from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, START, StateGraph

with patch.dict(os.environ, {"OPENAI_API_KEY": "offline-test-key"}):
    import app.agent.nodes.filter_metric as metric_node
    import app.agent.nodes.filter_table as table_node


class FilterMetricTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.metrics = [
            {"name": "GMV", "description": "订单成交金额总和",
             "relevant_columns": ["fact_order.order_amount"], "alias": ["销售额"]},
            {"name": "AOV", "description": "平均订单金额",
             "relevant_columns": ["fact_order.order_amount"], "alias": ["客单价"]},
        ]
        self.tables = [
            {"name": "fact_order", "role": "fact", "description": "订单事实表", "columns": [
                self.column("order_amount", "measure"),
                self.column("order_id", "primary_key"),
                self.column("region_id", "foreign_key"),
                self.column("remark", "dimension"),
            ]},
            {"name": "dim_region", "role": "dim", "description": "地区维度表", "columns": [
                self.column("region_name", "dimension"),
                self.column("region_id", "primary_key"),
            ]},
        ]
        self.state = {"query": "统计华北地区的销售总额",
                      "metric_infos": self.metrics, "table_infos": self.tables}
        self.events = []
        self.runtime = SimpleNamespace(stream_writer=self.events.append)
        for node in (metric_node, table_node):
            logger_patch = patch.object(node, "logger")
            logger_patch.start()
            self.addCleanup(logger_patch.stop)

    @staticmethod
    def column(name, role):
        return {"name": name, "type": "varchar(50)", "role": role,
                "examples": [], "description": name, "alias": []}

    async def invoke(self, response):
        with patch.object(metric_node, "llm", RunnableLambda(lambda _: response)):
            return await metric_node.filter_metric(self.state, self.runtime)

    async def test_selection_preserves_original_metric_definition(self):
        result = await self.invoke('["GMV"]')
        self.assertEqual(result, {"metric_infos": [self.metrics[0]]})
        self.assertEqual([event["status"] for event in self.events], ["running", "success"])

    async def test_preserves_candidate_order_and_deduplicates_selected_names(self):
        result = await self.invoke('["AOV", "GMV", "GMV"]')
        self.assertEqual([metric["name"] for metric in result["metric_infos"]], ["GMV", "AOV"])

    async def test_empty_candidates_skip_the_model(self):
        with patch.object(metric_node, "llm", None):
            result = await metric_node.filter_metric(
                {"query": "查看订单明细", "metric_infos": []}, self.runtime
            )
        self.assertEqual(result, {"metric_infos": []})
        self.assertEqual(self.events[-1]["status"], "success")

    async def test_empty_selection_is_valid(self):
        self.assertEqual(await self.invoke('[]'), {"metric_infos": []})
        self.assertEqual(self.events[-1]["status"], "success")

    async def test_unknown_names_and_aliases_are_rejected(self):
        for response in ('["unknown_metric"]', '["销售额"]', '[" GMV "]'):
            with self.subTest(response=response):
                with self.assertRaisesRegex(ValueError, "候选集合之外"):
                    await self.invoke(response)
                self.assertEqual(self.events[-1]["status"], "error")

    async def test_wrong_output_shapes_and_empty_names_are_rejected(self):
        for response in ('{"names": ["GMV"]}', 'null', '["GMV", 7]', '[""]', '["   "]'):
            with self.subTest(response=response):
                with self.assertRaisesRegex(ValueError, "JSON 数组"):
                    await self.invoke(response)
                self.assertEqual(self.events[-1]["status"], "error")

    async def test_malformed_json_is_reported(self):
        with self.assertRaises(ValueError):
            await self.invoke('not JSON')
        self.assertEqual(self.events[-1]["status"], "error")

    async def test_model_failure_is_propagated(self):
        async def fail(_):
            raise RuntimeError("model unavailable")

        with patch.object(metric_node, "llm", RunnableLambda(fail)):
            with self.assertRaisesRegex(RuntimeError, "model unavailable"):
                await metric_node.filter_metric(self.state, self.runtime)
        self.assertEqual(self.events[-1]["status"], "error")

    async def test_nested_output_does_not_share_input_lists(self):
        before = deepcopy(self.state)
        result = await self.invoke('["GMV"]')
        self.assertEqual(self.state, before)
        result["metric_infos"][0]["alias"].append("新别名")
        result["metric_infos"][0]["relevant_columns"].append("fact_order.other")
        self.assertEqual(self.state, before)

    async def test_prompt_includes_question_definition_and_field_references(self):
        prompts = []

        def respond(prompt):
            prompts.append(prompt.to_string())
            return '["GMV"]'

        with patch.object(metric_node, "llm", RunnableLambda(respond)):
            await metric_node.filter_metric(self.state, self.runtime)
        self.assertIn(self.state["query"], prompts[0])
        self.assertIn("订单成交金额总和", prompts[0])
        self.assertIn("fact_order.order_amount", prompts[0])
        self.assertNotIn("{metric_infos}", prompts[0])

    async def test_duplicate_candidate_names_are_rejected_before_model_call(self):
        self.state["metric_infos"] = [self.metrics[0], deepcopy(self.metrics[0])]
        with patch.object(metric_node, "llm", None):
            with self.assertRaisesRegex(ValueError, "候选指标名称重复"):
                await metric_node.filter_metric(self.state, self.runtime)
        self.assertEqual(self.events[-1]["status"], "error")

    async def test_graph_updates_metrics_and_preserves_tables(self):
        builder = StateGraph(metric_node.DataAgentState)
        builder.add_node("filter_metric", metric_node.filter_metric)
        builder.add_edge(START, "filter_metric")
        builder.add_edge("filter_metric", END)
        with patch.object(metric_node, "llm", RunnableLambda(lambda _: '["GMV"]')):
            result = await builder.compile().ainvoke(self.state)
        self.assertEqual(result["metric_infos"], [self.metrics[0]])
        self.assertEqual(result["table_infos"], self.tables)
        self.assertEqual(len(self.state["metric_infos"]), 2)

    async def test_parallel_filters_wait_for_both_results(self):
        observations = []

        def after_filters(state):
            observations.append(deepcopy(state))
            return {}

        builder = StateGraph(metric_node.DataAgentState)
        builder.add_node("filter_table", table_node.filter_table)
        builder.add_node("prepare_metric", lambda state: {})
        builder.add_node("filter_metric", metric_node.filter_metric)
        builder.add_node("after_filters", after_filters)
        builder.add_edge(START, "filter_table")
        builder.add_edge(START, "prepare_metric")
        builder.add_edge("prepare_metric", "filter_metric")
        builder.add_edge(["filter_table", "filter_metric"], "after_filters")
        builder.add_edge("after_filters", END)
        before = deepcopy(self.state)
        with patch.object(metric_node, "llm", RunnableLambda(lambda _: '["GMV"]')), \
             patch.object(table_node, "llm", RunnableLambda(lambda _:
                 '{"fact_order": ["order_amount"], "dim_region": ["region_name"]}'
             )):
            result = await builder.compile().ainvoke(self.state)
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0]["metric_infos"], [self.metrics[0]])
        self.assertNotIn("remark", [c["name"] for c in observations[0]["table_infos"][0]["columns"]])
        self.assertEqual(result["metric_infos"], [self.metrics[0]])
        self.assertEqual(self.state, before)


if __name__ == "__main__":
    unittest.main()
