import os
import unittest
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, call, patch

from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, START, StateGraph

# Construct clients with a dummy credential; every external request is mocked.
with patch.dict(os.environ, {"OPENAI_API_KEY": "offline-test-key"}):
    import app.agent.nodes.recall_metric as metric_node
    import app.agent.nodes.recall_value as value_node
    from app.entities.metric_info import MetricInfo
    from app.entities.value_info import ValueInfo
    from app.repositories.es.value_es_repository import ValueEsRepository


class ValueRecallTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.value = ValueInfo(
            "dim_region.region_name.华北", "华北", "dim_region.region_name"
        )
        self.repository = SimpleNamespace(search=AsyncMock(return_value=[]))
        self.events = []
        self.runtime = SimpleNamespace(
            stream_writer=self.events.append,
            context={"value_es_repository": self.repository},
        )
        for node in (value_node, metric_node):
            logger_patch = patch.object(node, "logger")
            logger_patch.start()
            self.addCleanup(logger_patch.stop)

    async def invoke(self, response, keywords=None):
        state = {"query": "统计华北地区的销售总额", "keywords": keywords or []}
        with patch.object(value_node, "llm", RunnableLambda(lambda _: response)):
            return await value_node.recall_value(state, self.runtime)

    async def test_deduplicates_hits_but_keeps_values_from_different_columns(self):
        other = ValueInfo("customers.area.华北", "华北", "customers.area")
        self.repository.search.return_value = [self.value, self.value, other]
        result = await self.invoke('[" 华北 ", "华北地区", "", "华北"]', ["华北地区"])
        self.assertEqual(result, {"retrieved_value_infos": [self.value, other]})
        self.assertEqual(self.repository.search.await_args_list,
                         [call("华北地区"), call("华北")])
        self.assertEqual([event["status"] for event in self.events],
                         ["running", "success"])

    async def test_no_hits_returns_empty_list_without_inventing_a_value(self):
        result = await self.invoke('["不存在的地区"]')
        self.assertEqual(result, {"retrieved_value_infos": []})
        self.assertEqual(self.events[-1]["status"], "success")

    async def test_empty_keywords_skip_es(self):
        self.assertEqual(await self.invoke('[]'), {"retrieved_value_infos": []})
        self.repository.search.assert_not_awaited()

    async def test_rejects_invalid_model_output_before_search(self):
        for response in ('{"values": ["华北"]}', '["华北", 7]', 'not JSON'):
            with self.subTest(response=response):
                with self.assertRaises(ValueError):
                    await self.invoke(response)
                self.assertEqual(self.events[-1]["status"], "error")
        self.repository.search.assert_not_awaited()

    async def test_es_failure_is_reported_and_raised(self):
        self.repository.search.side_effect = ConnectionError("ES unavailable")
        with self.assertRaisesRegex(ConnectionError, "ES unavailable"):
            await self.invoke('["华北"]')
        self.assertEqual(self.events[-1]["status"], "error")

    async def test_result_is_saved_in_graph_state(self):
        builder = StateGraph(
            value_node.DataAgentState, context_schema=value_node.DataAgentContext
        )
        builder.add_node("recall_value", value_node.recall_value)
        builder.add_edge(START, "recall_value")
        builder.add_edge("recall_value", END)
        self.repository.search.return_value = [self.value]
        with patch.object(value_node, "llm", RunnableLambda(lambda _: '["华北"]')):
            result = await builder.compile().ainvoke(
                {"query": "华北销售额", "keywords": ["华北"]},
                context={**self.runtime.context, "column_qdrant_repository": None,
                         "metric_qdrant_repository": None, "embedding_client": None},
            )
        self.assertEqual(result["retrieved_value_infos"], [self.value])

    async def test_parallel_metric_and_value_recall_keep_both_results(self):
        metric = MetricInfo("GMV", "GMV", "销售额", ["fact_order.order_amount"], [])
        builder = StateGraph(
            value_node.DataAgentState, context_schema=value_node.DataAgentContext
        )
        builder.add_node("recall_metric", metric_node.recall_metric)
        builder.add_node("recall_value", value_node.recall_value)
        builder.add_edge(START, "recall_metric")
        builder.add_edge(START, "recall_value")
        builder.add_edge("recall_metric", END)
        builder.add_edge("recall_value", END)
        self.repository.search.return_value = [self.value]
        context = {
            **self.runtime.context,
            "column_qdrant_repository": None,
            "embedding_client": SimpleNamespace(aembed_query=AsyncMock(return_value=[0.1])),
            "metric_qdrant_repository": SimpleNamespace(search=AsyncMock(return_value=[metric])),
        }
        with patch.object(value_node, "llm", RunnableLambda(lambda _: '["华北"]')), \
             patch.object(metric_node, "llm", RunnableLambda(lambda _: '["GMV"]')):
            result = await builder.compile().ainvoke(
                {"query": "华北销售额", "keywords": ["华北"]}, context=context
            )
        self.assertEqual(result["retrieved_value_infos"], [self.value])
        self.assertEqual(result["retrieved_metric_infos"], [metric])


class ValueEsRepositoryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.values = [
            ValueInfo(f"dim_region.region_name.{value}", value, "dim_region.region_name")
            for value in ("华北", "华东", "华南")
        ]
        self.client = SimpleNamespace(
            search=AsyncMock(), bulk=AsyncMock(return_value={"errors": False})
        )
        self.repository = ValueEsRepository(self.client)

    async def test_search_restores_values_and_passes_query_parameters(self):
        self.client.search.return_value = {
            "hits": {"hits": [{"_source": asdict(self.values[0])}]}
        }
        self.assertEqual(await self.repository.search("华北", 0.2, 8), self.values[:1])
        self.client.search.assert_awaited_once_with(
            index="value_index", query={"match": {"value": "华北"}},
            min_score=0.2, size=8,
        )

    async def test_search_without_hits_returns_empty_list(self):
        self.client.search.return_value = {"hits": {"hits": []}}
        self.assertEqual(await self.repository.search("不存在的取值"), [])

    async def test_bulk_uses_stable_document_ids_and_batches(self):
        await self.repository.index(self.values, batch_size=2)
        self.assertEqual(self.client.bulk.await_count, 2)
        first_batch = self.client.bulk.await_args_list[0].kwargs["operations"]
        self.assertEqual(len(first_batch), 4)  # Two action/document pairs.
        self.assertEqual(first_batch[0], {
            "index": {"_index": "value_index", "_id": self.values[0].id}
        })
        self.assertEqual(first_batch[1], asdict(self.values[0]))
        self.assertEqual(self.client.bulk.await_args_list[1].kwargs["operations"][1],
                         asdict(self.values[2]))
        await self.repository.index(self.values, batch_size=2)
        self.assertEqual(self.client.bulk.await_args_list[2].kwargs["operations"], first_batch)

    async def test_partial_bulk_failure_stops_later_batches(self):
        self.client.bulk.return_value = {
            "errors": True,
            "items": [{"index": {"error": {"reason": "invalid value"}}}],
        }
        with self.assertRaisesRegex(RuntimeError, "invalid value"):
            await self.repository.index(self.values, batch_size=1)
        self.client.bulk.assert_awaited_once()

    async def test_empty_batch_does_not_call_es(self):
        await self.repository.index([])
        self.client.bulk.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
