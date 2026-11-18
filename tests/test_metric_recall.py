import os
import unittest
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch

from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, START, StateGraph

# Importing the model creates a client but must not require a real credential.
# All model, embedding and database calls below are replaced with test doubles.
with patch.dict(os.environ, {"OPENAI_API_KEY": "offline-test-key"}):
    import app.agent.nodes.recall_metric as metric_node
    from app.entities.metric_info import MetricInfo
    from app.repositories.qdrant.metric_qdrant_repository import MetricQdrantRepository
    from app.services.meta_knowledge_service import MetaKnowledgeService


class MetricRecallTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.metric = MetricInfo(
            id="GMV", name="GMV", description="成交金额总和",
            relevant_columns=["fact_order.order_amount"], alias=["销售额"],
        )
        self.embedding = SimpleNamespace(
            aembed_query=AsyncMock(return_value=[0.1, 0.2])
        )
        self.repository = SimpleNamespace(search=AsyncMock(return_value=[]))
        self.events = []
        self.runtime = SimpleNamespace(
            stream_writer=self.events.append,
            context={
                "embedding_client": self.embedding,
                "metric_qdrant_repository": self.repository,
            },
        )
        logger_patch = patch.object(metric_node, "logger")
        logger_patch.start()
        self.addCleanup(logger_patch.stop)

    async def invoke(self, response, keywords=None):
        state = {"query": "统计销售额", "keywords": keywords or []}
        with patch.object(metric_node, "llm", RunnableLambda(lambda _: response)):
            return await metric_node.recall_metric(state, self.runtime)

    async def test_deduplicates_keywords_and_metric_ids(self):
        self.repository.search.return_value = [self.metric, self.metric]
        result = await self.invoke('[" GMV ", "销售额", "", "GMV"]', ["销售额"])
        self.assertEqual(result, {"retrieved_metric_infos": [self.metric]})
        self.assertEqual(self.embedding.aembed_query.await_args_list,
                         [call("销售额"), call("GMV")])
        self.assertEqual(self.repository.search.await_count, 2)
        self.assertEqual([event["status"] for event in self.events],
                         ["running", "success"])

    async def test_no_hits_is_a_successful_empty_result(self):
        result = await self.invoke('["GMV"]')
        self.assertEqual(result, {"retrieved_metric_infos": []})
        self.assertEqual(self.events[-1]["status"], "success")

    async def test_node_result_is_saved_in_graph_state(self):
        builder = StateGraph(
            metric_node.DataAgentState, context_schema=metric_node.DataAgentContext
        )
        builder.add_node("recall_metric", metric_node.recall_metric)
        builder.add_edge(START, "recall_metric")
        builder.add_edge("recall_metric", END)
        self.repository.search.return_value = [self.metric]
        with patch.object(metric_node, "llm", RunnableLambda(lambda _: '["GMV"]')):
            result = await builder.compile().ainvoke(
                {"query": "统计销售额", "keywords": ["销售额"]},
                context={**self.runtime.context, "column_qdrant_repository": None},
            )
        self.assertEqual(result["retrieved_metric_infos"], [self.metric])
        self.assertEqual(result["query"], "统计销售额")

    async def test_empty_keywords_do_not_query_services(self):
        result = await self.invoke('[]')
        self.assertEqual(result, {"retrieved_metric_infos": []})
        self.embedding.aembed_query.assert_not_awaited()
        self.repository.search.assert_not_awaited()

    async def test_rejects_wrong_model_output_shape(self):
        for response in ('{"keywords": ["GMV"]}', '["GMV", 7]'):
            with self.subTest(response=response):
                with self.assertRaisesRegex(ValueError, "JSON 数组"):
                    await self.invoke(response)
                self.assertEqual(self.events[-1]["status"], "error")
        self.embedding.aembed_query.assert_not_awaited()

    async def test_service_failure_is_reported_and_raised(self):
        self.repository.search.side_effect = RuntimeError("Qdrant unavailable")
        with self.assertRaisesRegex(RuntimeError, "Qdrant unavailable"):
            await self.invoke('["GMV"]')
        self.assertEqual(self.events[-1]["status"], "error")

    async def test_qdrant_search_restores_metric_entities(self):
        client = SimpleNamespace(query_points=AsyncMock(return_value=
            SimpleNamespace(points=[SimpleNamespace(payload=asdict(self.metric))])
        ))
        repository = MetricQdrantRepository(client)
        result = await repository.search([0.1, 0.2], score_threshold=0.7, limit=3)
        self.assertEqual(result, [self.metric])
        client.query_points.assert_awaited_once_with(
            collection_name="metric_info_collection", query=[0.1, 0.2],
            score_threshold=0.7, limit=3,
        )

    async def test_builder_returns_metrics_for_vector_indexing(self):
        transaction = MagicMock()
        transaction.__aenter__ = AsyncMock()
        transaction.__aexit__ = AsyncMock(return_value=False)
        meta_repository = SimpleNamespace(
            session=SimpleNamespace(begin=MagicMock(return_value=transaction)),
            save_metric_infos=AsyncMock(), save_column_metrics=AsyncMock(),
        )
        service = MetaKnowledgeService(meta_repository, None, None, None, None, None)
        result = await service._save_metrics_to_meta_db(SimpleNamespace(
            metrics=[self.metric]
        ))
        self.assertEqual(result, [self.metric])
        meta_repository.save_metric_infos.assert_awaited_once_with([self.metric])
        relationships = meta_repository.save_column_metrics.await_args.args[0]
        self.assertEqual(relationships[0].metric_id, "GMV")
        self.assertEqual(relationships[0].column_id, "fact_order.order_amount")


if __name__ == "__main__":
    unittest.main()
