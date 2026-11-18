import asyncio
import os
import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import httpx

with patch.dict(os.environ, {"OPENAI_API_KEY": "offline-test-key"}):
    import app.api.dependencies as dependencies
    import app.core.lifespan as lifecycle_module
    import app.services.query_service as service_module
    from main import app

import app.clients.embedding_client_manager as embedding_module
import app.clients.es_client_manager as es_module
import app.clients.mysql_client_manager as mysql_module
import app.clients.qdrant_client_manager as qdrant_module
from app.conf.app_config import app_config


class TrackingResource:
    def __init__(self, name, harness):
        self.name = name
        self.harness = harness
        self.closed = False
        self.close_count = 0

    async def close(self):
        self.closed = True
        self.close_count += 1
        if self.harness.fail_close == self.name:
            raise RuntimeError(f"{self.name} close failed")

    async def dispose(self):
        await self.close()


class TrackingSyncResource(TrackingResource):
    def close(self):
        self.closed = True
        self.close_count += 1


class TrackingSession:
    def __init__(self, engine):
        self.engine = engine
        self.closed = False
        self.commit = AsyncMock()

    async def __aenter__(self):
        assert not self.engine.closed
        return self

    async def __aexit__(self, *args):
        self.closed = True


class LifecycleHarness:
    """保留真实管理器、依赖和应用，只替换底层 SDK 构造器。"""

    names = ("embedding", "qdrant", "es", "meta", "dw")

    def __init__(self, fail_init=None, fail_close=None):
        self.fail_init = fail_init
        self.fail_close = fail_close
        self.events = []
        self.resources = []
        self.sessions = []
        self.managers = {
            "embedding": embedding_module.EmbeddingClientManager(app_config.embedding),
            "qdrant": qdrant_module.QdrantClientManager(app_config.qdrant),
            "es": es_module.EsClientManager(app_config.es),
            "meta": mysql_module.MysqlClientManager(app_config.db_meta),
            "dw": mysql_module.MysqlClientManager(app_config.db_dw),
        }

    def resource(self, name, sync=False):
        if self.fail_init == name:
            raise RuntimeError(f"{name} init failed")
        resource = (TrackingSyncResource if sync else TrackingResource)(name, self)
        self.resources.append(resource)
        return resource

    def engine(self, url, **kwargs):
        name = "meta" if url == self.managers["meta"]._get_url() else "dw"
        return self.resource(name)

    def session_factory(self, engine, **kwargs):
        if self.fail_init == engine.name + "-factory":
            raise RuntimeError(f"{engine.name} factory failed")

        def create():
            session = TrackingSession(engine)
            self.sessions.append(session)
            return session

        return create

    def __enter__(self):
        self.stack = ExitStack()
        attributes = {
            "embedding": "embedding_client_manager", "qdrant": "qdrant_client_manager",
            "es": "es_client_manager", "meta": "meta_mysql_client_manager", "dw": "dw_mysql_client_manager",
        }
        for name, manager in self.managers.items():
            original_init, original_close = manager.init, manager.close

            def initialize(name=name, initialize=original_init):
                self.events.append(("init", name))
                return initialize()

            async def close(name=name, close=original_close):
                self.events.append(("close", name))
                await close()

            manager.init = Mock(side_effect=initialize)
            manager.close = AsyncMock(side_effect=close)
            for module in (lifecycle_module, dependencies):
                self.stack.enter_context(patch.object(module, attributes[name], manager))

        # 包装对象自身的默认 SDK 客户端尚未发请求；故障测试中也允许它们关闭。
        def embedding_wrapper(**kwargs):
            return SimpleNamespace(client=SimpleNamespace(close=Mock()),
                                   async_client=SimpleNamespace(close=AsyncMock()))

        constructors = [
            patch.object(embedding_module, "HuggingFaceEndpointEmbeddings", side_effect=embedding_wrapper),
            patch.object(embedding_module, "InferenceClient", side_effect=lambda **_: self.resource("embedding-sync", sync=True)),
            patch.object(embedding_module, "AsyncInferenceClient", side_effect=lambda **_: self.resource("embedding")),
            patch.object(qdrant_module, "AsyncQdrantClient", side_effect=lambda **_: self.resource("qdrant")),
            patch.object(es_module, "AsyncElasticsearch", side_effect=lambda **_: self.resource("es")),
            patch.object(mysql_module, "create_async_engine", side_effect=self.engine),
            patch.object(mysql_module, "async_sessionmaker", side_effect=self.session_factory),
        ]
        for constructor in constructors:
            self.stack.enter_context(constructor)
        return self

    def __exit__(self, *args):
        return self.stack.__exit__(*args)


class AppLifespanTests(unittest.IsolatedAsyncioTestCase):
    def assert_reset(self, harness):
        for name, manager in harness.managers.items():
            if name in ("meta", "dw"):
                self.assertIsNone(manager.engine)
                self.assertIsNone(manager.session_factory)
            else:
                self.assertIsNone(manager.client)

    async def test_main_app_initializes_once_and_closes_in_reverse_order(self):
        with LifecycleHarness() as harness:
            async with app.router.lifespan_context(app):
                self.assertEqual(harness.events, [("init", name) for name in harness.names])
                self.assertEqual(len(harness.resources), 6)
                self.assertTrue(all(not resource.closed for resource in harness.resources))
                self.assertEqual(harness.sessions, [])
            self.assertEqual(harness.events[5:], [("close", name) for name in reversed(harness.names)])
            self.assertTrue(all(resource.closed and resource.close_count == 1 for resource in harness.resources))
            self.assert_reset(harness)

    async def test_failure_at_each_startup_stage_closes_all_allocated_resources(self):
        for failed in LifecycleHarness.names:
            with self.subTest(failed=failed), LifecycleHarness(fail_init=failed) as harness:
                with self.assertRaisesRegex(RuntimeError, f"{failed} init failed"):
                    async with app.router.lifespan_context(app):
                        self.fail("启动失败时不能进入运行阶段")
                index = harness.names.index(failed)
                self.assertEqual(harness.events[:index + 1], [("init", name) for name in harness.names[:index + 1]])
                self.assertEqual(harness.events[index + 1:], [("close", name) for name in reversed(harness.names[:index + 1])])
                self.assertTrue(all(resource.closed for resource in harness.resources))
                self.assert_reset(harness)

    async def test_mysql_engine_is_closed_if_session_factory_creation_fails(self):
        for failed in ("meta-factory", "dw-factory"):
            with self.subTest(failed=failed), LifecycleHarness(fail_init=failed) as harness:
                with self.assertRaisesRegex(RuntimeError, "factory failed"):
                    async with app.router.lifespan_context(app):
                        self.fail("会话工厂创建失败时不能进入运行阶段")
                self.assertTrue(all(resource.closed and resource.close_count == 1 for resource in harness.resources))
                self.assert_reset(harness)

    async def test_shutdown_failure_does_not_skip_other_cleanup(self):
        with LifecycleHarness(fail_close="dw") as harness:
            with self.assertRaisesRegex(RuntimeError, "dw close failed"):
                async with app.router.lifespan_context(app):
                    pass
            self.assertEqual(harness.events[5:], [("close", name) for name in reversed(harness.names)])
            self.assertTrue(all(resource.closed for resource in harness.resources))
            self.assert_reset(harness)

    async def test_embedding_async_close_failure_still_closes_sync_client(self):
        with LifecycleHarness(fail_close="embedding") as harness:
            with self.assertRaisesRegex(RuntimeError, "embedding close failed"):
                async with app.router.lifespan_context(app):
                    pass
            self.assertTrue(all(resource.closed for resource in harness.resources))
            self.assert_reset(harness)

    async def test_overlapping_lifespan_rejected_without_closing_active_resources(self):
        with LifecycleHarness() as harness:
            async with app.router.lifespan_context(app):
                with self.assertRaisesRegex(RuntimeError, "共享资源已初始化"):
                    async with app.router.lifespan_context(app):
                        self.fail("不能覆盖另一个生命周期的资源")
                self.assertTrue(all(not resource.closed for resource in harness.resources))
                self.assertEqual(harness.events, [("init", name) for name in harness.names])
            self.assert_reset(harness)

    async def test_preexisting_resource_is_preserved_when_startup_is_rejected(self):
        with LifecycleHarness() as harness:
            manager = harness.managers["qdrant"]
            manager.init()
            resource = manager.client
            try:
                with self.assertRaisesRegex(RuntimeError, "共享资源已初始化"):
                    async with app.router.lifespan_context(app):
                        self.fail("已有资源时不能进入运行阶段")
                self.assertIs(manager.client, resource)
                self.assertFalse(resource.closed)
                manager.close.assert_not_awaited()
                self.assertEqual(harness.events, [("init", "qdrant")])
            finally:
                await manager.close()

    async def test_each_manager_rejects_duplicate_init_and_allows_repeated_close(self):
        with LifecycleHarness() as harness:
            # 尚未初始化也可以关闭。
            for manager in harness.managers.values():
                await manager.close()
            async with app.router.lifespan_context(app):
                resources = list(harness.resources)
                for name, manager in harness.managers.items():
                    with self.subTest(manager=name), self.assertRaisesRegex(RuntimeError, "已初始化"):
                        manager.init()
                self.assertEqual(harness.resources, resources)
                self.assertTrue(all(not resource.closed for resource in resources))
            for manager in harness.managers.values():
                await manager.close()
            self.assertTrue(all(resource.close_count == 1 for resource in harness.resources))
            self.assert_reset(harness)

    async def test_new_lifespan_creates_new_resources_after_previous_shutdown(self):
        with LifecycleHarness() as harness:
            batches = []
            for _ in range(2):
                async with app.router.lifespan_context(app):
                    batches.append(list(harness.resources[-6:]))
                    self.assertTrue(all(not resource.closed for resource in batches[-1]))
                self.assert_reset(harness)
            self.assertTrue(all(first is not second for first, second in zip(*batches)))
            self.assertTrue(all(resource.closed and resource.close_count == 1 for resource in harness.resources))

    async def test_body_exception_and_cancellation_both_release_resources(self):
        for error in (RuntimeError("application failed"), asyncio.CancelledError()):
            with self.subTest(error=type(error).__name__), LifecycleHarness() as harness:
                with self.assertRaises(type(error)):
                    async with app.router.lifespan_context(app):
                        raise error
                self.assertTrue(all(resource.closed for resource in harness.resources))
                self.assert_reset(harness)

    async def test_retry_after_partial_startup_failure_creates_usable_resources(self):
        with LifecycleHarness(fail_init="dw-factory") as harness:
            with self.assertRaises(RuntimeError):
                async with app.router.lifespan_context(app):
                    pass
            self.assert_reset(harness)
            old_resources = list(harness.resources)
            harness.fail_init = None
            async with app.router.lifespan_context(app):
                new_resources = harness.resources[len(old_resources):]
                self.assertEqual(len(new_resources), 6)
                self.assertTrue(all(not resource.closed for resource in new_resources))
            self.assertTrue(all(resource.closed for resource in harness.resources))

    async def test_concurrent_queries_share_clients_and_keep_request_sessions_separate(self):
        contexts = []

        async def workflow(*, input, context, **kwargs):
            contexts.append(context)
            yield "custom", {"type": "progress", "step": "生命周期验证", "status": "running"}
            await asyncio.sleep(0)
            for key in ("meta_mysql_repository", "dw_mysql_repository"):
                session = context[key].session
                self.assertFalse(session.closed)
                self.assertFalse(session.engine.closed)
            if input["query"] == "故障":
                raise RuntimeError("offline workflow failure")
            result = {"columns": ["question"], "rows": [{"question": input["query"]}],
                      "row_count": 1, "truncated": False}
            yield "custom", {"type": "result", "data": result["rows"], "columns": result["columns"],
                             "row_count": 1, "truncated": False}
            yield "values", {"error": None, "sql_result": result}

        with (LifecycleHarness() as harness,
              patch.object(service_module, "graph", SimpleNamespace(astream=workflow)),
              patch.object(service_module, "logger")):
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://test", trust_env=False,
                ) as client:
                    first, second = await asyncio.gather(*[
                        client.post("/api/query", json={"query": question}) for question in ("问题一", "问题二")
                    ])
                    self.assertEqual((first.status_code, second.status_code), (200, 200))
                    self.assertIn("问题一", first.text)
                    self.assertIn("问题二", second.text)
                    self.assertEqual(len(harness.sessions), 4)
                    self.assertTrue(all(session.closed for session in harness.sessions))
                    self.assertIs(contexts[0]["embedding_client"], contexts[1]["embedding_client"])
                    for key in ("column_qdrant_repository", "metric_qdrant_repository", "value_es_repository"):
                        self.assertIs(contexts[0][key].client, contexts[1][key].client)
                    for key in ("meta_mysql_repository", "dw_mysql_repository"):
                        self.assertIsNot(contexts[0][key].session, contexts[1][key].session)
                    failure = await client.post("/api/query", json={"query": "故障"})
                    self.assertIn('"type": "error"', failure.text)
                    recovery = await client.post("/api/query", json={"query": "后续请求"})
                    self.assertIn('"type": "result"', recovery.text)
                self.assertTrue(all(not resource.closed for resource in harness.resources))
                self.assertEqual(harness.events, [("init", name) for name in harness.names])
                self.assertTrue(all(session.closed for session in harness.sessions))
                for session in harness.sessions:
                    session.commit.assert_not_awaited()
            self.assertTrue(all(resource.closed for resource in harness.resources))
            self.assert_reset(harness)


if __name__ == "__main__":
    unittest.main()
