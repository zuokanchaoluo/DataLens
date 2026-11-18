import asyncio
import os
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import httpx
from fastapi import HTTPException

with patch.dict(os.environ, {"OPENAI_API_KEY": "offline-test-key"}):
    import app.api.dependencies as dependencies
    import app.api.exception_handlers as handlers
    import app.services.query_service as service_module
    from main import app

from test_app_lifespan import LifecycleHarness


class HTTPErrorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.harness = self.enterContext(LifecycleHarness())
        self.logger = self.enterContext(patch.object(handlers, "logger"))
        self.graph = SimpleNamespace(astream=Mock(side_effect=AssertionError("工作流不应运行")))
        self.enterContext(patch.object(service_module, "graph", self.graph))
        self.enterContext(patch.object(service_module, "logger"))
        self.secret = "private-request-and-connection-value"

    async def asyncSetUp(self):
        lifecycle = app.router.lifespan_context(app)
        await lifecycle.__aenter__()
        self.addAsyncCleanup(lifecycle.__aexit__, None, None, None)
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://test", trust_env=False,
        )
        self.addAsyncCleanup(self.client.aclose)

    def assert_json_error(self, response, status, message):
        self.assertEqual(response.status_code, status)
        self.assertEqual(response.headers["content-type"], "application/json")
        self.assertEqual(response.json(), {"type": "error", "message": message})
        self.assertNotIn(self.secret, response.text)
        self.assertNotIn("Traceback", response.text)

    def assert_request_cleaned_up(self):
        self.assertTrue(all(session.closed for session in self.harness.sessions))
        self.assertTrue(all(not resource.closed for resource in self.harness.resources))
        self.graph.astream.assert_not_called()
        for session in self.harness.sessions:
            session.commit.assert_not_awaited()

    async def test_invalid_fields_return_422_with_selected_validation_details(self):
        cases = [({}, "missing"), ({"query": " "}, "string_too_short"),
                 ({"query": 123}, "string_type"),
                 ({"query": "问题", "password": self.secret}, "extra_forbidden")]
        for payload, error_type in cases:
            with self.subTest(payload=payload):
                response = await self.client.post("/api/query", json=payload)
                self.assertEqual(response.status_code, 422)
                self.assertEqual(response.headers["content-type"], "application/json")
                body = response.json()
                self.assertEqual(body["type"], "error")
                self.assertEqual(body["message"], handlers.VALIDATION_ERROR_MESSAGE)
                self.assertEqual(body["details"][0]["type"], error_type)
                for item in body["details"]:
                    self.assertEqual(set(item), {"location", "type", "message"})
                    self.assertEqual(item["location"][0], "body")
                self.assertNotIn(self.secret, response.text)
                self.assert_request_cleaned_up()

    async def test_malformed_json_returns_422_without_raw_body_or_decoder_context(self):
        response = await self.client.post(
            "/api/query", content='{"query":"' + self.secret,
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response.status_code, 422)
        body = response.json()
        self.assertEqual(body["details"][0]["type"], "json_invalid")
        self.assertEqual(set(body["details"][0]), {"location", "type", "message"})
        self.assertNotIn(self.secret, response.text)
        self.assert_request_cleaned_up()

    async def test_uninitialized_dependency_returns_json_503(self):
        with patch.object(self.harness.managers["embedding"], "client", None):
            response = await self.client.post("/api/query", json={"query": "问题"})
        self.assert_json_error(response, 503, handlers.SERVICE_UNAVAILABLE_MESSAGE)
        self.assert_request_cleaned_up()

    async def test_session_factory_failure_returns_500_and_closes_preceding_session(self):
        with patch.object(self.harness.managers["dw"], "session_factory", Mock(side_effect=RuntimeError(self.secret))):
            response = await self.client.post("/api/query", json={"query": "问题"})
        self.assert_json_error(response, 500, handlers.INTERNAL_ERROR_MESSAGE)
        self.assertEqual(len(self.harness.sessions), 1)
        self.assert_request_cleaned_up()
        self.assertNotIn(self.secret, str(self.logger.mock_calls))

    async def test_repository_construction_value_error_is_server_error_not_input_error(self):
        with patch.object(dependencies, "MetaMySQLRepository", side_effect=ValueError(self.secret)):
            response = await self.client.post("/api/query", json={"query": "合法问题"})
        self.assert_json_error(response, 500, handlers.INTERNAL_ERROR_MESSAGE)
        self.assertEqual(len(self.harness.sessions), 1)
        self.assert_request_cleaned_up()

    async def test_service_construction_failure_returns_500_and_closes_both_sessions(self):
        with patch.object(dependencies, "QueryService", side_effect=RuntimeError(self.secret)):
            response = await self.client.post("/api/query", json={"query": "合法问题"})
        self.assert_json_error(response, 500, handlers.INTERNAL_ERROR_MESSAGE)
        self.assertEqual(len(self.harness.sessions), 2)
        self.assert_request_cleaned_up()

    async def test_synchronous_service_call_failure_before_response_creation_returns_500(self):
        with patch.object(service_module.QueryService, "query", side_effect=RuntimeError(self.secret)):
            response = await self.client.post("/api/query", json={"query": "问题"})
        self.assert_json_error(response, 500, handlers.INTERNAL_ERROR_MESSAGE)
        self.assertEqual(len(self.harness.sessions), 2)
        self.assert_request_cleaned_up()

    async def test_http_5xx_messages_are_sanitized_and_status_headers_preserved(self):
        for status in (500, 502, 503):
            with self.subTest(status=status), patch.object(
                dependencies, "QueryService",
                side_effect=HTTPException(status, detail=self.secret, headers={"Retry-After": "3"}),
            ):
                response = await self.client.post("/api/query", json={"query": "问题"})
                message = handlers.SERVICE_UNAVAILABLE_MESSAGE if status == 503 else handlers.INTERNAL_ERROR_MESSAGE
                self.assert_json_error(response, status, message)
                self.assertEqual(response.headers["retry-after"], "3")
                self.assert_request_cleaned_up()

    async def test_intentional_http_4xx_preserves_safe_message_and_authentication_header(self):
        with patch.object(dependencies, "QueryService", side_effect=HTTPException(
            401, detail="需要先登录。", headers={"WWW-Authenticate": "Bearer"},
        )):
            response = await self.client.post("/api/query", json={"query": "问题"})
        self.assert_json_error(response, 401, "需要先登录。")
        self.assertEqual(response.headers["www-authenticate"], "Bearer")
        self.assert_request_cleaned_up()

    async def test_framework_404_and_405_use_same_json_format_and_keep_allow_header(self):
        missing = await self.client.get("/missing")
        self.assert_json_error(missing, 404, "Not Found")
        wrong_method = await self.client.get("/api/query")
        self.assert_json_error(wrong_method, 405, "Method Not Allowed")
        self.assertEqual(wrong_method.headers["allow"], "POST")
        self.assertEqual(self.harness.sessions, [])

    async def test_cancellation_before_response_propagates_and_closes_sessions(self):
        with patch.object(dependencies, "QueryService", side_effect=asyncio.CancelledError()):
            with self.assertRaises(asyncio.CancelledError):
                await self.client.post("/api/query", json={"query": "问题"})
        self.assertEqual(len(self.harness.sessions), 2)
        self.assert_request_cleaned_up()
        self.logger.error.assert_not_called()

    async def test_workflow_exception_before_first_event_still_uses_sse_with_http_200(self):
        async def failing_workflow(**kwargs):
            raise RuntimeError(self.secret)
            yield  # 保持为异步生成器，函数体在消费时才执行。

        with patch.object(service_module, "graph", SimpleNamespace(astream=failing_workflow)):
            response = await self.client.post("/api/query", json={"query": "问题"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "text/event-stream; charset=utf-8")
        self.assertEqual(response.text, 'data: {"type": "error", "message": "本次查询未能完成，请稍后重试。"}\n\n')
        self.assertNotIn(self.secret, response.text)
        self.assertTrue(all(session.closed for session in self.harness.sessions))
        self.assertTrue(all(not resource.closed for resource in self.harness.resources))
        self.logger.error.assert_not_called()

    async def test_openapi_documents_json_errors_and_keeps_sse_success(self):
        response = await self.client.get("/openapi.json")
        self.assertEqual(response.status_code, 200)
        operation = response.json()["paths"]["/api/query"]["post"]
        self.assertEqual(set(operation["responses"]["200"]["content"]), {"text/event-stream"})
        for status in ("422", "500", "503"):
            content = operation["responses"][status]["content"]
            self.assertEqual(content["application/json"]["schema"]["$ref"], "#/components/schemas/ErrorSchema")
        self.assertEqual(self.harness.sessions, [])


if __name__ == "__main__":
    unittest.main()
