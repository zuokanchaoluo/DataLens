import unittest
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

import app.scripts.lifespan_demo as demo


class LifespanTests(unittest.TestCase):
    def setUp(self):
        print_patch = patch("builtins.print")
        print_patch.start()
        self.addCleanup(print_patch.stop)

    def test_startup_prepares_shared_client_and_shutdown_closes_it(self):
        with TestClient(demo.app) as client:
            resource = demo.app.state.demo_client
            self.assertFalse(resource.is_closed)
            self.assertEqual(client.get("/demo/lifespan").json(),
                             {"resource_open": True, "request_count": 1})
            self.assertEqual(client.get("/demo/lifespan").json(),
                             {"resource_open": True, "request_count": 2})
            self.assertIs(demo.app.state.demo_client, resource)
            self.assertFalse(resource.is_closed)
        self.assertTrue(resource.is_closed)

    def test_exception_when_leaving_context_still_releases_client(self):
        with self.assertRaisesRegex(RuntimeError, "consumer failed"):
            with TestClient(demo.app):
                resource = demo.app.state.demo_client
                raise RuntimeError("consumer failed")
        self.assertTrue(resource.is_closed)

    def test_new_lifespan_creates_new_client_and_resets_counter(self):
        resources = []
        for _ in range(2):
            with TestClient(demo.app) as client:
                resources.append(demo.app.state.demo_client)
                self.assertEqual(client.get("/demo/lifespan").json()["request_count"], 1)
            self.assertTrue(resources[-1].is_closed)
        self.assertIsNot(resources[0], resources[1])

    def test_startup_failure_prevents_entering_running_state(self):
        entered = False
        with patch.object(demo.httpx, "AsyncClient", side_effect=RuntimeError("initialization failed")):
            with self.assertRaisesRegex(RuntimeError, "initialization failed"):
                with TestClient(demo.app):
                    entered = True
        self.assertFalse(entered)

    def test_request_failure_keeps_shared_client_until_shutdown(self):
        application = FastAPI(lifespan=demo.lifespan)
        application.add_api_route("/status", demo.resource_status, methods=["GET"])

        @application.get("/fail")
        async def fail():
            raise RuntimeError("one request failed")

        with TestClient(application, raise_server_exceptions=False) as client:
            resource = application.state.demo_client
            self.assertEqual(client.get("/fail").status_code, 500)
            self.assertFalse(resource.is_closed)
            self.assertEqual(client.get("/status").json(), {"resource_open": True, "request_count": 1})
        self.assertTrue(resource.is_closed)


if __name__ == "__main__":
    unittest.main()
