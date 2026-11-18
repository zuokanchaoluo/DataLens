import json
import os
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

import httpx

from app.clients.embedding_client_manager import EmbeddingClientManager
from app.conf.app_config import EmbeddingConfig
from app.scripts.check_data_agent import results_match


class LocalEmbeddingTests(unittest.IsolatedAsyncioTestCase):
    async def test_local_tei_bypasses_broken_proxy_and_closes_client(self):
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                requests.append(body)
                inputs = body["inputs"]
                vectors = [[0.1, 0.2, 0.3] for _ in inputs]
                data = json.dumps(vectors).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        proxy = "http://127.0.0.1:1"
        try:
            with patch.dict(os.environ, {"HTTP_PROXY": proxy, "HTTPS_PROXY": proxy, "ALL_PROXY": proxy,
                                         "NO_PROXY": "", "no_proxy": ""}):
                url = f"http://127.0.0.1:{server.server_port}"
                async with httpx.AsyncClient(timeout=1) as client:
                    with self.assertRaises(httpx.RequestError):
                        await client.get(url)
                os.environ["NO_PROXY"] = "internal.example"
                manager = EmbeddingClientManager(EmbeddingConfig("127.0.0.1", server.server_port, "BAAI/bge-large-zh-v1.5"))
                manager.init()
                try:
                    vector = await manager.client.aembed_query("测试")
                    self.assertEqual(len(vector), 3)
                    self.assertEqual(len(requests), 1)
                    self.assertIn("internal.example", os.environ["NO_PROXY"])
                    self.assertEqual(os.environ["HTTP_PROXY"], proxy)
                finally:
                    await manager.close()
                self.assertIsNone(manager.client)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


class ResultComparisonTests(unittest.TestCase):
    @staticmethod
    def result(rows, columns=None, truncated=False):
        return {"columns": columns or list(rows[0]), "rows": rows,
                "row_count": len(rows), "truncated": truncated}

    def test_allows_alias_order_and_decimal_encoding_differences(self):
        expected = self.result([{"region": "华北", "amount": 12.5}, {"region": "华南", "amount": 3.0}])
        actual = self.result([{"total": "3.00", "area": "华南"}, {"total": "12.50", "area": "华北"}])
        self.assertTrue(results_match(actual, expected))

    def test_rejects_wrong_amount_duplicate_rows_missing_columns_and_truncation(self):
        expected = self.result([{"region": "华北", "amount": 12.5}, {"region": "华南", "amount": 3.0}])
        candidates = [
            self.result([{"region": "华北", "amount": 125.0}, {"region": "华南", "amount": 3.0}]),
            self.result([{"region": "华北", "amount": 12.5}, {"region": "华北", "amount": 12.5}]),
            self.result([{"amount": 12.5}, {"amount": 3.0}]),
            self.result(expected["rows"], truncated=True),
        ]
        for actual in candidates:
            with self.subTest(actual=actual):
                self.assertFalse(results_match(actual, expected))


if __name__ == "__main__":
    unittest.main()
