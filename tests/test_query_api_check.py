import unittest
from unittest.mock import patch

import httpx

from app.api.sse import encode_sse
from app.scripts.check_query_api import APICheckError, read_query


class ByteStream(httpx.AsyncByteStream):
    def __init__(self, body):
        self.body = body

    async def __aiter__(self):
        # 刻意在中文 UTF-8 字符中间拆包，交给 HTTPX 增量解码。
        for value in self.body:
            yield bytes([value])


class QueryAPICheckTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(patch("builtins.print"))
        self.result = {"type": "result", "data": [{"销售额": "41099.50"}],
                       "columns": ["销售额"], "row_count": 1, "truncated": False}
        self.progress = {"type": "progress", "step": "生成SQL", "status": "running"}

    async def invoke(self, body, *, status=200, request_id="a" * 32):
        def response(request):
            return httpx.Response(status, request=request, stream=ByteStream(body),
                                  headers={"content-type": "text/event-stream; charset=utf-8",
                                           "cache-control": "no-cache", "x-request-id": request_id})

        async with httpx.AsyncClient(transport=httpx.MockTransport(response), base_url="http://test") as client:
            return await read_query(client, "region-total", 2)

    async def test_reads_split_chinese_frames_and_preserves_result_metadata(self):
        wire = (encode_sse(self.progress) + encode_sse(self.result)).encode()
        report = await self.invoke(wire)
        self.assertEqual(report["request_id"], "a" * 32)
        self.assertEqual([item["event"] for item in report["events"]], [self.progress, self.result])
        self.assertEqual(report["actual"], {"rows": self.result["data"], "columns": ["销售额"], "row_count": 1, "truncated": False})

    async def test_rejects_business_error_in_http_200_response(self):
        with self.assertRaisesRegex(APICheckError, "终止错误"):
            await self.invoke(encode_sse({"type": "error", "message": "private-test-message"}).encode())

    async def test_rejects_duplicate_terminal_event_late_progress_and_incomplete_frame(self):
        frames = [encode_sse(self.result) * 2, encode_sse(self.result) + encode_sse(self.progress),
                  encode_sse(self.result)[:-1]]
        for wire in frames:
            with self.subTest(wire=wire), self.assertRaises(APICheckError):
                await self.invoke(wire.encode())

    async def test_rejects_http_error_or_invalid_request_id(self):
        for status, request_id in ((503, "a" * 32), (200, "invalid-id")):
            with self.subTest(status=status), self.assertRaises(APICheckError):
                await self.invoke(encode_sse(self.result).encode(), status=status, request_id=request_id)


if __name__ == "__main__":
    unittest.main()
