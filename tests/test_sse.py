import codecs
import json
import unittest

import httpx

from app.api.sse import encode_sse
from app.scripts.streaming_demo import app


def read_demo_frames(chunks):
    """模拟客户端增量解码和缓冲；适用于本项目的单行 JSON、LF 分隔格式。"""
    decoder = codecs.getincrementaldecoder("utf-8")()
    buffer = ""
    events = []
    for chunk in chunks:
        buffer += decoder.decode(chunk)
        while "\n\n" in buffer:
            frame, buffer = buffer.split("\n\n", 1)
            if not frame.startswith("data: "):
                raise AssertionError("Unexpected demo event format")
            events.append(json.loads(frame[6:]))
    buffer += decoder.decode(b"", final=True)
    return events, buffer


class SSEEncodingTests(unittest.TestCase):
    def test_encodes_one_chinese_event_with_a_blank_line(self):
        frame = encode_sse({"type": "progress", "step": "生成SQL", "status": "running"})
        self.assertEqual(frame, 'data: {"type": "progress", "step": "生成SQL", "status": "running"}\n\n')

    def test_newlines_quotes_and_null_stay_inside_json_data(self):
        payload = {"type": "result", "data": [{"文字": '第一行\r\n\ndata: "第二行"', "空值": None}]}
        frame = encode_sse(payload)
        self.assertEqual(frame.count("\n"), 2)
        events, pending = read_demo_frames([frame.encode("utf-8")])
        self.assertEqual(events, [payload])
        self.assertEqual(pending, "")

    def test_requires_an_application_json_object(self):
        for payload in (None, "progress", [], 123):
            with self.subTest(payload=payload), self.assertRaises(TypeError):
                encode_sse(payload)

    def test_rejects_values_the_browser_cannot_parse_as_json(self):
        for value in (float("nan"), float("inf"), object()):
            with self.subTest(value=value), self.assertRaises((ValueError, TypeError)):
                encode_sse({"value": value})

    def test_combined_and_single_byte_chunks_produce_the_same_events(self):
        payloads = [
            {"type": "progress", "step": "召回字段", "status": "running"},
            {"type": "result", "data": [{"名称": "华北\n销售额"}]},
            {"type": "error", "message": "示例错误"},
        ]
        wire = "".join(encode_sse(item) for item in payloads).encode("utf-8")
        for chunks in ([wire], [wire[i:i + 1] for i in range(len(wire))]):
            with self.subTest(chunk_count=len(chunks)):
                events, pending = read_demo_frames(chunks)
                self.assertEqual(events, payloads)
                self.assertEqual(pending, "")

    def test_event_is_incomplete_until_its_blank_line_arrives(self):
        wire = encode_sse({"type": "progress"}).encode("utf-8")
        events, pending = read_demo_frames([wire[:-1]])
        self.assertEqual(events, [])
        self.assertNotEqual(pending, "")
        self.assertEqual(read_demo_frames([wire[:-1], wire[-1:]])[0], [{"type": "progress"}])


class SSEDemoTests(unittest.IsolatedAsyncioTestCase):
    async def get_events(self, query=""):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/demo/sse" + query)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "text/event-stream; charset=utf-8")
        self.assertEqual(response.headers["cache-control"], "no-cache")
        events, pending = read_demo_frames([response.content])
        self.assertEqual(pending, "")
        return events

    async def test_success_stream_has_progress_and_result(self):
        events = await self.get_events()
        self.assertEqual([event["type"] for event in events], ["progress", "progress", "result"])
        self.assertEqual([event["status"] for event in events[:2]], ["running", "success"])
        self.assertEqual(events[-1]["data"], [{"示例字段": "你好，SSE！"}])
        self.assertEqual(events[-1]["columns"], ["示例字段"])

    async def test_error_stream_has_no_result(self):
        events = await self.get_events("?fail=true")
        self.assertEqual([event["type"] for event in events], ["progress", "progress", "error"])
        self.assertEqual(events[-2]["status"], "error")
        self.assertEqual(events[-1]["message"], "这是一条演示错误消息。")


if __name__ == "__main__":
    unittest.main()
