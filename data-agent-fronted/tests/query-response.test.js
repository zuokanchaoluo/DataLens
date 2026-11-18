import assert from "node:assert/strict";
import test from "node:test";
import {QueryResponseError, requireQueryStream} from "../src/api/query-response.js";

function rejectsWith(response, message, status = null) {
  return assert.rejects(requireQueryStream(response), (error) => {
    assert.ok(error instanceof QueryResponseError);
    assert.equal(error.message, message);
    assert.equal(error.status, status);
    return true;
  });
}

test("422、503、500 读取后端 ErrorSchema 的中文消息和状态码", async () => {
  for (const status of [422, 503, 500]) {
    const message = `本次请求失败（${status}）。`;
    const response = Response.json({type: "error", message, details: []}, {status});
    await rejectsWith(response, message, status);
  }
});

test("HTML 代理错误不当作 SSE，也不把原始页面显示给用户", async () => {
  const response = new Response("<html>proxy failure with internal context</html>", {
    status: 502, headers: {"Content-Type": "text/html"},
  });
  await rejectsWith(response, "服务暂时无法完成请求，请稍后重试。", 502);
  assert.equal(response.bodyUsed, false);
});

test("损坏的 JSON 使用对应 HTTP 状态的中文兜底消息", async () => {
  const response = new Response("{broken", {
    status: 503, headers: {"Content-Type": "application/json; charset=utf-8"},
  });
  await rejectsWith(response, "查询服务尚未就绪，请稍后重试。", 503);
});

test("不符合 ErrorSchema 的 JSON、空消息、非字符串消息均使用兜底", async () => {
  for (const payload of [null, {detail: "internal"}, {message: "internal"},
    {type: "error", message: " "}, {type: "error", message: {internal: true}}]) {
    await rejectsWith(Response.json(payload, {status: 422}),
      "请求参数不合法，请检查提交内容。", 422);
  }
});

test("其他 HTTP 错误也明确失败，不读取错误类型的响应体", async () => {
  for (const status of [400, 404, 429]) {
    await rejectsWith(new Response("failure", {status}),
      "请求未能完成，请检查请求后重试。", status);
  }
});

test("HTTP 成功但响应类型错误时，拒绝进入 SSE 读取循环", async () => {
  for (const mediaType of ["application/json", "text/html", "text/event-streaming", ""]) {
    const response = new Response("unexpected", {headers: {"Content-Type": mediaType}});
    await rejectsWith(response, "服务器返回的响应格式不正确，请稍后重试。");
    assert.equal(response.bodyUsed, false);
  }
});

test("SSE 响应无 body 时给出明确提示", async () => {
  await rejectsWith(new Response(null, {headers: {"Content-Type": "text/event-stream"}}),
    "服务器未返回查询数据，请稍后重试。");
});

test("正常 SSE 保持流式读取，中文逐字节拆包和流内 error 交由原循环处理", async () => {
  const events = 'data: {"type":"progress","step":"生成SQL","status":"running"}\n\n'
    + 'data: {"type":"error","message":"本次查询未能完成，请稍后重试。"}\n\n';
  const bytes = new TextEncoder().encode(events);
  const stream = new ReadableStream({
    start(controller) {
      for (const byte of bytes) controller.enqueue(Uint8Array.of(byte));
      controller.close();
    },
  });
  const response = new Response(stream, {
    headers: {"Content-Type": "Text/Event-Stream; charset=utf-8"},
  });
  assert.equal(await requireQueryStream(response), response.body);
  assert.equal(response.bodyUsed, false);
  assert.equal(await response.text(), events);
});
