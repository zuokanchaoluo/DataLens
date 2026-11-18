API 第 3 模块：SSE 协议

SSE（Server-Sent Events）让服务器在一个 HTTP 响应中持续发送文本事件。上一模块实现分段发送，这一模块为每条消息增加可识别的格式。正式问数接口仍按课程计划在后续模块接入。

**本项目采用的格式**

```text
data: {"type":"progress","step":"生成SQL","status":"running"}

data: {"type":"result","data":[{"total_amount":41099.5}]}

```

每条事件都以空行结束。代码中的 `\n\n` 表示两个换行符：前一个结束 data 行，后一个形成空行。响应类型为 `text/event-stream; charset=utf-8`。

SSE 的 data 本身是文本。本项目约定该文本是一个 JSON 对象；JSON 中的 `type` 用于区分业务消息。没有输出 SSE 的 `event:` 字段，因此使用浏览器 EventSource 时，默认事件名仍是 `message`。

| JSON type | 客户端用途 |
| --- | --- |
| `progress` | 更新步骤名称与 running/success/error 状态 |
| `result` | 展示行数据和列信息 |
| `error` | 展示终止错误信息 |

`progress` 中的 error 状态与 `type=error` 是两个层次。业务工作流中校验失败可能继续纠错；明确的终止失败会发送 `type=error`。

**可复用编码工具**

`app/api/sse.py` 中的 `encode_sse(payload)` 接受 JSON 对象，执行 JSON 序列化，再添加 `data: ` 前缀和末尾空行。

- `ensure_ascii=False` 保留可读的中文；响应使用 UTF-8。
- JSON 会把字段内容里的换行转义为 `\n`，使它保持在字段内部，不会变成事件分隔符。
- `allow_nan=False` 拒绝浏览器 JSON.parse 不接受的 NaN/Infinity。
- 金额、日期等类型应先完成应用层转换，已有 SQL 结果模块已经处理了这些类型；编码器不使用 `default=str` 隐藏未知类型。

本轮只实现 SSE 的 data-only JSON 输出，不包含事件持久化、Last-Event-ID 重放或自动恢复查询功能。

**运行成功与失败示例**

激活虚拟环境，在项目根目录启动：

```powershell
python -m uvicorn app.scripts.streaming_demo:app --port 8001
```

另开终端，观察成功示例：

```powershell
curl.exe --noproxy 127.0.0.1 -N http://127.0.0.1:8001/demo/sse
```

首先立即收到 running 进度；约一秒后收到 success 进度和一条包含“你好，SSE！”的结果事件。

观察演示错误：

```powershell
curl.exe --noproxy 127.0.0.1 -N "http://127.0.0.1:8001/demo/sse?fail=true"
```

这里会依次发送 running 进度、error 进度、终止 error 消息，随后结束；不会发送 result。这是显式产生的演示消息，实际工作流异常如何转换为错误事件，将在异常处理模块实现。

**网络数据块不等于事件**

一条事件可能被拆成几次读取，例如：

```text
第 1 次：data: {"type":"pro
第 2 次：gress","step":"生成SQL"}\n
第 3 次：\n
```

只有累计收到末尾空行，才能得到完整事件。多条事件也可能在同一次读取中到达。

中文字符的 UTF-8 字节同样可能拆开，因此客户端需要先增量解码，再把文本放入缓冲区，按事件边界取出完整 JSON。现有 Vue 前端已使用 TextDecoder 的 stream 模式和双换行缓冲，能够接收本模块产生的单行 JSON、LF 分隔格式。

SSE 标准还允许 CRLF/CR 行结尾、连续多行 data 等格式；本项目编码器固定使用单行 JSON 和 LF，因此这里的教学客户端逻辑针对这一明确的输出约定。

**GET 示例与 POST 问数接口**

教学接口使用 GET，便于用 curl 或浏览器 EventSource 观察。正式问数接口按开发文档使用 `POST /api/query`，携带 JSON 请求体；现有前端使用 fetch 读取响应流。EventSource 构造器没有设置 POST 和 JSON 请求体的参数，不能直接替换现有 fetch 调用。

EventSource 自带的重新连接行为也不会自动恢复一次 POST 问数任务。后续接入采用当前 fetch 方案。

**验证范围**

`tests/test_sse.py` 验证中文与字段内换行、严格 JSON、空行边界、逐字节拆分与多事件合并，以及成功/错误教学接口的消息顺序。真实 HTTP 验证另外记录接收时间，确认 SSE 仍然逐步到达。

本次真实 HTTP 成功示例在约 0.014 秒收到首条进度、1.001 秒收到结果；失败示例在约 0.002 秒收到首条进度、1.009 秒收到错误。客户端按单字节读取，中文与 JSON 仍完整解析。记录保存在 `.local/sse_check.json`，临时服务器已关闭。本模块新增 8 项测试。

参考：[WHATWG SSE 标准](https://html.spec.whatwg.org/multipage/server-sent-events.html)、[FastAPI StreamingResponse](https://fastapi.tiangolo.com/advanced/custom-response/#streamingresponse)。
