API 第 2 模块：流式响应

本轮实现独立的流式响应教学应用，代码位于 `app/scripts/streaming_demo.py`。它只产生示例文本，帮助观察消息何时到达；正式应用仍由 `main.py` 提供，后续模块接入 SSE 和真实问数服务。

**启动与观察**

在项目根目录、已激活的虚拟环境中启动示例：

```powershell
python -m uvicorn app.scripts.streaming_demo:app --port 8001
```

另开一个终端，请求普通响应：

```powershell
curl.exe --noproxy 127.0.0.1 -N http://127.0.0.1:8001/demo/normal
```

约等待 4 秒后，一起看到 5 条消息。然后请求流式响应：

```powershell
curl.exe --noproxy 127.0.0.1 -N http://127.0.0.1:8001/demo/stream
```

第一条很快出现，后续约每隔 1 秒出现一条。两个接口的最终文本相同，区别在于发送时间。

Windows PowerShell 中使用 `curl.exe` 明确调用 curl 程序，`-N` 禁用 curl 的输出缓冲，`--noproxy 127.0.0.1` 让本机示例请求直连。用这两个命令比较，不用 Swagger 的最终响应文本判断消息到达时间。

| 时间（示意） | 普通响应客户端 | 流式响应客户端 |
| --- | --- | --- |
| 0 秒 | 等待 | 第 1 条消息 |
| 1 秒 | 等待 | 第 2 条消息 |
| 2 秒 | 等待 | 第 3 条消息 |
| 3 秒 | 等待 | 第 4 条消息 |
| 4 秒 | 收到全部 5 条 | 第 5 条消息，响应结束 |

时间受调度和网络影响，这张表表示示例的预期顺序。

**代码中的三个角色**

1. `generate_messages()` 负责产生数据。它是包含 `yield` 的异步生成器，第一次产生一条消息，后续在异步等待后继续产生消息。
2. `normal_response()` 把生成器的所有消息收集到列表，拼成完整字符串后返回，因此产生中间消息时客户端还在等待。
3. `streaming_response()` 把生成器对象交给 `StreamingResponse`。响应对象迭代生成器，每取得一段内容就将其交给服务器发送。

流式接口的核心代码：

```python
return StreamingResponse(generate_messages(), media_type="text/plain")
```

`generate_messages()` 返回可供异步迭代的对象；`StreamingResponse` 负责迭代和发送。此处不用 `await generate_messages()`。

**return、yield 和 await**

- `return`：在本例的接口函数中返回一个响应对象，函数结束。返回的 `StreamingResponse` 对象随后继续消费生成器并发送响应体。
- `yield`：在生成器中交出当前这条消息，保留执行位置和局部变量，下一次迭代时继续。
- `await asyncio.sleep(1)`：等待一秒并把执行机会让给事件循环，让其他协程能够运行。本例用它模拟两条消息之间的等待。

单纯把函数声明成 `async def` 不会自动变成流式响应。普通示例接口同样是异步函数，但它仍会等待所有消息生成完毕。

在已有工作流中，节点通过 `runtime.stream_writer(...)` 产生消息，外层通过 `graph.astream(...)` 接收。后续 QueryService 会把这些真实消息交给流式响应，替换当前示例消息的来源。

**与下一模块 SSE 的关系**

本例使用 `text/plain; charset=utf-8`。流式响应解决“何时把内容发出去”；下一模块 SSE 规定“每条事件如何编码、如何区分边界”。

一次 `yield` 不保证对应客户端一次网络读取。传输过程中内容可能拆分或合并；本例使用换行展示文本，SSE 会定义明确的事件边界。流式响应属于同一个 HTTP 响应，不是为每条消息重新发起请求。

**验证方式**

`tests/test_streaming_response.py` 使用受控异步生成器，在后半段消息尚未允许产生时检查：流式接口已经发送第一条，普通接口尚未发送响应。这样能够验证发送时机，而不仅仅比较最后收集到的字符串。

另外通过真实 Uvicorn 和 HTTP 客户端记录到达时间，验证默认示例的实际分段传输。本次普通响应的 5 条消息均在约 4.056 秒收到；流式响应的 5 条消息分别在约 0.002、1.012、2.022、3.051、4.061 秒收到。记录保存在 `.local/streaming_check.json`。这些数值是本次测量结果，复跑时会变化。

本模块不需要数据库、Embedding 或模型密钥；验证用的临时服务器已在测试结束后关闭。

参考：[FastAPI StreamingResponse](https://fastapi.tiangolo.com/advanced/custom-response/#streamingresponse)。
