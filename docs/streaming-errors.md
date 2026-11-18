API 第 11 模块：流内异常、终止与断开连接

本模块完成 SSE 响应已经开始后的失败与清理。目标是：连接正常时产生一条最终 result 或 error；客户端断开或任务取消时，停止消费并完成资源清理，不额外发送业务失败事件。

**改动位置**

| 文件 | 作用 |
| --- | --- |
| `app/api/streaming_response.py` | 新增 QueryStreamingResponse，负责发送、监听断开、等待任务与关闭生成器 |
| `app/core/async_cleanup.py` | 新增 complete_cleanup，等待异步收尾完成，再继续传播取消 |
| `app/services/query_service.py` | 统一终止出口，取消/关闭与普通失败分别处理 |
| `app/api/dependencies.py` | 会话退出时使用受保护的异步清理 |
| `app/api/routers/query_router.py` | 使用 QueryStreamingResponse，接口路径、SSE 格式与错误模型保留 |
| `tests/test_streaming_errors.py` | 15 项流内异常与断开验证 |

**这次实际复现的问题**

发送一条 SSE 时，服务生成器已经执行到 yield，把文本交给响应层，然后暂停。如果网络发送此时失败，异常发生在响应层；服务生成器并未自动收到这个发送异常，因此不能仅依赖它内部的 finally 被及时执行。

本轮先写回归测试，确认原实现存在：发送失败后生成器未及时关闭；ASGI 2.4 分支在等待工作流时没有主动读取断开事件；AnyIO 取消路径中，真实 LangGraph 节点的异步清理可能晚于会话关闭。再增加带异步等待的 Session 退出测试，复现取消域打断会话清理的情况。

修复后测试持有生成器引用，直接检查请求处理返回时已经关闭，不依赖垃圾回收或事件循环结束来释放资源。

**响应层为什么需要两个任务**

QueryStreamingResponse 继承 StreamingResponse，保留原有的分块发送与 HTTP/SSE 编码行为，使用两个任务协调：

```text
发送任务：消费 QueryService → 发送 SSE
监听任务：读取 receive → 等待 http.disconnect
```

用 asyncio.wait(..., FIRST_COMPLETED) 等待任意一侧结束。正常发送完成，取消监听任务；收到断开事件，取消发送任务；外部取消请求时，两者都进入收尾。

仅调用 task.cancel() 是发出取消请求，不代表任务已经结束。因此响应层还会 gather 等待任务完成，并显式关闭服务生成器。发送和关闭在同一个发送任务中进行，覆盖停在 yield 时断开的情况；发送任务尚未开始就被取消时，也有额外的生成器关闭路径。

原 Starlette 实现按 ASGI spec_version 采取不同断开机制。本项目既验证了 ASGI 2.3 下的真实 Uvicorn，也构造了 2.4 下“工作流等待期间断开”的测试。新的响应类始终监听断开；send 抛出的 OSError 也作为传输断开处理。ASGI 的 [disconnect 事件与关闭后的 send](https://asgi.readthedocs.io/en/latest/specs/www.html#disconnect-receive-event)解释了这些信号的含义。

该响应类专用于本项目可 aclose 的异步生成器，不作为任意同步迭代器的通用替代品。

**业务失败与取消的处理不同**

| 情况 | 服务行为 |
| --- | --- |
| 普通流内异常 | 清理工作流后，产生一次通用 error |
| 纠错耗尽，带 error 正常 END | 识别最终 State，产生一次 error，不执行 SQL |
| progress.status 为 error，之后修复成功 | 保留进度，最终产生一次 result |
| 收到明确的 type=error 自定义事件 | 停止继续消费，清理后产生通用 error |
| 节点先发送 result，随后失败 | 暂存结果不外发，最终只产生 error |
| 结果事件重复 | 保留第一份，确认成功后只产生一次 result |
| 取消、显式关闭、连接断开 | 取消/关闭继续传播，不追加业务 error |

QueryService 现在先消费并暂存结果，在 finally 中关闭工作流，再编码并 yield 最后一条事件。终止 yield 位于业务异常捕获和工作流关闭之后，避免关闭或调用方异常导致再次进入错误输出分支。

取消/关闭期间的清理失败会记录通用日志或异常类型，不把原始异常发送给客户端，也不会因清理报错而重新恢复业务事件流。HTTP 已开始，仍使用现有 HTTP 200 + SSE 协议；第 10 模块的全局处理器不能把它改写成新的 500 JSON。

这里的“至多一次”约束是本次生成器产生的终止事件，不是网络端到端的恰好一次交付。断开的客户端可能没有收到终止事件；当前仍无事件 ID、断线续传或自动重放。

**为什么要保护清理**

asyncio 取消会在等待处抛出 CancelledError；AnyIO 取消域还可能在后续 await 处继续触发取消。如果关闭生成器或 Session 本身需要 await，清理也可能被中断。

complete_cleanup 为收尾保留一个任务引用，用 asyncio.shield 等待它，并在清理区间使用 AnyIO shield；清理期间收到的取消先被记住，等任务结束后继续传播。这样处理了取消恰好发生在工作流清理中的情况，也避免把清理任务留在后台不等待。

保护范围只包括等待任务收尾、aclose 和会话退出。模型调用、SQL 查询和正常工作流等待仍然可取消。原理可参考 [Python asyncio 取消与 shield](https://docs.python.org/3.12/library/asyncio-task.html#shielding-from-cancellation)和 [AnyIO 清理保护](https://anyio.readthedocs.io/en/stable/cancellation.html#finalization)。

本项目使用协作式取消，不强制终止外部服务的服务器计算，也没有新增数据库 KILL、模型服务端取消或清理硬超时。节点和 SDK 的异步操作需要配合取消，已有查询/客户端超时仍保留。

**资源释放顺序**

```text
收到断开/取消
    ↓
取消发送/工作流任务
    ↓
等待节点的 finally 和工作流退出
    ↓
关闭 QueryService 生成器，等待响应任务收尾
    ↓
FastAPI 请求依赖退出，关闭 meta / dw Session
    ↓
共享客户端与连接池继续供其他请求使用
```

会话依赖仍是普通 yield 依赖，保留 scope="request"。新增的 `_request_session` 是内部 asynccontextmanager，利用 AsyncExitStack 登记会话退出，再用 complete_cleanup 执行关闭。直接交给 Depends 的 get_meta_session / get_dw_session 没有加 asynccontextmanager 装饰器。

**验证与运行方式**

在项目根目录、激活 Python 3.12 虚拟环境后运行：

```powershell
python -m unittest discover -s tests -p test_streaming_errors.py -v
```

2026-11-18 新增 15 项测试通过，全量离线回归 256 项通过，耗时 14.299 秒。覆盖普通失败、纠错上限、结果后失败、编码/关闭同时失败、明确终止事件、重复结果、中间可恢复错误、发送失败、任务取消、AnyIO 取消、第一条事件前断开、清理期间断开、终止结果后断开，以及真实 LangGraph 节点的异步清理。测试还检查没有遗留的请求发送/监听任务。

真实纠错节点测试使用替身模型，确认最多生成两次纠错候选、执行三次校验，最终只输出一条 error 且不执行 SQL。

本机真实 Uvicorn 验证开启正式 lifespan，使用真实 LangGraph 和替身节点/SDK。客户端读到进度后主动关闭 HTTP 流，服务端完成节点的异步清理后才关闭 Session，后续查询成功，共享资源仍可用。该次从客户端关闭到清理/会话关闭的观测约为 0.0301 秒，仅代表本机测试，不是性能承诺。

报告 `.local/streaming_errors_check.json`，脚本 `.local/streaming_errors_check.py`，临时服务已停止。本轮没有调用真实百炼、业务数据库或外部存储。后续第 12 模块 request_id 日志现已完成，说明见 `docs/request-id.md`，当前进度以 API 开发说明为准。
