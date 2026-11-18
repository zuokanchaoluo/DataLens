API 第 5 模块：中间件基础

本轮实现独立教学应用 `app/scripts/middleware_demo.py`。中间件统一处理进入应用的 HTTP 请求，并在取得响应对象后添加响应头。本轮不连接数据库或模型服务。

**启动与观察**

在项目根目录、激活虚拟环境后运行：

```powershell
python -m uvicorn app.scripts.middleware_demo:app --port 8003
```

另开终端，用 `-i` 查看响应头：

```powershell
curl.exe --noproxy 127.0.0.1 -i "http://127.0.0.1:8003/demo/middleware?label=A"
```

响应体类似：

```json
{"label":"A","steps":["middleware.before","handler"]}
```

响应头中包含：

```text
x-demo-trace: middleware.before -> handler -> middleware.after
x-process-time: 0.012345
```

耗时数值会随运行变化，单位为秒。它由 perf_counter 的时间差得到。

服务端打印顺序为：中间件前 → 接口 → 中间件后。响应体中的 steps 是接口生成响应时的快照；middleware.after 是之后由中间件加入的，所以在响应头中的完整轨迹里出现。

label 只是用于观察请求隔离的演示参数，不是身份标识或 request_id。

**call_next 的作用**

核心结构：

```python
@app.middleware("http")
async def observe_request(request: Request, call_next):
    # 进入路由处理之前
    response = await call_next(request)
    # 已取得响应对象，可以添加响应头
    return response
```

call_next 把请求交给应用后续处理，不是我们手动创建的路径函数。框架负责寻找匹配的路由、解析参数并调用接口。得到响应后，中间件可以添加响应头，再把响应交回框架。

中间件也会处理没有匹配路由的请求；示例中访问不存在的路径会得到 404，并带有中间件添加的响应头。

如果 call_next 抛出未处理异常，它后面的普通语句不会执行。异常的统一处理在后续模块完成；本轮不把所有异常转换成成功响应。

**request.state 的作用**

中间件为每次请求建立一个独立的 steps 列表，接口通过同一次请求的 request.state 取得它，并保存本次请求的 label。接口在一次异步等待后读取 label，并发请求仍应各自得到自己的值。

| 保存位置 | 生命周期和用途 |
| --- | --- |
| app.state | 应用级共享客户端等资源 |
| request.state | 当前 HTTP 请求的数据，例如本例轨迹与标签 |
| DataAgentState | 当前问数工作流的问题、SQL 和结果 |

不同请求的数据不放进同一个全局可变列表，否则可能出现轨迹累积、标签串到其他请求等问题。本例的 request.state.steps 在中间件进入时创建。

**结合前面学过的 SSE**

观察流式接口：

```powershell
curl.exe --noproxy 127.0.0.1 -i -N http://127.0.0.1:8003/demo/middleware/stream
```

首先看到响应头和 running 事件，约一秒后看到 result 事件。中间件的“响应已就绪”表示已取得响应对象，流式生成器此时可能仍在等待、继续产生消息。

因此，本例 X-Process-Time 表示从进入中间件到取得响应对象并添加响应头之间的耗时，不代表 SSE 全部发送完毕，更不是整个问数任务的总耗时。中间件不会先收集整个流来计时，这样保留了实时发送效果。

**与生命周期、后续 request_id 的联系**

生命周期围绕应用的一次启动和关闭运行；中间件则处理进入这个应用的每次 HTTP 请求。本文件保留第 5 模块的 call_next 教学示例；第 12 模块已经使用纯 ASGI 中间件和 ContextVar 完成正式 request_id 日志，保持标识到 SSE 与会话清理结束，说明见 `docs/request-id.md`。

**验证**

`tests/test_middleware.py` 验证处理顺序、响应头、404 路由、并发请求隔离，以及流尚未结束时中间件后置步骤已执行。本例的受控流测试不依赖实际等待时间来断言顺序。

本模块新增 4 项测试。真实 Uvicorn 验证中，并发标签 A/B 分别正确返回，各自轨迹没有累积；404 响应也带有中间件头。流式示例的 X-Process-Time 约为 0.000607 秒，首条事件约 0.002 秒到达，最后事件约 1.023 秒到达，证实响应准备计时不包含整个流的等待过程。记录位于 `.local/middleware_check.json`，临时服务器已停止。数值是本次实测，复跑时会变化。

参考：[FastAPI 中间件](https://fastapi.tiangolo.com/tutorial/middleware/)。
