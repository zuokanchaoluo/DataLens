API 第 9 模块：应用生命周期集成

第 8 模块已经接通路由、依赖、仓库与 QueryService。本模块让共享客户端和数据库连接池随正式应用启动和关闭，覆盖正常路径与部分启动失败的清理。

**改动位置**

| 文件 | 改动 |
| --- | --- |
| `app/core/lifespan.py` | 新增正式应用生命周期，管理五个共享管理器 |
| `main.py` | FastAPI 构造时传入 lifespan |
| `app/clients/*_client_manager.py` | 四类管理器补齐重复初始化检查、重复关闭和引用清理 |
| `tests/test_app_lifespan.py` | 新增 12 项生命周期验证，底层 SDK 使用替身 |

**应用启动、请求和关闭分别发生什么**

```text
启动应用
  → 检查没有已经初始化的共享资源
  → 初始化 Embedding、Qdrant、ES、meta 连接池、dw 连接池
  → 执行到 lifespan 的 yield，开始接收请求

每次请求
  → Depends 创建独立的 meta / dw Session
  → 创建仓库和 QueryService，引用共享客户端
  → 执行工作流并发送 SSE
  → 响应结束，关闭本次请求的 Session

关闭应用
  → 逆序关闭 dw、meta、ES、Qdrant、Embedding
  → 清空管理器保存的资源引用
```

生命周期中的 yield 把控制权交给 FastAPI，应用在这一段时间处理许多请求，不是每次请求都重新进入 lifespan。main.py 注册方式是：

```python
app = FastAPI(
    # title、description、version 等原有配置保留。
    lifespan=lifespan,
)
```

`@asynccontextmanager` 将异步生成器包装成异步上下文管理器。lifespan 的 yield 前准备资源，退出上下文时执行清理；这与提供给 Depends 的普通 yield 依赖属于不同入口。FastAPI 官方推荐使用 [lifespan 参数](https://fastapi.tiangolo.com/advanced/events/)管理应用级启动和关闭。

**为什么使用 AsyncExitStack**

只在所有 init 之后写 yield，再在下面依次 close，会遗漏“尚未执行到 yield 就失败”的情况。例如 Embedding、Qdrant 已经创建，但 ES 初始化抛错，前两个资源也需要关闭。

本模块使用：

```python
async with AsyncExitStack() as stack:
    for manager in managers:
        stack.push_async_callback(manager.close)
        manager.init()
    yield
```

这段省略了管理器列表，完整代码见 lifespan.py。push_async_callback 登记未来要 await 的清理函数，此时不执行 close，也不写 `manager.close()`。

stack 会按登记的相反顺序清理。正常运行结束会清理；init 抛错也会清理已经登记的资源。某个 close 失败时，stack 仍尝试剩余清理动作，清理错误不会被静默吞掉。行为依据 [Python AsyncExitStack](https://docs.python.org/3.12/library/contextlib.html#contextlib.AsyncExitStack)。

**为什么先登记 close，再执行 init**

一个 init 内部也可能分多步：

```python
self.engine = create_async_engine(...)
self.session_factory = async_sessionmaker(self.engine, ...)
```

如果第一步成功、第二步失败，在 init 之后才登记 close，就会遗漏已经创建的 engine。先登记让失败的管理器自身也有机会清理；尚未开始初始化的后续管理器不需要登记。

这要求 close 允许资源尚未创建，所以本模块同时补齐了管理器行为。

**管理器的三个约定**

| 情况 | 行为 |
| --- | --- |
| 已经初始化，再次调用 init | 明确报错，保留原资源 |
| 资源尚未初始化，或已经关闭，再调用 close | 允许，直接结束 |
| 关闭一个已创建资源 | 先清空管理器引用，再调用底层关闭方法 |

MySQL 会清空 engine 和 session_factory，随后 dispose 连接池；ES、Qdrant 会清空 client 再关闭客户端。Embedding 同时持有同步 InferenceClient 和 AsyncInferenceClient，会关闭两者；即使异步关闭抛错，finally 仍尝试关闭同步客户端。

清空引用让依赖函数能够识别资源已经不可用，也让后续一个新的、顺序启动的生命周期可以创建新对象。关闭失败仍会向上传播，不能把“尝试过关闭”理解为底层资源一定释放成功。

lifespan 开始时会先检查所有管理器是否已有资源。若有，会拒绝启动，避免先登记 close 后发现重复 init，反而关闭原调用者的资源。这一实现使用进程内全局管理器，因此同一进程只允许一个活动的正式生命周期。多个 worker 进程各有自己的资源。

**如何与第 8 模块配合**

共享客户端和连接池在应用启动时准备，Depends 只取得它们，不调用 init 或 close。每个请求仍然有独立的仓库、服务、State、Context 和数据库 Session，Session 覆盖整个 SSE 响应；请求结束后只关闭 Session。

单个工作流失败时，QueryService 可以输出通用 SSE error，当前请求的会话被清理；应用共享客户端仍然保留，后续请求继续使用。API 请求不调用面向 CLI 的 graph.create_context()。

**启动方法和验证边界**

在项目根目录、激活 Python 3.12 虚拟环境后：

```powershell
if (-not $env:OPENAI_API_KEY) {
    $env:OPENAI_API_KEY = [Environment]::GetEnvironmentVariable('OPENAI_API_KEY', 'User')
}
python -m uvicorn main:app --reload --port 8000
```

现在 Uvicorn 默认会触发正式 lifespan，正常初始化成功后再接收请求；关闭服务器时触发清理。不要在正式运行时设置 `--lifespan off`。第 8 模块结束时“共享资源总是 None，因此默认查询 503”的状态已经结束；503 保护仍用于初始化缺失等情况。

可以访问 `http://127.0.0.1:8000/docs` 查看接口。真正查询还需要配置的外部服务、元数据、向量和取值索引可用，并会调用配置的百炼模型。

本模块没有新增主动的全依赖健康检查。部分 SDK 采用延迟连接，创建客户端或连接池成功不能证明远端可访问，也不能证明业务知识已准备好。本轮只验证生命周期与接口资源管理，没有新做真实模型/数据库的正式 API 全链路联调。

**测试与结果**

```powershell
python -m unittest discover -s tests -p test_app_lifespan.py -v
```

2026-11-18 新增 12 项测试通过：正式应用启动/关闭顺序、五个启动阶段故障、MySQL 会话工厂创建失败后的引擎清理、关闭失败时继续其他清理、Embedding 双客户端清理、重叠生命周期拒绝、已有资源保留、重复初始化/关闭、关闭后重启、异常/取消时清理、部分失败后重试，以及并发查询的共享与隔离。

测试保留真实管理器、依赖函数、仓库、QueryService 和正式应用，替换底层 SDK 构造器与工作流，不调用真实百炼、业务数据库或外部存储服务。全量离线回归 228 项通过，耗时 19.594 秒。

另外启动本机真实 Uvicorn，开启正式 lifespan，验证启动准备五个管理器、SSE 到达时会话开启、请求结束关闭会话且保留共享客户端、并发隔离、故障后的请求继续成功，以及停机时关闭资源并清空引用。报告 `.local/app_lifespan_check.json`，验证脚本 `.local/app_lifespan_http_check.py`，临时服务已停止。

第 9 模块完成后按要求停止；后续第 10 模块的流式响应开始前异常处理现已完成，学习说明见 `docs/http-errors.md`。当前进度以 API 开发说明为准，每个模块结束后仍需等待用户说“继续”。
