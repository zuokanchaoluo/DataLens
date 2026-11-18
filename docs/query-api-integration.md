正式 HTTP 接口的真实依赖联调

2026-11-18，正式 `POST /api/query` 使用真实 MySQL、Qdrant、Elasticsearch、Embedding 与阿里云百炼 qwen3.8-max 完成 3/3 查询验证。API 第 1～12 模块的真实调用链已接通。本轮新增可复跑脚本与脚本测试，业务实现及前端没有改动。

**本次经过的调用链**

```text
真实 HTTP 客户端
  → Uvicorn / main.app
  → request_id 中间件
  → 请求模型与 Depends
  → QueryService / QueryStreamingResponse
  → 完整 LangGraph 工作流
  → 真实模型、Embedding、向量库、ES、MySQL
  → SSE progress / result
  → 请求会话退出并归还连接
  → 停服后 lifespan 清理共享资源
```

此前的 API 教学测试使用替身 SDK/节点；本轮执行完整的正式工作流，没有替换模型、节点、仓库或客户端。现有 CLI 联调直接调用 graph，本轮则通过真实 TCP/HTTP 请求调用正式路由，两种验证的入口不同。

**服务与知识预检查**

| 项目 | 本次状态 |
| --- | --- |
| MySQL | 8.0.46，meta/dw 可读 |
| 元数据 | 5 张表、24 个字段、2 项指标 |
| Qdrant | 1.16.3；字段集合 98 个点，指标集合 8 个点 |
| ES | yellow；value_index 75 条，实际取值检索通过 |
| Embedding | 本机健康端点 HTTP 200，真实召回执行通过 |
| 模型 | qwen3.8-max；目的地 dashscope.aliyuncs.com |

本轮没有全量重建知识库，也没有修改分片或副本配置。

真实模型执行前，自动审批未接受交接记录中的旧授权，要求当前对话明确确认外发内容及目的地。用户随后明确允许本教学项目的查询问题、表/字段说明、指标定义、少量字段示例及 SQL 校验上下文发送到已配置的百炼模型，再执行真实查询。本次授权已完成，不是绕过审批。

**真实结果**

| 用例 | 结果 | HTTP 查询耗时 | 首条 SSE |
| --- | --- | --- | --- |
| 华北销售总额 | 41099.50 | 30.686 秒 | 0.718 秒 |
| 按大区统计销售总额 | 5 行，与基准逐行一致 | 34.475 秒 | 0.005 秒 |
| 全部订单 AOV | 2427.473913043478，约 2427.47 | 112.262 秒 | 0.004 秒 |

分组结果为华南 70202.00、华东 107373.00、华北 41099.50、西南 31528.00、华中 28957.00。

AOV 本次的主要等待发生在召回和筛选上下文阶段；进度约在 70.263 秒进入合并、105.444 秒进入额外上下文、105.448 秒进入 SQL 生成，112.262 秒完成。上述时间是本机这次运行的观测值，不是性能承诺。

**如何证明查询结果正确**

新脚本复用已有 CLI 中独立编写的三条基准 SQL，先执行只读校验和基准查询，再发 HTTP 问题。将 SSE result 的 data 映射为行数据，使用现有 results_match 比较。

比较器针对这三项金额用例，允许列别名和行顺序变化，数值允许 0.01 的绝对误差，拒绝缺行、缺列和截断。例如模型把华北结果列命名为 total_sales，而基准为 total_amount，结果仍能正确核对；不会要求模型输出与手写 SQL 文本完全相同。

三项本次均首次 SQL 校验通过，没有执行模型纠错。因此本轮证明的是这三项成功业务路径；纠错与上限仍主要由已有离线测试覆盖。

**除了结果，还验证了什么**

- 三项均为 HTTP 200，Content-Type 为 text/event-stream，带 no-cache。
- 每项收到 21 条合法 SSE 事件，最终只有一条 result，没有重复终止事件。
- 每项 X-Request-ID 为不同的 32 位标识，节点日志均与本请求响应头一致，包含真实生成与执行节点。
- 每项响应之后，等待 meta/dw 连接池借出连接数归零，再检查下一项。
- 停服之前两池借出连接数都为 0，Uvicorn 临时服务正常结束，五个管理器的资源引用均已清空。
- 专用报告和日志已检查不包含配置中的 API Key 或数据库密码。

“连接已归还池”是本轮实际观察的资源指标；会话退出时机与异常/取消分支另有前期 API 测试覆盖。

**脚本如何运行**

新增 `app/scripts/check_query_api.py`。在项目根目录、已激活 Python 3.12 虚拟环境、外部服务和知识索引已准备好的情况下：

```powershell
if (-not $env:OPENAI_API_KEY) {
    $env:OPENAI_API_KEY = [Environment]::GetEnvironmentVariable('OPENAI_API_KEY', 'User')
}
python -m app.scripts.check_query_api
```

脚本自己启动仅监听 127.0.0.1 随机端口的临时 Uvicorn，不需要提前启动 8000 端口，也不使用 CLI 的 graph.create_context。客户端和连接池由正式 lifespan 管理，基准 SQL 使用同一应用资源中的独立会话。

仅复跑一项：

```powershell
python -m app.scripts.check_query_api --case region-total
```

默认每次 HTTP 查询总等待 180 秒，可通过 --timeout 调整；这不包含查询之前的只读基准计算。--case 可重复指定，--report 指定报告位置。--help 不加载模型或读取实际密钥。

验证进程使用专用脱敏日志，过滤配置中的凭据，减少控制台元数据输出；业务日志配置文件保持原有设置。发生失败时先看报告状态与专用日志。报告含业务结果，统一保存在 Git 忽略的本机目录。

**本机产物与测试**

| 路径 | 内容 |
| --- | --- |
| `.local/query_api_e2e_report.json` | 三项真实 HTTP 状态、事件时间、结果、基准与检查结果 |
| `.local/query_api_e2e_report.md` | 本次真实联调摘要 |
| `.local/query_api_e2e_report.log` | 本次带 request_id 的脱敏节点日志 |
| `.local/query_api_preflight_report.json` | 模型执行前的只读预检查，明确标记无模型调用 |

这些文件保存在已被 Git 忽略的 .local 目录。它们与前期 `.local/*_check.json` 替身报告、旧 CLI 的 e2e_report.json 相互独立。

新增 `tests/test_query_api_check.py`，4 项离线测试验证联调脚本不会把 HTTP 200 中的 SSE error 当作成功，能识别重复/错序终止、未完成帧、无效 request_id，并处理中文 UTF-8 拆包。全量离线回归 273 项通过，耗时 23.417 秒。

随后在浏览器通过 Vite 代理提交了华北销售总额查询，页面出现实时进度，最终金额 41099.5 与当次独立 SQL 一致。验收记录见[交接记录第 8 节](../handoff.md)。
