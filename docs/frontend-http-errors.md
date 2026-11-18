前端第 1 小模块：HTTP 错误提示

2026-11-18 完成。前端现在先检查 HTTP 状态和响应类型，再决定读取 JSON 错误或 SSE 流。后端 API 第 1～12 模块及真实 HTTP 3/3 验证已在此前完成，本模块使用本机模拟接口，没有调用模型或业务数据库。

**为什么原来页面可能没有错误提示**

`fetch()` 收到 422、503、500 这样的 HTTP 响应时，通常仍正常返回 Response；不会因为状态码失败而自动进入 catch。网络断开等传输错误才会使 fetch 本身拒绝。

旧 App.vue 只检查 response.body 是否存在，然后直接用 getReader 读取。JSON 错误体同样可以有 body，但不是 `data: ...\n\n`，因此可能被 SSE 循环跳过：请求结束，页面却没有显示错误。

后端两类错误需要分别处理：

| 场景 | HTTP 状态及类型 | 前端处理 |
| --- | --- | --- |
| 参数校验失败 | 422，application/json | 读取 ErrorSchema.message |
| 服务未就绪 | 503，application/json | 读取 ErrorSchema.message |
| 响应开始前的内部异常 | 500，application/json | 读取 ErrorSchema.message |
| 代理返回 HTML 或损坏 JSON | 非成功状态 | 使用状态对应的中文兜底，不显示原始页面 |
| 响应成功但类型不符 | 2xx，非 text/event-stream | 提示响应格式不正确 |
| 流内业务失败 | 200，text/event-stream，type=error | 由原 SSE 循环展示 message |
| 网络连接失败 | 可能没有 HTTP 响应 | 显示中文网络提示，不虚构请求编号 |

`response.ok` 表示状态码在 200～299。检查它能区分 HTTP 是否成功，但不能判断 SSE 中的业务是否成功；HTTP 200 后仍可能收到 type=error。

**代码如何协作**

新增 `data-agent-fronted/src/api/query-response.js`，专门处理响应进入 SSE 循环前的检查，不负责 Vue 界面：

1. 提取 Content-Type 的媒体类型，忽略 charset 等参数并统一大小写。
2. 若 response.ok 为 false，先准备按状态选择的中文消息。仅在 application/json 中尝试读取 ErrorSchema，要求 type=error 且 message 为非空字符串；解析失败或结构不符继续使用兜底消息。
3. 抛出 QueryResponseError，携带 message 和 status。这里的 throw 让 App.vue 统一进入 catch，而不是继续读取 SSE。
4. 对成功响应，要求 text/event-stream 且 body 存在，再返回 body。不会预先调用 response.text()/json()，保证正常 SSE 可以增量消费。

App.vue 的主要顺序是：

```javascript
const response = await fetch(API_URL, options);
requestId = response.headers.get("X-Request-ID");
const body = await requireQueryStream(response);
const reader = body.getReader();
// 后面仍是已有的 UTF-8 增量解码与 SSE 拆分循环。
```

请求编号在调用检查函数前读取，因此即使检查函数抛错，catch 仍能显示响应头中的编号。requestId 是 sendQuestion 内部的局部变量，每次发送都从 null 开始；网络失败不会沿用上一条请求的编号。

catch 对已知 QueryResponseError 使用其中文 message 和 HTTP status；其他异常显示通用网络提示。SSE type=error 也附带本次请求编号。错误使用 Vue 文本插值，不把服务返回内容作为 HTML 执行；参数 details 和原始代理页面不直接展示。

错误气泡示例：

```text
查询服务尚未就绪，请稍后重试。
HTTP 503
请求编号：服务器本次生成的 32 位标识
```

用户可以选择、复制编号，在后端日志中搜索对应 request_id。没有响应头时隐藏该行。finally 继续将 loading 设为 false，所以失败后发送按钮仍然可用。

页面还隐藏了 steps 为空的进度行：首次进度到达后才显示；HTTP 错误在任何进度之前发生时，不再留下空白气泡。正常进度、结果表格与 SSE 缓冲处理沿用原代码。

**实际验证**

新增 `data-agent-fronted/tests/query-response.test.js`，使用 Node 内置测试工具，不增加测试依赖。8 项通过，覆盖 422/503/500、HTML、损坏 JSON、错误 JSON 结构、其他失败状态、成功响应类型不符、缺少 body，以及中文逐字节拆包的 SSE 仍保持可读。`npm run build` 通过。

本机临时 Node 服务托管构建产物，模拟 `/api/query`。浏览器通过真实 fetch 验证以下 8 种场景：422、503、500、HTML 502、HTTP 200 JSON、SSE error、网络断开、正常 SSE 表格。均显示预期内容，每次发送完成后按钮恢复可用；错误编号与模拟服务记录一致，网络失败不显示上一条编号。随后复核 503 页面，确认空白进度气泡已消除。临时服务已停止。

本机记录位于 `.local/frontend_http_errors_fixture.json`、`.local/frontend_http_errors_check.json`；模拟服务脚本为 `.local/frontend_http_errors_fixture.mjs`。均在 Git 忽略目录。它们验证前端分支和真实浏览器交互，不属于真实模型或真实后端全链路联调。

在前端目录运行：

```powershell
cd data-agent-fronted
npm ci
npm test
npm run build
npm run dev
```

依赖按已有 package-lock 安装，本轮没有修改锁文件或新增依赖。开发时仍使用原有 Vite `/api` → localhost:8000 代理，正式后端需要单独启动。本轮只重跑前端测试与构建，后端最近一次全量离线结果仍为此前的 273 项通过。

后续零行展示见 [前端空结果展示](frontend-empty-results.md)。当前进度见根目录 handoff.md。
