前端第 2 小模块：空结果展示

2026-11-18 完成。本模块只修改 App.vue 的列来源、空结果行和对应样式。前端 HTTP 错误提示已在第 1 小模块完成，后端正式 HTTP 真实依赖联调已在此前 3/3 通过。

**为什么没有数据也应该有列头**

后端的 result 事件把列定义和行数据分开返回：

```json
{"type":"result","columns":["地区","销售额"],"data":[],"row_count":0,"truncated":false}
```

columns 描述查询输出的字段及顺序；data 保存本次查到的行。WHERE 条件没有匹配记录时，可以返回零行，但仍知道这次查询有哪些输出列。QueryService 已把这种结果作为成功发送，不需要修改后端。

旧前端使用 `Object.keys(data.data[0] || {})` 推导列名。空数组没有第一行，所以得到空列数组；正常结果也会受第一行对象键顺序影响。现在 result 分支使用后端约定的列数组：

```javascript
messages.value.push({
  role: "assistant",
  type: "table",
  columns: data.columns,
  rows: data.data,
});
```

表头按 msg.columns 遍历；每行也按同一个列数组取 row[col]。因此无论行对象的键顺序如何，列头与单元格都遵循后端列顺序。这里沿用正式 result 契约，不从第一行猜列名。

**空结果如何显示**

tbody 增加一行条件展示：

```vue
<tr v-if="msg.rows.length === 0">
  <td :colspan="Math.max(msg.columns.length, 1)" class="empty-result">
    <span role="status">查询成功，暂无数据</span>
  </td>
</tr>
```

v-if 根据实际行数决定是否显示。空数组时，原来的 v-for 不会产生数据行，所以页面只显示这一条提示；有数据时不显示提示，仍渲染原来的单元格。

colspan 是 HTML 表格的合并列数。两列结果需要让提示横跨两列，而不是只放在第一列。Math.max 保证即使模拟响应的 columns=[]，合并列数也至少是 1；没有列时隐藏空 thead，提示仍可见。

empty-result 样式使提示居中、使用中性色并允许换行。role=status 为这条成功状态提供语义。判断只看 rows.length，不看单元格值：

| data | 本次展示 |
| --- | --- |
| `[]` | 保留列头，显示“查询成功，暂无数据” |
| `[{"销售额":0}]` | 显示一行 0 |
| `[{"销售额":null}]` | 保留一行，单元格沿用原来的空白显示 |
| SSE type=error | 进入已有错误分支，不新增结果表格 |

SQL 聚合没有匹配记录时，有些查询仍可能返回一行 NULL，或者 COUNT 返回一行 0；它们与零行结果不同。本模块不改变这些值的展示策略。

**本次验证**

本机临时 Node 服务托管构建产物并模拟 /api/query；真实浏览器通过 fetch 和已有 SSE 循环展示结果。8 种场景通过：

1. 两列、零行：列头保留，提示跨 2 列。
2. 正常一行：行对象键顺序与 columns 相反，页面仍按“地区、销售额”显示“华北、123.45”。
3. 数值 0：保留数据行，不显示空结果提示。
4. null：保留数据行，不显示空结果提示。
5. columns=[] 且零行：提示仍可见，colspan=1。
6. SSE error：错误文字和请求编号正常，不新增表格。
7. HTTP 503：中文提示和请求编号正常，不新增表格。
8. 连续查询后再次零行：新表格使用新列“月份、订单数”，不沿用旧列。

每项结束后发送按钮都恢复可用，列元数据与模拟服务记录核对通过。临时服务已停止，退出码 0；未访问真实模型或业务数据库。

`npm test` 复跑既有 8 项响应处理测试全部通过（约 129ms）；`npm run build` 通过。本轮没有新增测试依赖或测试文件，空结果界面通过上述浏览器场景验证。后端此前 273 项测试与真实 HTTP 3/3 为历史通过记录，本轮未重跑。

本机产物均在 Git 忽略目录：

| 路径 | 内容 |
| --- | --- |
| `.local/frontend_empty_results_fixture.mjs` | 本机模拟服务脚本，项目根目录下用 node 运行，监听 127.0.0.1:8765 |
| `.local/frontend_empty_results_fixture.json` | 本次模拟请求、列元数据、响应事件与编号 |
| `.local/frontend_empty_results_check.json` | 浏览器 8 种场景的实际检查结果、构建/测试与停服状态 |
| `.local/frontend_empty_results.jpg` | 空结果页面截图 |

在 data-agent-fronted 目录运行 `npm test`、`npm run build` 可复跑现有检查。使用正式后端时，通过原有 `npm run dev` 和 Vite /api 代理访问，不需要修改后端结果格式。

后续截断提示已完成，验证记录见根目录 handoff.md 第 7 节。
