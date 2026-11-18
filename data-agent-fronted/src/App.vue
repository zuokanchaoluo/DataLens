<template>
  <div class="workspace">
    <aside class="sidebar" aria-label="会话导航">
      <div class="brand">
        <span class="brand-mark" aria-hidden="true"><i></i><i></i><i></i></span>
        <div>
          <div class="brand-name">数析 DataLens</div>
          <div class="brand-caption">经营数据工作台</div>
        </div>
      </div>

      <button class="new-query" type="button" :disabled="loading" @click="newConversation">
        <span aria-hidden="true">＋</span> 新建会话
      </button>

      <div class="side-section">
        <div class="side-label">工作空间</div>
        <div class="side-active" aria-current="page">
          <span class="nav-icon" aria-hidden="true">▦</span> 数据问答
        </div>
      </div>

      <div class="side-section history">
        <div class="side-label">本次会话</div>
        <p v-if="!messages.length" class="history-empty">发送问题后，记录会出现在这里。</p>
        <ul v-else>
          <template v-for="(msg, index) in messages" :key="index">
            <li v-if="msg.role === 'user'">
              <a :href="'#question-' + index" @click.prevent="jumpToQuestion(index)">{{ msg.content }}</a>
            </li>
          </template>
        </ul>
      </div>
    </aside>

    <main class="main-panel">
      <header class="topbar">
        <div class="breadcrumb"><span>工作空间</span><span aria-hidden="true">/</span><strong>数据问答</strong></div>
        <div class="mobile-brand">数析 DataLens</div>
        <button class="mobile-new" type="button" :disabled="loading" @click="newConversation">＋ 新建会话</button>
      </header>

      <div ref="messagesEl" class="messages" aria-label="问答内容">
        <div class="content-width">
          <div v-if="!messages.length" class="welcome">
            <section class="hero" aria-labelledby="welcome-title">
              <div class="hero-art" aria-hidden="true"></div>
              <div class="hero-copy">
                <span class="hero-kicker"><i aria-hidden="true"></i> 经营数据问答</span>
                <h1 id="welcome-title">今天，想了解<br class="hero-break" />哪些数据？</h1>
                <p>用自然语言提问，快速查看销售、订单和区域表现。</p>
              </div>
            </section>

            <section class="suggestions" aria-labelledby="suggestions-title">
              <div class="section-head">
                <h2 id="suggestions-title">试试这些问题</h2>
                <p>点击卡片填入输入框</p>
              </div>
              <div class="suggestion-grid">
                <button v-for="item in suggestions" :key="item.question" class="suggestion" type="button"
                        @click="useSuggestion(item.question)">
                  <span :class="['suggestion-icon', item.tone]" aria-hidden="true">{{ item.icon }}</span>
                  <span class="suggestion-copy">
                    <strong>{{ item.title }}</strong>
                    <span>{{ item.question }}</span>
                  </span>
                  <span class="suggestion-arrow" aria-hidden="true">↗</span>
                </button>
              </div>
            </section>
          </div>

          <div v-else class="conversation">
            <div v-for="(msg, index) in messages" v-show="msg.type !== 'steps' || msg.steps.length"
                 :id="msg.role === 'user' ? 'question-' + index : undefined" :key="index"
                 :class="['message-row', msg.role]">
              <span v-if="msg.role === 'assistant'" class="assistant-mark" aria-hidden="true">▥</span>
              <div :class="['bubble', 'bubble-' + msg.type]">
                <div v-if="msg.type === 'text'" class="question-text">{{ msg.content }}</div>

                <section v-else-if="msg.type === 'steps'" class="progress-card" aria-label="查询进度">
                  <h2>查询进度</h2>
                  <ul class="steps">
                    <li v-for="step in msg.steps" :key="step.text" class="step">
                      <span class="dot" :class="step.status" aria-hidden="true"></span>
                      <span>{{ step.text }}</span>
                      <span class="sr-only">{{ stepLabels[step.status] }}</span>
                    </li>
                  </ul>
                </section>

                <section v-else-if="msg.type === 'table'" class="result-card" aria-label="查询结果">
                  <div class="result-head">
                    <h2>查询结果</h2>
                    <span class="result-count">{{ msg.rows.length }} 行</span>
                  </div>
                  <p v-if="msg.truncated" class="truncation-note" role="status">
                    结果已截断，仅展示前 {{ msg.rowCount }} 行。
                  </p>
                  <div class="table-wrap" tabindex="0" role="region" aria-label="查询结果表格">
                    <table class="result-table" aria-label="查询结果">
                      <thead v-if="msg.columns.length">
                        <tr><th v-for="col in msg.columns" :key="col" scope="col">{{ col }}</th></tr>
                      </thead>
                      <tbody>
                        <tr v-if="!msg.rows.length">
                          <td :colspan="Math.max(msg.columns.length, 1)" class="empty-result">
                            <span role="status">查询成功，暂无数据</span>
                          </td>
                        </tr>
                        <tr v-for="(row, rowIndex) in msg.rows" :key="rowIndex">
                          <td v-for="col in msg.columns" :key="col">{{ row[col] }}</td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </section>

                <div v-else-if="msg.type === 'error'" class="error-text" role="alert">
                  <strong>{{ msg.content }}</strong>
                  <div v-if="msg.httpStatus || msg.requestId" class="error-details">
                    <span v-if="msg.httpStatus">HTTP {{ msg.httpStatus }}</span>
                    <span v-if="msg.requestId">请求编号 <code>{{ msg.requestId }}</code></span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <footer class="composer">
        <div class="content-width">
          <form class="input-box" @submit.prevent="sendQuestion">
            <span class="composer-icon" aria-hidden="true">⌕</span>
            <label for="question" class="sr-only">你的问题</label>
            <input id="question" ref="questionInput" v-model="question" autocomplete="off"
                   placeholder="输入你想查询的数据…" />
            <button type="submit" :disabled="loading || !question.trim()">
              {{ loading ? "查询中…" : "发送" }} <span v-if="!loading" aria-hidden="true">↑</span>
            </button>
          </form>
          <p class="input-help" aria-live="polite">{{ loading ? "正在查询，请稍候" : "按 Enter 发送问题" }}</p>
        </div>
      </footer>
    </main>
  </div>
</template>

<script setup>
import {nextTick, ref} from "vue";
import {QueryResponseError, requireQueryStream} from "./api/query-response.js";

const API_URL = "/api/query";

const question = ref("");
const loading = ref(false);
const messages = ref([]);
const messagesEl = ref(null);
const questionInput = ref(null);

const suggestions = [
  {title: "销售总额", question: "统计华北地区的销售总额", tone: "blue", icon: "▥"},
  {title: "区域对比", question: "按大区统计销售总额", tone: "violet", icon: "◫"},
  {title: "客单价", question: "统计所有订单的平均订单金额（AOV）", tone: "teal", icon: "↗"},
];

const stepLabels = {running: "进行中", success: "已完成", error: "未完成"};

function useSuggestion(value) {
  question.value = value;
  questionInput.value?.focus();
}

function newConversation() {
  if (loading.value) return;
  messages.value = [];
  useSuggestion("");
}

function jumpToQuestion(index) {
  document.getElementById("question-" + index)?.scrollIntoView({block: "start"});
}

function scrollToBottom() {
  const el = messagesEl.value;
  if (!el) return;
  el.scrollTop = el.scrollHeight;
}

async function sendQuestion() {
  const q = question.value.trim();
  if (!q || loading.value) return;
  question.value = "";
  loading.value = true;

  messages.value.push({role: "user", type: "text", content: q});

  // steps 容器
  const stepIndex =
      messages.value.push({
        role: "assistant",
        type: "steps",
        steps: [],
      }) - 1;

  await nextTick();
  scrollToBottom();

  let requestId = null;
  try {
    const response = await fetch(API_URL, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({query: q}),
    });

    requestId = response.headers.get("X-Request-ID");
    const body = await requireQueryStream(response);

    const reader = body.getReader();
    const decoder = new TextDecoder("utf-8");
    let buffer = "";

    while (true) {
      const {value, done} = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, {stream: true});
      const events = buffer.split("\n\n");
      buffer = events.pop();

      for (const evt of events) {
        const line = evt.trim();
        if (!line.startsWith("data:")) continue;

        let data;
        try {
          data = JSON.parse(line.replace(/^data:\s*/, ""));
        } catch {
          continue;
        }

        const steps = messages.value[stepIndex].steps;

        // ✅ progress：完全按后端状态渲染
        if (data.type === "progress") {
          let step = steps.find((s) => s.text === data.step);

          if (!step) {
            step = {
              text: data.step,
              status: data.status,
            };
            steps.push(step);
          } else {
            step.status = data.status;
          }
        }

        // ✅ 表格结果
        else if (data.type === "result" && Array.isArray(data.data)) {
          messages.value.push({
            role: "assistant",
            type: "table",
            columns: data.columns,
            rows: data.data,
            rowCount: data.row_count,
            truncated: data.truncated === true,
          });
        }

        // ✅ 错误
        else if (data.type === "error") {
          messages.value.push({
            role: "assistant",
            type: "error",
            content: data.message || "发生错误",
            requestId,
          });
        }

        await nextTick();
        scrollToBottom();
      }
    }
  } catch (e) {
    messages.value.push({
      role: "assistant",
      type: "error",
      content: e instanceof QueryResponseError
          ? e.message : "请求未能完成，请检查网络连接后重试。",
      httpStatus: e instanceof QueryResponseError ? e.status : null,
      requestId,
    });
  } finally {
    loading.value = false;
    await nextTick();
    scrollToBottom();
  }
}
</script>
