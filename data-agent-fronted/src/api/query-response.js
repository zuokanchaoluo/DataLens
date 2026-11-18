export class QueryResponseError extends Error {
  constructor(message, status = null) {
    super(message);
    this.name = "QueryResponseError";
    this.status = status;
  }
}

function fallbackMessage(status) {
  if (status === 422) return "请求参数不合法，请检查提交内容。";
  if (status === 503) return "查询服务尚未就绪，请稍后重试。";
  if (status >= 500) return "服务暂时无法完成请求，请稍后重试。";
  return "请求未能完成，请检查请求后重试。";
}

// HTTP 错误是普通 JSON；只有成功的 SSE 响应才能交给流读取循环。
export async function requireQueryStream(response) {
  const mediaType = (response.headers.get("Content-Type") || "")
    .split(";", 1)[0].trim().toLowerCase();

  if (!response.ok) {
    let message = fallbackMessage(response.status);
    if (mediaType === "application/json") {
      try {
        const payload = await response.json();
        if (payload?.type === "error" && typeof payload.message === "string"
            && payload.message.trim()) {
          message = payload.message.trim();
        }
      } catch {
        // 代理或服务可能返回损坏的 JSON，仍展示可理解的状态说明。
      }
    }
    throw new QueryResponseError(message, response.status);
  }

  if (mediaType !== "text/event-stream") {
    throw new QueryResponseError("服务器返回的响应格式不正确，请稍后重试。");
  }
  if (!response.body) {
    throw new QueryResponseError("服务器未返回查询数据，请稍后重试。");
  }
  return response.body;
}
