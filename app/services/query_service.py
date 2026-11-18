import asyncio
from collections.abc import AsyncIterator

from langchain_huggingface import HuggingFaceEndpointEmbeddings

from app.agent.context import DataAgentContext
from app.agent.graph import graph
from app.agent.state import DataAgentState
from app.api.sse import encode_sse
from app.core.async_cleanup import complete_cleanup
from app.core.log import logger
from app.repositories.es.value_es_repository import ValueEsRepository
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository
from app.repositories.qdrant.column_qdrant_repository import ColumnQdrantRepository
from app.repositories.qdrant.metric_qdrant_repository import MetricQdrantRepository


QUERY_ERROR_MESSAGE = "本次查询未能完成，请稍后重试。"
LEGACY_PROGRESS_STEPS = {"抽取关键词", "召回字段信息"}


class QueryService:
    """使用外部提供的依赖运行问数工作流，逐条产出 SSE 文本。"""

    def __init__(
        self,
        embedding_client: HuggingFaceEndpointEmbeddings,
        column_qdrant_repository: ColumnQdrantRepository,
        value_es_repository: ValueEsRepository,
        metric_qdrant_repository: MetricQdrantRepository,
        meta_mysql_repository: MetaMySQLRepository,
        dw_mysql_repository: DWMySQLRepository,
    ):
        self.embedding_client = embedding_client
        self.column_qdrant_repository = column_qdrant_repository
        self.value_es_repository = value_es_repository
        self.metric_qdrant_repository = metric_qdrant_repository
        self.meta_mysql_repository = meta_mysql_repository
        self.dw_mysql_repository = dw_mysql_repository

    async def query(self, query: str) -> AsyncIterator[str]:
        context = DataAgentContext(
            embedding_client=self.embedding_client,
            column_qdrant_repository=self.column_qdrant_repository,
            value_es_repository=self.value_es_repository,
            metric_qdrant_repository=self.metric_qdrant_repository,
            meta_mysql_repository=self.meta_mysql_repository,
            dw_mysql_repository=self.dw_mysql_repository,
        )
        state = DataAgentState(query=query)
        final_state = None
        result_event = None
        stream = None
        failed = False
        interrupted = False

        try:
            # custom 接收节点事件；values 只用于检查终态，不直接外发。
            stream = graph.astream(
                input=state, context=context,
                stream_mode=["custom", "values"], version="v1",
            )
            async for mode, chunk in stream:
                if mode == "values":
                    final_state = chunk
                    continue
                if mode != "custom":
                    continue
                if isinstance(chunk, str):
                    # 仅适配现存的两个旧节点，不把任意调试文本当进度。
                    if chunk not in LEGACY_PROGRESS_STEPS:
                        continue
                    chunk = {"type": "progress", "step": chunk, "status": "running"}
                if not isinstance(chunk, dict):
                    continue
                if chunk.get("type") == "progress":
                    yield encode_sse({key: chunk[key] for key in ("type", "step", "status")})
                elif chunk.get("type") == "result" and result_event is None:
                    # 暂存已有结果事件，确认工作流成功结束后只发送一次。
                    result_event = {key: chunk[key] for key in
                                    ("type", "data", "columns", "row_count", "truncated")}
                elif chunk.get("type") == "error":
                    failed = True
                    break
        except (asyncio.CancelledError, GeneratorExit):
            interrupted = True
            raise
        except Exception as error:
            logger.error("问数工作流执行失败，异常类型：{}", type(error).__name__)
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                # 节点清理抛错也不能把正在取消的请求重新变成业务事件流。
                interrupted = True
                raise asyncio.CancelledError() from error
            failed = True
        finally:
            if stream is not None:
                try:
                    await complete_cleanup(stream.aclose())
                except Exception as error:
                    logger.error("关闭问数工作流失败，异常类型：{}", type(error).__name__)
                    if not interrupted:
                        failed = True

        # 终止事件放在异常捕获和工作流关闭之后，最多只产出一条。
        frame = None
        if not failed:
            if (final_state is None or final_state.get("error") is not None
                    or final_state.get("sql_result") is None or result_event is None):
                logger.warning("问数工作流结束，但未产生完整的成功结果")
            else:
                try:
                    frame = encode_sse(result_event)
                except Exception as error:
                    logger.error("编码问数结果失败，异常类型：{}", type(error).__name__)
        if frame is None:
            frame = encode_sse({"type": "error", "message": QUERY_ERROR_MESSAGE})
        yield frame
