import asyncio

from langgraph.runtime import Runtime

from app.agent.state import DataAgentState
from app.agent.context import DataAgentContext
from app.core.log import logger


EXECUTION_TIMEOUT_SECONDS = 30


async def run_sql(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer({"type": "progress", "step": "执行SQL", "status": "running"})
    try:
        sql = state.get("validated_sql")
        if (state.get("error") is not None or not isinstance(sql, str)
                or not sql.strip() or state.get("sql") != sql):
            raise ValueError("SQL 未通过校验或与已校验文本不一致，禁止执行")
        repository = runtime.context["dw_mysql_repository"]
        result = await asyncio.wait_for(
            repository.execute_sql(sql), timeout=EXECUTION_TIMEOUT_SECONDS
        )
        logger.info(f"SQL 执行成功，返回 {result['row_count']} 行，截断={result['truncated']}")
        writer({"type": "progress", "step": "执行SQL", "status": "success"})
        writer({"type": "result", "data": result["rows"], "columns": result["columns"],
                "row_count": result["row_count"], "truncated": result["truncated"]})
        return {"sql_result": result, "error": None}
    except Exception:
        writer({"type": "progress", "step": "执行SQL", "status": "error"})
        logger.exception("执行 SQL 失败")
        raise
