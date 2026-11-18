import asyncio

from langgraph.runtime import Runtime

from app.agent.state import DataAgentState
from app.agent.context import DataAgentContext
from app.core.log import logger
from app.core.sql_validation import SQLValidationError


VALIDATION_TIMEOUT_SECONDS = 10


async def validate_sql(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer({"type": "progress", "step": "校验SQL", "status": "running"})
    try:
        repository = runtime.context["dw_mysql_repository"]
        normalized_sql = await asyncio.wait_for(
            repository.validate_sql(state["sql"]), timeout=VALIDATION_TIMEOUT_SECONDS
        )
        logger.info(f"SQL 校验通过: {normalized_sql}")
        writer({"type": "progress", "step": "校验SQL", "status": "success"})
        return {"sql": normalized_sql, "validated_sql": normalized_sql, "error": None, "sql_result": None}
    except SQLValidationError as error:
        logger.warning(f"SQL 校验未通过: {error}")
        writer({"type": "progress", "step": "校验SQL", "status": "error"})
        return {"validated_sql": None, "error": str(error), "sql_result": None}
    except Exception:
        writer({"type": "progress", "step": "校验SQL", "status": "error"})
        logger.exception("SQL 校验发生服务故障或超时")
        raise
