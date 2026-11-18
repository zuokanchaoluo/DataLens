import yaml
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from app.agent.state import DataAgentState
from app.agent.context import DataAgentContext
from app.agent.llm import llm
from app.core.log import logger
from app.core.sql_output import clean_sql_output
from app.prompt.prompt_loader import load_prompt


MAX_SQL_CORRECTIONS = 2


async def correct_sql(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer({"type": "progress", "step": "校正SQL", "status": "running"})
    try:
        error = state["error"]
        if not isinstance(error, str) or not error.strip():
            raise ValueError("SQL 校正需要具体的校验错误信息")
        attempts = state.get("sql_correction_attempts", 0)
        if type(attempts) is not int or attempts < 0:
            raise ValueError("SQL 校正次数必须是非负整数")
        if attempts >= MAX_SQL_CORRECTIONS:
            # 保留最后一次校验错误，并结束流程，不再调用模型。
            message = f"SQL 校正次数已达上限（{MAX_SQL_CORRECTIONS} 次），最后一次校验错误：{error}"
            logger.warning(message)
            writer({"type": "progress", "step": "校正SQL", "status": "error"})
            return {"validated_sql": None, "error": message, "sql_result": None}

        query = state["query"].strip()
        sql = state["sql"].strip()
        if not query:
            raise ValueError("用户问题不能为空")
        if not sql:
            raise ValueError("待校正的 SQL 不能为空")
        if not state["table_infos"] or any(not table["columns"] for table in state["table_infos"]):
            raise ValueError("没有完整可用的候选表字段，无法校正 SQL")

        prompt = PromptTemplate(
            template=load_prompt("correct_sql"),
            input_variables=["query", "table_infos", "metric_infos", "date_info", "db_info", "sql", "error"],
        )
        chain = prompt | llm | StrOutputParser()
        prompt_values = {"query": query, "sql": sql, "error": error}
        for key in ("table_infos", "metric_infos", "date_info", "db_info"):
            prompt_values[key] = yaml.safe_dump(state[key], allow_unicode=True, sort_keys=False)
        corrected_sql = clean_sql_output(await chain.ainvoke(prompt_values))

        logger.info(f"第 {attempts + 1} 次校正的候选 SQL（尚未校验）: {corrected_sql}")
        writer({"type": "progress", "step": "校正SQL", "status": "success"})
        # 新候选清除旧校验标记；error=None 仅表示已生成候选，后续必须重新校验。
        return {"sql": corrected_sql, "validated_sql": None, "error": None,
                "sql_correction_attempts": attempts + 1, "sql_result": None}
    except Exception:
        writer({"type": "progress", "step": "校正SQL", "status": "error"})
        logger.exception("校正 SQL 失败")
        raise
