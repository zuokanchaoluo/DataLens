import yaml
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from app.agent.llm import llm
from app.agent.state import DataAgentState
from app.agent.context import DataAgentContext
from app.core.log import logger
from app.core.sql_output import clean_sql_output
from app.prompt.prompt_loader import load_prompt


async def generate_sql(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer({"type": "progress", "step": "生成SQL", "status": "running"})

    try:
        query = state["query"].strip()
        table_infos = state["table_infos"]
        if not query:
            raise ValueError("用户问题不能为空")
        if not table_infos or any(not table["columns"] for table in table_infos):
            raise ValueError("没有完整可用的候选表字段，请检查召回和表过滤结果")

        # 两路过滤并行执行，调用模型前核对保留指标的字段是否仍在候选表中。
        available_columns = {
            f"{table['name']}.{column['name']}"
            for table in table_infos for column in table["columns"]
        }
        for metric in state["metric_infos"]:
            missing = set(metric["relevant_columns"]) - available_columns
            if missing:
                raise ValueError(
                    f"指标 {metric['name']} 依赖的字段不在筛选后的表信息中: {sorted(missing)}；"
                    "请检查表和指标过滤结果"
                )

        prompt = PromptTemplate(
            template=load_prompt("generate_sql"),
            input_variables=["query", "table_infos", "metric_infos", "date_info", "db_info"],
        )
        chain = prompt | llm | StrOutputParser()
        prompt_values = {"query": query}
        for key in ("table_infos", "metric_infos", "date_info", "db_info"):
            prompt_values[key] = yaml.safe_dump(state[key], allow_unicode=True, sort_keys=False)
        result = await chain.ainvoke(prompt_values)
        sql = clean_sql_output(result)

        logger.info(f"生成的候选 SQL（尚未校验）: {sql}")
        writer({"type": "progress", "step": "生成SQL", "status": "success"})
        return {"sql": sql, "validated_sql": None, "error": None,
                "sql_correction_attempts": 0, "sql_result": None}
    except Exception:
        writer({"type": "progress", "step": "生成SQL", "status": "error"})
        logger.exception("生成 SQL 失败")
        raise
