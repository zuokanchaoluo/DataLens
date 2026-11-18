from copy import deepcopy

import yaml
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from app.agent.llm import llm
from app.agent.state import DataAgentState, TableInfoState
from app.agent.context import DataAgentContext
from app.core.log import logger
from app.prompt.prompt_loader import load_prompt


async def filter_table(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer({"type": "progress", "step": "过滤表信息", "status": "running"})

    try:
        table_infos = state["table_infos"]
        if not table_infos:
            logger.info("没有候选表，跳过模型筛选")
            writer({"type": "progress", "step": "过滤表信息", "status": "success"})
            return {"table_infos": []}

        prompt = PromptTemplate(
            template=load_prompt("filter_table_info"),
            input_variables=["query", "table_infos"],
        )
        chain = prompt | llm | JsonOutputParser()
        selection = await chain.ainvoke({
            "query": state["query"],
            "table_infos": yaml.safe_dump(table_infos, allow_unicode=True, sort_keys=False),
        })
        if not isinstance(selection, dict):
            raise ValueError("表筛选结果必须是表名映射到字段名列表的 JSON 对象")

        # 模型只返回名称；字段类型、说明、示例等仍从原候选数据中提取。
        table_map = {table["name"]: table for table in table_infos}
        for table_name, column_names in selection.items():
            if table_name not in table_map:
                raise ValueError(f"模型选择了候选集合之外的表: {table_name}")
            if not isinstance(column_names, list) or not column_names or not all(
                isinstance(name, str) for name in column_names
            ):
                raise ValueError(f"表 {table_name} 的筛选字段必须是非空字符串列表")
            available_columns = {column["name"] for column in table_map[table_name]["columns"]}
            unknown_columns = set(column_names) - available_columns
            if unknown_columns:
                raise ValueError(f"表 {table_name} 中没有这些候选字段: {sorted(unknown_columns)}")

        filtered_tables: list[TableInfoState] = []
        for table in table_infos:
            if table["name"] not in selection:
                continue
            selected_columns = set(selection[table["name"]])
            if len(selection) > 1:
                # 当前元数据没有完整外键指向，保留已选表的候选主外键供后续选择。
                key_columns = {
                    column["name"] for column in table["columns"]
                    if column["role"] in ("primary_key", "foreign_key")
                }
                restored_keys = key_columns - selected_columns
                if restored_keys:
                    logger.info(f"表 {table['name']} 补回主外键候选: {sorted(restored_keys)}")
                selected_columns.update(key_columns)

            # 保留原顺序和完整字段说明，也避免与并行节点共享可变列表。
            filtered_table = deepcopy(table)
            filtered_table["columns"] = [
                column for column in filtered_table["columns"]
                if column["name"] in selected_columns
            ]
            filtered_tables.append(filtered_table)

        selected_schema = {
            table["name"]: [column["name"] for column in table["columns"]]
            for table in filtered_tables
        }
        logger.info(f"过滤后的候选表和字段: {selected_schema}")
        writer({"type": "progress", "step": "过滤表信息", "status": "success"})
        return {"table_infos": filtered_tables}
    except Exception:
        writer({"type": "progress", "step": "过滤表信息", "status": "error"})
        logger.exception("过滤表信息失败")
        raise
