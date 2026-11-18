from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from app.agent.llm import llm
from app.agent.state import DataAgentState
from app.agent.context import DataAgentContext
from app.core.log import logger
from app.entities.value_info import ValueInfo
from app.prompt.prompt_loader import load_prompt


async def recall_value(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer({"type": "progress", "step": "召回字段取值", "status": "running"})

    try:
        value_repository = runtime.context["value_es_repository"]
        prompt = PromptTemplate(
            template=load_prompt("extend_keywords_for_value_recall"),
            input_variables=["query"],
        )
        chain = prompt | llm | JsonOutputParser()
        expanded_keywords = await chain.ainvoke({"query": state["query"]})
        if not isinstance(expanded_keywords, list) or not all(
            isinstance(keyword, str) for keyword in expanded_keywords
        ):
            raise ValueError("字段取值关键词扩展结果必须是字符串组成的 JSON 数组")

        keywords = list(dict.fromkeys(
            keyword.strip()
            for keyword in state["keywords"] + expanded_keywords
            if keyword.strip()
        ))
        logger.info(f"字段取值召回检索关键词: {keywords}")

        value_info_map: dict[str, ValueInfo] = {}
        for keyword in keywords:
            # ES 直接检索文本；模型生成的词不一定是数据库中的真实取值。
            values = await value_repository.search(keyword)
            for value in values:
                # 按记录 ID 去重，保留不同字段中恰好相同的取值。
                if value.id not in value_info_map:
                    value_info_map[value.id] = value

        retrieved_value_infos = list(value_info_map.values())
        logger.info(f"检索到字段取值: {list(value_info_map)}")
        writer({"type": "progress", "step": "召回字段取值", "status": "success"})
        return {"retrieved_value_infos": retrieved_value_infos}
    except Exception:
        writer({"type": "progress", "step": "召回字段取值", "status": "error"})
        logger.exception("召回字段取值失败")
        raise
