from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from app.agent.llm import llm
from app.agent.state import DataAgentState
from app.agent.context import DataAgentContext
from app.core.log import logger
from app.entities.metric_info import MetricInfo
from app.prompt.prompt_loader import load_prompt


async def recall_metric(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer({"type": "progress", "step": "召回指标", "status": "running"})

    try:
        embedding_client = runtime.context["embedding_client"]
        metric_repository = runtime.context["metric_qdrant_repository"]

        # 模型只提供检索线索，真正的指标定义来自已有的向量知识库。
        prompt = PromptTemplate(
            template=load_prompt("extend_keywords_for_metric_recall"),
            input_variables=["query"],
        )
        chain = prompt | llm | JsonOutputParser()
        expanded_keywords = await chain.ainvoke({"query": state["query"]})
        if not isinstance(expanded_keywords, list) or not all(
            isinstance(keyword, str) for keyword in expanded_keywords
        ):
            raise ValueError("指标关键词扩展结果必须是字符串组成的 JSON 数组")

        # 去除空白及重复关键词，并保留顺序，避免重复请求向量服务。
        keywords = list(dict.fromkeys(
            keyword.strip()
            for keyword in state["keywords"] + expanded_keywords
            if keyword.strip()
        ))
        logger.info(f"指标召回检索关键词: {keywords}")

        metric_info_map: dict[str, MetricInfo] = {}
        for keyword in keywords:
            embedding = await embedding_client.aembed_query(keyword)
            metrics = await metric_repository.search(embedding)
            for metric in metrics:
                # 名称、描述、别名可能分别命中，同一个指标只返回一次。
                if metric.id not in metric_info_map:
                    metric_info_map[metric.id] = metric

        retrieved_metric_infos = list(metric_info_map.values())
        logger.info(f"检索到指标信息: {list(metric_info_map)}")
        writer({"type": "progress", "step": "召回指标", "status": "success"})
        return {"retrieved_metric_infos": retrieved_metric_infos}
    except Exception:
        writer({"type": "progress", "step": "召回指标", "status": "error"})
        logger.exception("召回指标信息失败")
        raise
