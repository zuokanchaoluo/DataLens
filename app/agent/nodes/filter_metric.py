from copy import deepcopy

import yaml
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from app.agent.llm import llm
from app.agent.state import DataAgentState
from app.agent.context import DataAgentContext
from app.core.log import logger
from app.prompt.prompt_loader import load_prompt


async def filter_metric(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer({"type": "progress", "step": "过滤指标信息", "status": "running"})

    try:
        metric_infos = state["metric_infos"]
        if not metric_infos:
            logger.info("没有候选指标，跳过模型筛选")
            writer({"type": "progress", "step": "过滤指标信息", "status": "success"})
            return {"metric_infos": []}

        available_names = {metric["name"] for metric in metric_infos}
        if len(available_names) != len(metric_infos):
            raise ValueError("候选指标名称重复，无法按名称唯一筛选，请检查元知识数据")

        prompt = PromptTemplate(
            template=load_prompt("filter_metric_info"),
            input_variables=["query", "metric_infos"],
        )
        chain = prompt | llm | JsonOutputParser()
        selection = await chain.ainvoke({
            "query": state["query"],
            "metric_infos": yaml.safe_dump(metric_infos, allow_unicode=True, sort_keys=False),
        })
        if not isinstance(selection, list) or not all(
            isinstance(name, str) and name.strip() for name in selection
        ):
            raise ValueError("指标筛选结果必须是非空指标名称组成的 JSON 数组，允许空数组 []")

        selected_names = set(selection)
        unknown_names = selected_names - available_names
        if unknown_names:
            raise ValueError(f"模型选择了候选集合之外的指标名称: {sorted(unknown_names)}")

        # 只按名称做选择，不让模型重新生成业务定义；保持候选顺序并隔离可变列表。
        filtered_metrics = [
            deepcopy(metric) for metric in metric_infos
            if metric["name"] in selected_names
        ]
        logger.info(f"过滤后的候选指标: {[metric['name'] for metric in filtered_metrics]}")
        writer({"type": "progress", "step": "过滤指标信息", "status": "success"})
        return {"metric_infos": filtered_metrics}
    except Exception:
        writer({"type": "progress", "step": "过滤指标信息", "status": "error"})
        logger.exception("过滤指标信息失败")
        raise
