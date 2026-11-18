from copy import deepcopy

from langgraph.runtime import Runtime

from app.agent.state import ColumnInfoState, DataAgentState, MetricInfoState, TableInfoState
from app.agent.context import DataAgentContext
from app.core.log import logger
from app.entities.column_info import ColumnInfo


async def merge_retrieved_info(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer({"type": "progress", "step": "合并召回信息", "status": "running"})

    try:
        repository = runtime.context["meta_mysql_repository"]
        columns: dict[str, ColumnInfo] = {}
        column_aliases: dict[str, str] = {}

        def add_column(column: ColumnInfo) -> str:
            column_id = f"{column.table_id}.{column.name}"
            if column_id not in columns:
                # 后续会向 examples 补值，不能修改召回节点返回的原始对象。
                copied = deepcopy(column)
                copied.id = column_id
                copied.examples = copied.examples or []
                columns[column_id] = copied
            column_aliases[column.id] = column_id
            return column_id

        async def require_column(reference: str) -> ColumnInfo:
            column_id = column_aliases.get(reference, reference)
            if column_id not in columns:
                column = await repository.get_column_info_by_id(reference)
                if column is None:
                    raise ValueError(f"元数据库中找不到字段 {reference}，请检查元知识数据是否一致")
                column_id = add_column(column)
            return columns[column_id]

        for column in state["retrieved_column_infos"]:
            add_column(column)

        # 补入指标引用但字段召回未命中的字段，同时统一引用格式。
        metric_infos: list[MetricInfoState] = []
        seen_metrics: set[str] = set()
        for metric in state["retrieved_metric_infos"]:
            if metric.id in seen_metrics:
                continue
            relevant_columns = []
            for reference in metric.relevant_columns:
                column = await require_column(reference)
                relevant_columns.append(column.id)
            metric_infos.append(MetricInfoState(
                name=metric.name,
                description=metric.description,
                relevant_columns=list(dict.fromkeys(relevant_columns)),
                alias=list(metric.alias),
            ))
            seen_metrics.add(metric.id)

        # 取值命中的字段也要进入候选表，并保留用户问题中匹配的真实值。
        for value in state["retrieved_value_infos"]:
            column = await require_column(value.column_id)
            if value.value not in column.examples:
                column.examples.append(value.value)

        table_to_columns: dict[str, dict[str, ColumnInfo]] = {}
        for column in columns.values():
            table_to_columns.setdefault(column.table_id, {})[column.id] = column

        table_infos: list[TableInfoState] = []
        for table_id, table_columns in table_to_columns.items():
            table = await repository.get_table_info_by_id(table_id)
            if table is None:
                raise ValueError(f"元数据库中找不到表 {table_id}，请检查元知识数据是否一致")
            key_columns = await repository.get_key_columns_by_table_id(table_id)
            for column in key_columns:
                if column.table_id != table_id:
                    raise ValueError(f"主外键字段 {column.id} 的所属表与 {table_id} 不一致")
                column_id = add_column(column)
                table_columns[column_id] = columns[column_id]
            table_infos.append(TableInfoState(
                name=table.name,
                role=table.role,
                description=table.description,
                columns=[ColumnInfoState(
                    name=column.name,
                    type=column.type,
                    role=column.role,
                    examples=column.examples,
                    description=column.description,
                    alias=column.alias,
                ) for column in table_columns.values()],
            ))

        logger.info(f"合并后的候选表: {[table['name'] for table in table_infos]}，"
                    f"候选指标: {[metric['name'] for metric in metric_infos]}")
        writer({"type": "progress", "step": "合并召回信息", "status": "success"})
        return {"table_infos": table_infos, "metric_infos": metric_infos}
    except Exception:
        writer({"type": "progress", "step": "合并召回信息", "status": "error"})
        logger.exception("合并召回信息失败")
        raise
