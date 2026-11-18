from typing import Any, TypedDict

from app.core.sql_result import SQLResult
from app.entities.column_info import ColumnInfo
from app.entities.metric_info import MetricInfo
from app.entities.value_info import ValueInfo


class ColumnInfoState(TypedDict):
    name: str
    type: str
    role: str
    examples: list[Any]
    description: str
    alias: list[str]


class TableInfoState(TypedDict):
    name: str
    role: str
    description: str
    columns: list[ColumnInfoState]


class MetricInfoState(TypedDict):
    name: str
    description: str
    relevant_columns: list[str]
    alias: list[str]


class DateInfoState(TypedDict):
    date: str
    weekday: str
    quarter: str
    timezone: str


class DBInfoState(TypedDict):
    dialect: str
    version: str


class DataAgentState(TypedDict):
    query:str #用户输入的查询
    keywords:list[str]#抽取的关键词
    retrieved_column_infos:list[ColumnInfo]
    retrieved_metric_infos: list[MetricInfo] # 召回的候选指标，后续还需要过滤
    retrieved_value_infos: list[ValueInfo] # 真实取值及其所属字段，供后续合并
    table_infos: list[TableInfoState] # 按表分组并补充主外键后的候选上下文
    metric_infos: list[MetricInfoState] # 包含统一字段引用的候选指标上下文
    date_info: DateInfoState # 明确时区的当前日期信息
    db_info: DBInfoState # 实际连接的业务数据库类型和版本
    sql: str # 模型生成的候选 SQL，后续必须校验
    validated_sql: str | None # 通过结构检查和数据库 EXPLAIN 的同一份 SQL
    error: str | None # 校验 SQL 时的错误信息；无错误时为 None
    sql_correction_attempts: int # 本次问数已经生成的纠错候选数，生成初始 SQL 时归零
    sql_result: SQLResult | None # 本次 SQL 的列名、行数据、返回行数和截断标记
