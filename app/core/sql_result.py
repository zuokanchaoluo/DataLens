from datetime import date, time, timedelta
from decimal import Decimal
from math import isfinite
from typing import Any, Sequence, TypedDict


class SQLResult(TypedDict):
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    truncated: bool


def _serialize_value(value: Any) -> Any:
    # 金额保持十进制精度，不转成可能产生舍入误差的 float。
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "0x" + bytes(value).hex()
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float) and isfinite(value):
        return value
    raise ValueError(f"查询结果包含不支持的值类型或非有限数值: {type(value).__name__}")


def build_sql_result(columns: list[str], rows: Sequence[Sequence[Any]], max_rows: int) -> SQLResult:
    if len(set(columns)) != len(columns):
        raise ValueError("查询结果包含重复列名，请使用 AS 为输出字段设置不同的别名")
    selected_rows = rows[:max_rows]
    return {
        "columns": columns,
        "rows": [
            {name: _serialize_value(value) for name, value in zip(columns, row)}
            for row in selected_rows
        ],
        "row_count": len(selected_rows),
        "truncated": len(rows) > max_rows,
    }
