import re


def clean_sql_output(content: str) -> str:
    """只整理模型输出的文本外壳，SQL 本身仍需交给校验节点。"""
    sql = content.strip()
    if sql.startswith("```"):
        block = re.fullmatch(r"```(?:sql)?[ \t]*\r?\n(.*?)\r?\n```", sql,
                             flags=re.IGNORECASE | re.DOTALL)
        if block is None or re.search(r"(?m)^[ \t]*```", block.group(1)):
            raise ValueError("模型返回的 SQL 代码块格式不完整或包含多个代码块，请返回纯 SQL 文本")
        sql = block.group(1).strip()
    if not sql:
        raise ValueError("模型没有返回 SQL 文本")
    return sql
