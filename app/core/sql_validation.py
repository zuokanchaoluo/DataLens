import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel, SqlglotError


class SQLValidationError(ValueError):
    """SQL 本身不符合要求，可作为后续纠错节点的输入。"""


def normalize_read_only_sql(sql: str) -> str:
    """解析一条 MySQL 查询，返回将被数据库校验的规范化文本。"""
    if not isinstance(sql, str) or not sql.strip():
        raise SQLValidationError("待校验 SQL 不能为空")
    try:
        parsed = sqlglot.parse(sql, read="mysql", error_level=ErrorLevel.RAISE)
    except SqlglotError as error:
        raise SQLValidationError(f"SQL 结构解析失败: {error}") from error

    # MySQL 可执行注释的含义不同于普通注释，不能当作无害文字忽略。
    for statement in parsed:
        if statement is not None:
            for node in statement.walk():
                if any(comment.lstrip().upper().startswith(("!", "M!"))
                       for comment in node.comments or []):
                    raise SQLValidationError("不允许 MySQL 可执行注释")

    # SQLGlot 可能将末尾普通注释解析为 Semicolon，它不代表第二条查询。
    statements = [item for item in parsed if item is not None and not isinstance(item, exp.Semicolon)]
    if len(statements) != 1:
        raise SQLValidationError("只允许一条查询 SQL，不允许多条语句")
    statement = statements[0]
    if not isinstance(statement, exp.Query) or statement.find(exp.Select) is None:
        raise SQLValidationError("只允许 SELECT 查询，支持只读 CTE 和集合查询")

    forbidden_nodes = (
        exp.DML, exp.DDL, exp.Command, exp.Into, exp.Lock,
        exp.PropertyEQ, exp.Parameter, exp.SessionParameter, exp.Placeholder, exp.Hint,
    )
    for node in statement.walk():
        if isinstance(node, forbidden_nodes):
            raise SQLValidationError("不允许写入、导出、锁定、变量/参数或查询提示等操作")
        if isinstance(node, exp.Anonymous):
            raise SQLValidationError(f"当前不支持自定义或未识别的查询函数: {node.name}")
        if isinstance(node, exp.Dot) and isinstance(node.expression, exp.Func):
            raise SQLValidationError("不支持带库名前缀的函数调用")

    try:
        # 后续使用同一份规范化文本，避免解析器与数据库对原始注释理解不同。
        return statement.sql(dialect="mysql", comments=False, unsupported_level=ErrorLevel.RAISE)
    except SqlglotError as error:
        raise SQLValidationError(f"该 SQL 结构暂不支持 MySQL 输出: {error}") from error
