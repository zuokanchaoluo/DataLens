from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.sql_result import SQLResult, build_sql_result
from app.core.sql_validation import SQLValidationError, normalize_read_only_sql


MAX_RESULT_ROWS = 1000


class DWMySQLRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_column_types(self, table_name)->dict[str, str]:
        sql = f"show columns from {table_name}"
        result = await self.session.execute(text(sql))
        result_dict = result.mappings().fetchall()
        # {Field:order_id,Type:varchar(30),Null:No},{Field:customer_id,Type:varchar(20),Null:YES
        return {row['Field']:row['Type'] for row in result_dict}
        # {order_id:varchar(30),customer_id:varchar(30)}

    async def get_column_values(self, table_name, column_name,limit = 10):
        sql = f"select distinct {column_name} from {table_name} limit {limit}"
        result = await self.session.execute(text(sql))
        return [row[0]for row in result.fetchall()]

    async def get_db_info(self) -> dict[str, str]:
        result = await self.session.execute(text("SELECT VERSION()"))
        version = result.scalar()
        if not isinstance(version, str) or not version.strip():
            raise ValueError("无法读取业务数据库版本信息，请检查数据库连接和查询结果")
        return {
            "dialect": self.session.get_bind().dialect.name,
            "version": version,
        }

    async def validate_sql(self, sql: str) -> str:
        normalized_sql = normalize_read_only_sql(sql)
        if self.session.get_bind().dialect.name not in ("mysql", "mariadb"):
            raise RuntimeError("当前数据库校验仅支持 MySQL/MariaDB")
        try:
            connection = await self.session.connection()
            # 普通 EXPLAIN 查看计划，不使用会实际执行查询的 ANALYZE。
            # 直接交给驱动，避免 SQL 字符串中的 ':name' 被当成 SQLAlchemy 绑定参数。
            # 不传空参数集合，保留 DATE_FORMAT 等表达式中的百分号字面量。
            result = await connection.exec_driver_sql(
                f"EXPLAIN FORMAT=JSON {normalized_sql}", execution_options={"no_parameters": True}
            )
            result.close()
        except DBAPIError as error:
            args = getattr(error.orig, "args", ())
            code = args[0] if args else None
            # 只将可通过改写 SQL 处理的错误交给纠错；连接/权限等故障继续抛出。
            sql_error_codes = {1052, 1054, 1055, 1064, 1109, 1111, 1140, 1146, 1222, 1235}
            if not error.connection_invalidated and code in sql_error_codes:
                detail = args[1] if len(args) > 1 else "数据库拒绝该查询"
                raise SQLValidationError(f"MySQL SQL 校验失败（{code}）: {detail}") from error
            raise
        return normalized_sql

    async def execute_sql(self, sql: str, *, max_rows: int = MAX_RESULT_ROWS) -> SQLResult:
        if type(max_rows) is not int or max_rows <= 0:
            raise ValueError("查询返回行数上限必须是正整数")
        # 仓库方法也保留结构检查，直接调用时同样拒绝写入或多条语句。
        normalized_sql = normalize_read_only_sql(sql)
        connection = await self.session.connection()

        def fetch_result(sync_connection):
            # run_sync 会将驱动 I/O 适配到异步连接；流式游标避免先缓冲整个结果集。
            # 直接交给驱动，保留 ':name' 等字面量，不改写已校验 SQL 的 LIMIT。
            result = sync_connection.exec_driver_sql(
                normalized_sql,
                execution_options={"stream_results": True, "max_row_buffer": max_rows + 1,
                                   "no_parameters": True},
            )
            try:
                columns = list(result.keys())
                # 多读一行，用于判断是否存在未返回的数据。
                rows = result.fetchmany(max_rows + 1)
                return build_sql_result(columns, rows, max_rows)
            finally:
                result.close()

        return await connection.run_sync(fetch_result)
