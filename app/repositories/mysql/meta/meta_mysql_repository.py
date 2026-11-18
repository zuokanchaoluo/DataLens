from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.entities.column_info import ColumnInfo
from app.entities.column_metric import ColumnMetric
from app.entities.metric_info import MetricInfo
from app.entities.table_info import TableInfo
from app.models.column_info import ColumnInfoMySQL
from app.models.table_info import TableInfoMySQL
from app.repositories.mysql.meta.mappers.column_info_mapper import ColumnInfoMapper
from app.repositories.mysql.meta.mappers.column_metric_mapper import ColumnMetricMapper
from app.repositories.mysql.meta.mappers.metric_info_mapper import MetricInfoMapper
from app.repositories.mysql.meta.mappers.table_info_mapper import TableInfoMapper


class MetaMySQLRepository:
    def __init__(self,session:AsyncSession):
        self.session = session

    async def save_table_infos(self, table_infos:list[TableInfo]):
        for table_info in table_infos:
            await self.session.merge(TableInfoMapper.to_model(table_info))

    async def save_column_infos(self, column_infos:list[ColumnInfo]):
        for column_info in column_infos:
            await self.session.merge(ColumnInfoMapper.to_model(column_info))

    async def save_metric_infos(self, metric_infos:list[MetricInfo]):
        for metric_info in metric_infos:
            await self.session.merge(MetricInfoMapper.to_model(metric_info))

    async def save_column_metrics(self, column_metrics:list[ColumnMetric]):
        for column_metric in column_metrics:
            await self.session.merge(ColumnMetricMapper.to_model(column_metric))

    async def get_column_info_by_id(self, column_id: str) -> ColumnInfo | None:
        model = await self.session.get(ColumnInfoMySQL, column_id)
        if model is not None:
            return ColumnInfoMapper.to_entity(model)

        # 指标/ES 使用 table.column，而已有字段主键可能是 table_column。
        # 通过实际的所属表和字段名定位，不用拆解含下划线的旧主键。
        if "." not in column_id:
            return None
        table_id, column_name = column_id.split(".", 1)
        result = await self.session.execute(select(ColumnInfoMySQL).where(
            ColumnInfoMySQL.table_id == table_id,
            ColumnInfoMySQL.name == column_name,
        ))
        models = result.scalars().all()
        if len(models) > 1:
            raise ValueError(f"字段 {column_id} 对应多条元数据，请检查元数据库记录")
        return ColumnInfoMapper.to_entity(models[0]) if models else None

    async def get_table_info_by_id(self, table_id: str) -> TableInfo | None:
        model = await self.session.get(TableInfoMySQL, table_id)
        return TableInfoMapper.to_entity(model) if model is not None else None

    async def get_key_columns_by_table_id(self, table_id: str) -> list[ColumnInfo]:
        result = await self.session.execute(select(ColumnInfoMySQL).where(
            ColumnInfoMySQL.table_id == table_id,
            ColumnInfoMySQL.role.in_(("primary_key", "foreign_key")),
        ).order_by(ColumnInfoMySQL.name, ColumnInfoMySQL.id))
        return [ColumnInfoMapper.to_entity(model) for model in result.scalars().all()]

