import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession, async_sessionmaker

from app.conf.app_config import DBConfig, app_config


class MysqlClientManager:
    def __init__(self,config:DBConfig):
        self.engine:AsyncEngine|None = None
        self.session_factory= None
        self.config = config

    def _get_url(self):
        return f"mysql+asyncmy://{self.config.user}:{self.config.password}@{self.config.host}:{self.config.port}/{self.config.database}?charset=utf8mb4"

    def init(self):
        if self.engine is not None or self.session_factory is not None:
            raise RuntimeError("MySQL 客户端已初始化，请先关闭")
        self.engine = create_async_engine(self._get_url(),pool_size = 10,pool_pre_ping=True)
        self.session_factory = async_sessionmaker(self.engine, autoflush = True, expire_on_commit = False)

    async def close(self):
        engine = self.engine
        self.engine = None
        self.session_factory = None
        if engine is not None:
            await engine.dispose()

meta_mysql_client_manager = MysqlClientManager(app_config.db_meta)
dw_mysql_client_manager = MysqlClientManager(app_config.db_dw)

if __name__ == '__main__':
    dw_mysql_client_manager.init()


    async def test():
        async with dw_mysql_client_manager.session_factory() as session:
            sql = "select * from fact_order limit 10"
            result = await session.execute(text(sql))

            rows = result.mappings().fetchall()

            print(type(rows))
            print(type(rows[0]))
            print(rows[0]['order_id'])

    asyncio.run(test())
