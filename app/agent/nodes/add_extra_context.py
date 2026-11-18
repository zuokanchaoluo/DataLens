from datetime import datetime, timedelta, timezone

from langgraph.runtime import Runtime

from app.agent.state import DataAgentState, DateInfoState
from app.agent.context import DataAgentContext
from app.core.log import logger


# 当前业务按北京时间处理，使用固定 UTC+8 避免依赖服务器时区或额外时区数据包。
BEIJING_TIMEZONE = timezone(timedelta(hours=8), name="Asia/Shanghai")
WEEKDAYS = ("星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日")


async def add_extra_context(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer({"type": "progress", "step": "添加额外上下文", "status": "running"})

    try:
        # 只读取一次时钟，保证日期、星期和季度来自同一个时间点。
        today = datetime.now(BEIJING_TIMEZONE)
        date_info = DateInfoState(
            date=today.strftime("%Y-%m-%d"),
            weekday=WEEKDAYS[today.weekday()],
            quarter=f"Q{(today.month - 1) // 3 + 1}",
            timezone="Asia/Shanghai",
        )
        repository = runtime.context["dw_mysql_repository"]
        db_info = await repository.get_db_info()

        logger.info(f"额外上下文: 日期信息-{date_info}，数据库信息-{db_info}")
        writer({"type": "progress", "step": "添加额外上下文", "status": "success"})
        return {"date_info": date_info, "db_info": db_info}
    except Exception:
        writer({"type": "progress", "step": "添加额外上下文", "status": "error"})
        logger.exception("添加额外上下文失败")
        raise
