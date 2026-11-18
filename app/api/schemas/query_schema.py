from pydantic import BaseModel, ConfigDict, Field


class QuerySchema(BaseModel):
    """客户端提交的问数请求，只描述接口输入。"""

    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    query: str = Field(
        min_length=1,
        strict=True,
        description="用户想查询的业务问题，去除首尾空白后不能为空。",
        examples=["统计华北地区的销售总额"],
    )
