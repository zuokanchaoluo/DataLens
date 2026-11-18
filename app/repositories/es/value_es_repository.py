from dataclasses import asdict

from elasticsearch import AsyncElasticsearch

from app.entities.value_info import ValueInfo


class ValueEsRepository:

    index_name = "value_index"
    index_mappings = {
        "dynamic": False,
        "properties": {
            "id": {"type": "keyword"},
            "value": {"type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_max_word"},
            "column_id": {"type": "keyword"}
        }
    }


    def __init__(self,client:AsyncElasticsearch):
        self.client = client

    async def ensure_index(self):
        if not await self.client.indices.exists(index=self.index_name):
            await self.client.indices.create(
                index=self.index_name,
                mappings=self.index_mappings
            )

    async def index(self, value_infos: list[ValueInfo],batch_size = 20):
        for i in range(0,len(value_infos),batch_size):
            batch_value_infos = value_infos[i:i+batch_size]
            batch_operations = []
            for value_info in batch_value_infos:
                batch_operations.append(
                    {
                        "index":{
                            "_index":self.index_name,
                            "_id":value_info.id
                        }
                    }

                )
                batch_operations.append(asdict(value_info))
            result = await self.client.bulk(operations = batch_operations)
            if result.get("errors"):
                errors = [
                    detail["error"]
                    for item in result["items"]
                    for detail in item.values()
                    if "error" in detail
                ]
                raise RuntimeError(f"字段取值批量写入 ES 失败: {errors[0] if errors else '未知错误'}")

    async def search(
        self, keyword: str, score_threshold: float = 0.6, limit: int = 5
    ) -> list[ValueInfo]:
        result = await self.client.search(
            index=self.index_name,
            query={"match": {"value": keyword}},
            min_score=score_threshold,
            size=limit,
        )
        return [ValueInfo(**hit["_source"]) for hit in result["hits"]["hits"]]
