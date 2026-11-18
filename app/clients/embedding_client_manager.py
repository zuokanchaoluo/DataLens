import asyncio

from huggingface_hub import AsyncInferenceClient, InferenceClient
from langchain_huggingface import HuggingFaceEndpointEmbeddings

from app.conf.app_config import EmbeddingConfig, app_config
from app.core.network import configure_local_proxy_bypass


class EmbeddingClientManager:
    def __init__(self,config:EmbeddingConfig):
        self.client:HuggingFaceEndpointEmbeddings|None = None
        self.config = config

    def _get_url(self):
        return f"http://{self.config.host}:{self.config.port}"

    def init(self):
        if self.client is not None:
            raise RuntimeError("Embedding 客户端已初始化，请先关闭")
        configure_local_proxy_bypass()
        # LangChain requires a repo ID; the underlying HF clients accept a TEI URL.
        self.client = HuggingFaceEndpointEmbeddings(model=self.config.model)
        self.client.client = InferenceClient(model=self._get_url(), timeout=30)
        self.client.async_client = AsyncInferenceClient(model=self._get_url(), timeout=30)

    async def close(self):
        client = self.client
        self.client = None
        if client is not None:
            try:
                await client.async_client.close()
            finally:
                client.client.close()

embedding_client_manager = EmbeddingClientManager(app_config.embedding)

if __name__ == '__main__':
    embedding_client_manager.init()
    client = embedding_client_manager.client

    async def test():
        text = "What is deep learning?"
        query_result = await client.aembed_query(text)
        print(query_result[:3])
    asyncio.run(test())
