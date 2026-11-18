from langchain.chat_models import init_chat_model

from app.conf.app_config import app_config

# app_config.yaml resolves this value from OPENAI_API_KEY at runtime.
api_key = app_config.llm.api_key.strip()
if not api_key:
    raise RuntimeError(
        "未读取到 OPENAI_API_KEY，请在运行 Python 的终端或 IDE 中设置百炼 API Key。"
    )

llm = init_chat_model(
    model=app_config.llm.model_name,
    model_provider="openai",
    base_url = app_config.llm.base_url,
    api_key=api_key,
    temperature = 0
)

if __name__ == '__main__':
    print(llm.invoke("你好").content)
