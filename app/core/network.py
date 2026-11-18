import os


def configure_local_proxy_bypass():
    """当前进程的回环服务直连，保留已有代理和其他 NO_PROXY 规则。"""
    entries = [
        item.strip()
        for key in ("NO_PROXY", "no_proxy")
        for item in os.environ.get(key, "").split(",")
        if item.strip()
    ]
    value = ",".join(dict.fromkeys(entries + ["localhost", "127.0.0.1", "::1"]))
    os.environ["NO_PROXY"] = value
    os.environ["no_proxy"] = value
