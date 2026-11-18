"""启动正式 API，通过真实 HTTP/SSE 问数并与独立只读 SQL 核对。

需要真实外部服务与 OPENAI_API_KEY；模型外发范围与现有真实联调相同。
"""
import argparse
import asyncio
import json
import re
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter

import httpx
import uvicorn

from app.scripts.check_data_agent import CASES, results_match


class APICheckError(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise APICheckError(message)


async def read_query(client, name, timeout):
    case = CASES[name]
    started = perf_counter()
    report = {"case": name, "query": case["query"], "events": []}
    async with asyncio.timeout(timeout):
        async with client.stream("POST", "/api/query", json={"query": case["query"]}) as response:
            report.update({"http_status": response.status_code,
                           "content_type": response.headers.get("content-type", ""),
                           "request_id": response.headers.get("x-request-id", ""),
                           "headers_seconds": round(perf_counter() - started, 4)})
            require(response.status_code == 200, "接口未返回 HTTP 200")
            require(response.headers.get("content-type", "").startswith("text/event-stream"), "响应不是 SSE")
            require(response.headers.get("cache-control") == "no-cache", "SSE 缺少 no-cache 响应头")
            require(len(response.headers.get_list("x-request-id")) == 1
                    and re.fullmatch(r"[0-9a-f]{32}", report["request_id"]), "request_id 响应头无效")
            print(f"[{name}] HTTP 200, request_id={report['request_id']}", flush=True)
            lines = []
            async for line in response.aiter_lines():
                if line:
                    lines.append(line)
                    continue
                if not lines:
                    continue
                require(len(lines) == 1 and lines[0].startswith("data: "), "收到不符合项目约定的 SSE 帧")
                event = json.loads(lines[0][6:])
                lines.clear()
                require(isinstance(event, dict) and event.get("type") in ("progress", "result", "error"), "SSE 事件类型无效")
                elapsed = round(perf_counter() - started, 4)
                if not report["events"]:
                    report["first_event_seconds"] = elapsed
                report["events"].append({"elapsed_seconds": elapsed, "event": event})
                if event["type"] == "progress":
                    require(isinstance(event.get("step"), str)
                            and event.get("status") in ("running", "success", "error"), "进度格式无效")
                    if event["status"] == "running":
                        print(f"[{name}] {elapsed:.3f}s {event['step']}", flush=True)
            require(not lines, "SSE 在完整帧结束之前中断")
    report["elapsed_seconds"] = round(perf_counter() - started, 4)
    terminal = [item["event"] for item in report["events"] if item["event"]["type"] in ("result", "error")]
    require(len(terminal) == 1 and report["events"][-1]["event"] is terminal[0], "终止事件缺失、重复或顺序错误")
    require(terminal[0]["type"] == "result", "工作流返回终止错误，请查看脱敏日志")
    event = terminal[0]
    require(isinstance(event.get("data"), list) and isinstance(event.get("columns"), list)
            and type(event.get("row_count")) is int and event["row_count"] == len(event["data"])
            and type(event.get("truncated")) is bool, "结果元数据无效")
    report["actual"] = {"rows": event["data"], "columns": event["columns"],
                        "row_count": event["row_count"], "truncated": event["truncated"]}
    return report


def pool_usage(meta_manager, dw_manager):
    return {"meta": meta_manager.engine.pool.checkedout(), "dw": dw_manager.engine.pool.checkedout()}


async def wait_pools_idle(meta_manager, dw_manager):
    for _ in range(200):
        counts = pool_usage(meta_manager, dw_manager)
        if not any(counts.values()):
            return counts
        await asyncio.sleep(0.05)
    raise APICheckError("响应结束后数据库连接未及时归还池")


async def check(names, timeout, report_path):
    # 延迟导入：--help 不加载模型，也不读取实际凭据。
    from sqlalchemy import text
    from app.conf.app_config import app_config
    from app.core.log import log_format, logger
    from app.clients.embedding_client_manager import embedding_client_manager
    from app.clients.es_client_manager import es_client_manager
    from app.clients.mysql_client_manager import meta_mysql_client_manager, dw_mysql_client_manager
    from app.clients.qdrant_client_manager import qdrant_client_manager
    from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository
    from main import app

    report_path.parent.mkdir(parents=True, exist_ok=True)
    log_path = report_path.with_suffix(".log")
    records = []
    secrets = {app_config.llm.api_key, app_config.db_meta.password, app_config.db_dw.password} - {""}

    def redact(value):
        for secret in sorted(secrets, key=len, reverse=True):
            value = value.replace(secret, "[REDACTED]")
        return value

    report = {"checked_at": datetime.now(timezone(timedelta(hours=8))).isoformat(),
              "timezone": "Asia/Shanghai", "real_external_services": True,
              "model": app_config.llm.model_name, "cases": [], "log_path": str(log_path), "passed": False}

    def save():
        report_path.write_text(redact(json.dumps(report, ensure_ascii=False, indent=2)), encoding="utf-8")

    # 此验证运行在独立进程，只向专用文件写脱敏日志，避免控制台刷出元数据。
    logger.remove()
    log_file = log_path.open("w", encoding="utf-8")

    def capture(message):
        log_file.write(redact(str(message)))
        log_file.flush()
        record = message.record
        records.append({"request_id": record["extra"].get("request_id", "-"),
                        "module": record["name"], "level": record["level"].name})

    sink = logger.add(capture, format=log_format, level="INFO", backtrace=False, diagnose=False, colorize=False)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, lifespan="on", log_level="critical", access_log=False))
    server_task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        for _ in range(600):
            if server.started:
                break
            if server_task.done():
                await server_task
                raise APICheckError("正式应用未能启动")
            await asyncio.sleep(0.05)
        require(server.started, "正式应用启动超时")
        print("正式 Uvicorn 与 lifespan 已启动，开始只读预检查。", flush=True)

        async with meta_mysql_client_manager.session_factory() as session:
            result = await session.execute(text("SELECT (SELECT COUNT(*) FROM table_info) AS tables_count, "
                                                "(SELECT COUNT(*) FROM column_info) AS columns_count, "
                                                "(SELECT COUNT(*) FROM metric_info) AS metrics_count"))
            report["metadata_counts"] = dict(result.mappings().one())
        report["qdrant_points"] = {}
        for collection in ("column_info_collection", "metric_info_collection"):
            info = await qdrant_client_manager.client.get_collection(collection)
            report["qdrant_points"][collection] = info.points_count
        report["es_value_count"] = (await es_client_manager.client.count(index="value_index"))["count"]
        report["es_cluster_status"] = (await es_client_manager.client.cluster.health())["status"]
        async with dw_mysql_client_manager.session_factory() as session:
            report["database"] = await DWMySQLRepository(session).get_db_info()
        require(all(value > 0 for value in report["metadata_counts"].values()), "元数据库缺少必要知识")
        require(all(value and value > 0 for value in report["qdrant_points"].values()), "向量集合为空")
        require(report["es_value_count"] > 0 and report["es_cluster_status"] != "red", "取值索引或 ES 未就绪")
        print("元数据、字段/指标向量与取值索引已就绪；没有重建知识库。", flush=True)
        save()

        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", trust_env=False,
                                     timeout=httpx.Timeout(timeout, connect=5)) as client:
            identifiers = set()
            for name in names:
                case_report = {"case": name, "query": CASES[name]["query"], "passed": False}
                report["cases"].append(case_report)
                try:
                    async with dw_mysql_client_manager.session_factory() as session:
                        repository = DWMySQLRepository(session)
                        sql = await repository.validate_sql(CASES[name]["reference_sql"])
                        case_report["reference_sql"] = sql
                        case_report["expected"] = await repository.execute_sql(sql)
                    log_start = len(records)
                    case_report.update(await read_query(client, name, timeout))
                    case_report["connections_after_response"] = await wait_pools_idle(meta_mysql_client_manager, dw_mysql_client_manager)
                    request_id = case_report["request_id"]
                    require(request_id not in identifiers, "不同查询复用了 request_id")
                    identifiers.add(request_id)
                    node_logs = [record for record in records[log_start:] if record["module"].startswith("app.agent.nodes.")]
                    require(node_logs and all(record["request_id"] == request_id for record in node_logs), "节点日志与响应头的 request_id 不一致")
                    case_report["log_modules"] = sorted({record["module"] for record in node_logs})
                    case_report["node_log_count"] = len(node_logs)
                    case_report["request_id_verified"] = True
                    require({"app.agent.nodes.generate_sql", "app.agent.nodes.run_sql"}.issubset(case_report["log_modules"]), "缺少生成/执行节点日志")
                    require(results_match(case_report["actual"], case_report["expected"]), "结果与独立基准 SQL 不一致")
                    case_report["passed"] = True
                    print(f"[{name}] PASS，返回 {case_report['actual']['row_count']} 行，耗时 {case_report['elapsed_seconds']:.3f}s", flush=True)
                except Exception as error:
                    case_report["error"] = str(error) if isinstance(error, APICheckError) else type(error).__name__
                    print(f"[{name}] FAIL：{case_report['error']}", flush=True)
                save()
        report["connections_before_shutdown"] = await wait_pools_idle(meta_mysql_client_manager, dw_mysql_client_manager)
    except Exception as error:
        report["error"] = str(error) if isinstance(error, APICheckError) else type(error).__name__
    finally:
        try:
            server.should_exit = True
            await asyncio.wait_for(server_task, timeout=15)
            report["temporary_server_stopped"] = server_task.done()
            report["resources_cleared"] = all(resource is None for resource in (
                embedding_client_manager.client, qdrant_client_manager.client, es_client_manager.client,
                meta_mysql_client_manager.engine, meta_mysql_client_manager.session_factory,
                dw_mysql_client_manager.engine, dw_mysql_client_manager.session_factory,
            ))
        finally:
            listener.close()
            logger.remove(sink)
            log_file.close()
            report["passed"] = ("error" not in report and len(report["cases"]) == len(names)
                                and all(case["passed"] for case in report["cases"])
                                and report.get("temporary_server_stopped") and report.get("resources_cleared"))
            save()
    print(f"通过 {sum(case['passed'] for case in report['cases'])}/{len(names)}，报告：{report_path.resolve()}", flush=True)
    return report["passed"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=list(CASES), action="append", help="可重复指定；默认全部三项")
    parser.add_argument("--timeout", type=float, default=180, help="单次 HTTP 查询总等待秒数")
    parser.add_argument("--report", type=Path, default=Path(".local/query_api_e2e_report.json"))
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout 必须大于 0")
    raise SystemExit(0 if asyncio.run(check(args.case or list(CASES), args.timeout, args.report)) else 1)
