"""真实服务端到端联调：运行工作流，并用独立编写的 SQL 核对结果。"""
import argparse
import asyncio
import json
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path
from time import perf_counter


CASES = {
    "region-total": {
        "query": "统计华北地区的销售总额",
        "reference_sql": "SELECT SUM(o.order_amount) AS total_amount FROM fact_order AS o "
                         "JOIN dim_region AS r ON o.region_id = r.region_id WHERE r.region_name = '华北'",
    },
    "region-breakdown": {
        "query": "按大区统计销售总额",
        "reference_sql": "SELECT r.region_name, SUM(o.order_amount) AS total_amount FROM fact_order AS o "
                         "JOIN dim_region AS r ON o.region_id = r.region_id GROUP BY r.region_name",
    },
    "average-order": {
        "query": "统计所有订单的平均订单金额（AOV）",
        "reference_sql": "SELECT AVG(order_amount) AS average_amount FROM fact_order",
    },
}


def _same_value(actual, expected):
    if actual is None or expected is None:
        return actual is expected
    try:
        left, right = Decimal(str(actual)), Decimal(str(expected))
    except InvalidOperation:
        return str(actual) == str(expected)
    return left.is_finite() and right.is_finite() and abs(left - right) <= Decimal("0.01")


def _same_row(actual, expected):
    # 本脚本的用例只含单个金额指标及可选地区维度，允许列别名和列顺序不同。
    if len(actual) != len(expected):
        return False
    remaining = list(actual.values())
    for expected_value in expected.values():
        index = next((i for i, value in enumerate(remaining) if _same_value(value, expected_value)), None)
        if index is None:
            return False
        remaining.pop(index)
    return True


def results_match(actual, expected):
    if actual is None or actual["truncated"] or expected["truncated"]:
        return False
    if len(actual["columns"]) != len(expected["columns"]) or len(actual["rows"]) != len(expected["rows"]):
        return False
    remaining = list(actual["rows"])
    for expected_row in expected["rows"]:
        index = next((i for i, row in enumerate(remaining) if _same_row(row, expected_row)), None)
        if index is None:
            return False
        remaining.pop(index)
    return True


async def run_case(name, timeout):
    # 延迟导入，查看 --help 或离线测试结果比较函数时无需模型密钥。
    from app.agent.graph import create_context, graph

    case = CASES[name]
    started = perf_counter()
    report = {"case": name, **case, "passed": False, "completed_nodes": []}
    print(f"\n[{name}] {case['query']}", flush=True)
    try:
        async with create_context() as context:
            repository = context["dw_mysql_repository"]
            reference_sql = await repository.validate_sql(case["reference_sql"])
            report["expected"] = await repository.execute_sql(reference_sql)
            final_state = None

            async def consume():
                nonlocal final_state
                async for mode, chunk in graph.astream(
                    {"query": case["query"]}, context=context,
                    stream_mode=["updates", "values", "custom"],
                ):
                    if mode == "updates":
                        for node in chunk:
                            report["completed_nodes"].append({"node": node, "elapsed_seconds": round(perf_counter() - started, 3)})
                            print(f"[{name}] completed: {node}", flush=True)
                    elif mode == "values":
                        final_state = chunk

            await asyncio.wait_for(consume(), timeout=timeout)
            if final_state is None:
                raise RuntimeError("工作流未返回最终状态")
            report.update({
                "generated_sql": final_state.get("sql"),
                "actual": final_state.get("sql_result"),
                "error": final_state.get("error"),
                "correction_attempts": final_state.get("sql_correction_attempts", 0),
                "recall_counts": {key: len(final_state.get(key, [])) for key in
                                  ("retrieved_column_infos", "retrieved_metric_infos", "retrieved_value_infos")},
                "selected_tables": [table["name"] for table in final_state.get("table_infos", [])],
                "selected_metrics": [metric["name"] for metric in final_state.get("metric_infos", [])],
            })
            report["passed"] = report["error"] is None and results_match(report["actual"], report["expected"])
    except Exception as error:
        message = f"{type(error).__name__}: {error}"
        key = os.environ.get("OPENAI_API_KEY")
        report["error"] = message.replace(key, "[REDACTED]") if key else message
    report["elapsed_seconds"] = round(perf_counter() - started, 3)
    print(f"[{name}] {'PASS' if report['passed'] else 'FAIL'} ({report['elapsed_seconds']}s)", flush=True)
    return report


async def check(names, timeout, report_path):
    reports = []
    report_path.parent.mkdir(parents=True, exist_ok=True)
    for name in names:
        reports.append(await run_case(name, timeout))
        report_path.write_text(json.dumps(reports, ensure_ascii=False, indent=2), encoding="utf-8")
    passed = sum(item["passed"] for item in reports)
    print(f"\nPassed {passed}/{len(reports)}; report: {report_path.resolve()}")
    return passed == len(reports)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=list(CASES), action="append", help="可重复指定；默认运行全部用例")
    parser.add_argument("--timeout", type=float, default=180, help="单个工作流的等待秒数")
    parser.add_argument("--report", type=Path, default=Path(".local/e2e_report.json"))
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout 必须大于 0")
    raise SystemExit(0 if asyncio.run(check(args.case or list(CASES), args.timeout, args.report)) else 1)
