import io
import os
import unittest
from contextlib import redirect_stdout
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from langgraph.graph import END, START, StateGraph
from sqlalchemy import create_engine, event
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

# All external services are replaced below; no real credential is used.
with patch.dict(os.environ, {"OPENAI_API_KEY": "offline-test-key"}):
    import app.agent.graph as graph_module
    import app.agent.nodes.add_extra_context as context_node
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository


class ExtraContextTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.db_info = {"dialect": "mysql", "version": "8.0.41-test"}
        self.repository = SimpleNamespace(get_db_info=AsyncMock(return_value=self.db_info))
        self.events = []
        self.runtime = SimpleNamespace(
            context={"dw_mysql_repository": self.repository},
            stream_writer=self.events.append,
        )
        self.state = {"query": "统计去年的销售总额", "table_infos": [], "metric_infos": []}
        logger_patch = patch.object(context_node, "logger")
        logger_patch.start()
        self.addCleanup(logger_patch.stop)

    async def invoke_at(self, instant):
        with patch.object(context_node, "datetime") as clock:
            clock.now.side_effect = lambda tz: instant.astimezone(tz)
            result = await context_node.add_extra_context(self.state, self.runtime)
            clock.now.assert_called_once_with(context_node.BEIJING_TIMEZONE)
        return result

    async def test_adds_date_and_database_info_without_changing_input(self):
        before = deepcopy(self.state)
        result = await self.invoke_at(datetime(2026, 11, 16, 2, tzinfo=timezone.utc))
        self.assertEqual(result["date_info"], {
            "date": "2026-11-16", "weekday": "星期一", "quarter": "Q4",
            "timezone": "Asia/Shanghai",
        })
        self.assertEqual(result["db_info"], self.db_info)
        self.repository.get_db_info.assert_awaited_once_with()
        self.assertEqual(self.state, before)
        self.assertEqual([event["status"] for event in self.events], ["running", "success"])

    async def test_uses_beijing_date_when_utc_is_still_the_previous_day(self):
        result = await self.invoke_at(datetime(2026, 11, 15, 16, 30, tzinfo=timezone.utc))
        self.assertEqual(result["date_info"]["date"], "2026-11-16")
        self.assertEqual(result["date_info"]["weekday"], "星期一")

    async def test_quarter_and_year_rollovers_use_the_same_business_clock(self):
        cases = [
            (datetime(2026, 3, 31, 16, 5, tzinfo=timezone.utc), "2026-04-01", "Q2"),
            (datetime(2026, 12, 31, 16, 5, tzinfo=timezone.utc), "2027-01-01", "Q1"),
        ]
        for instant, date, quarter in cases:
            with self.subTest(date=date):
                result = await self.invoke_at(instant)
                self.assertEqual(result["date_info"]["date"], date)
                self.assertEqual(result["date_info"]["quarter"], quarter)

    async def test_all_months_have_the_expected_quarter(self):
        expected = ["Q1", "Q1", "Q1", "Q2", "Q2", "Q2",
                    "Q3", "Q3", "Q3", "Q4", "Q4", "Q4"]
        for month, quarter in enumerate(expected, start=1):
            result = await self.invoke_at(datetime(2026, month, 15, tzinfo=timezone.utc))
            self.assertEqual(result["date_info"]["quarter"], quarter)

    async def test_weekdays_are_stable_chinese_labels(self):
        start = datetime(2026, 11, 16, tzinfo=timezone.utc)
        for offset, weekday in enumerate(
            ("星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日")
        ):
            result = await self.invoke_at(start + timedelta(days=offset))
            self.assertEqual(result["date_info"]["weekday"], weekday)

    async def test_database_failure_is_reported_without_partial_state(self):
        before = deepcopy(self.state)
        self.repository.get_db_info.side_effect = RuntimeError("database unavailable")
        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            await self.invoke_at(datetime(2026, 11, 16, tzinfo=timezone.utc))
        self.assertEqual(self.events[-1]["status"], "error")
        self.assertEqual(self.state, before)

    async def test_graph_saves_extra_context_and_keeps_filtered_state(self):
        builder = StateGraph(context_node.DataAgentState,
                             context_schema=context_node.DataAgentContext)
        builder.add_node("add_extra_context", context_node.add_extra_context)
        builder.add_edge(START, "add_extra_context")
        builder.add_edge("add_extra_context", END)
        with patch.object(context_node, "datetime") as clock:
            clock.now.return_value = datetime(2026, 11, 16, tzinfo=context_node.BEIJING_TIMEZONE)
            result = await builder.compile().ainvoke(
                self.state, context=self.runtime.context
            )
        self.assertEqual(result["date_info"]["date"], "2026-11-16")
        self.assertEqual(result["db_info"], self.db_info)
        self.assertEqual(result["table_infos"], self.state["table_infos"])
        self.assertEqual(result["metric_infos"], self.state["metric_infos"])


class DWDatabaseInfoTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.version = "8.0.41-test"
        self.statements = []
        self.engine = create_engine("sqlite://")

        @event.listens_for(self.engine, "connect")
        def install_version_function(connection, record):
            # SQLite has no VERSION(): emulate that function for the real SQL query.
            connection.create_function("VERSION", 0, lambda: self.version)

        @event.listens_for(self.engine, "before_cursor_execute")
        def record_statement(connection, cursor, statement, parameters, context, executemany):
            self.statements.append(statement)

        self.session = Session(self.engine)
        sync_session = self.session

        class SessionAdapter:
            async def execute(self, statement):
                return sync_session.execute(statement)

            def get_bind(self):
                return sync_session.get_bind()

        self.repository = DWMySQLRepository(SessionAdapter())

    def tearDown(self):
        self.session.close()
        self.engine.dispose()

    async def test_reads_version_and_actual_bound_dialect_with_read_only_sql(self):
        self.assertEqual(await self.repository.get_db_info(),
                         {"dialect": "sqlite", "version": "8.0.41-test"})
        self.assertEqual(self.statements, ["SELECT VERSION()"])

    async def test_missing_or_invalid_version_is_rejected(self):
        for version in (None, "", "   ", 123):
            with self.subTest(version=version):
                self.version = version
                with self.assertRaisesRegex(ValueError, "数据库版本信息"):
                    await self.repository.get_db_info()

    async def test_database_execution_error_is_propagated(self):
        self.version = object()  # SQLite cannot return this from a user function.
        with self.assertRaises(OperationalError):
            await self.repository.get_db_info()


class ExampleResourceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.trace = []
        self.contexts = []
        self.fail_stream = False
        self.sessions = {name: object() for name in ("meta", "dw")}
        self.managers = {}
        trace = self.trace

        class SessionContext:
            def __init__(self, name, session):
                self.name, self.session = name, session

            async def __aenter__(self):
                trace.append(f"open:{self.name}-session")
                return self.session

            async def __aexit__(self, *args):
                trace.append(f"close:{self.name}-session")

        for name in ("qdrant", "es", "meta", "dw"):
            self.managers[name] = SimpleNamespace(
                init=Mock(side_effect=lambda name=name: trace.append(f"init:{name}")),
                close=AsyncMock(side_effect=lambda name=name: trace.append(f"close:{name}")),
                client=object(),
            )
        for name in ("meta", "dw"):
            self.managers[name].session_factory = Mock(return_value=
                SessionContext(name, self.sessions[name]))

        async def stream(**kwargs):
            self.contexts.append(kwargs["context"])
            if self.fail_stream:
                raise RuntimeError("workflow failed")
            yield {"type": "progress", "status": "success"}

        replacements = {
            "qdrant_client_manager": self.managers["qdrant"],
            "es_client_manager": self.managers["es"],
            "meta_mysql_client_manager": self.managers["meta"],
            "dw_mysql_client_manager": self.managers["dw"],
            "embedding_client_manager": SimpleNamespace(
                init=Mock(), client=object(),
                close=AsyncMock(side_effect=lambda: trace.append("close:embedding")),
            ),
            "graph": SimpleNamespace(astream=stream),
        }
        manager_patch = patch.multiple(graph_module, **replacements)
        manager_patch.start()
        self.addCleanup(manager_patch.stop)

    async def test_example_provides_two_repositories_and_closes_sessions_first(self):
        with redirect_stdout(io.StringIO()):
            await graph_module.run_example()
        self.assertIs(self.contexts[0]["meta_mysql_repository"].session, self.sessions["meta"])
        self.assertIs(self.contexts[0]["dw_mysql_repository"].session, self.sessions["dw"])
        self.assertEqual(self.trace[-7:], [
            "close:dw-session", "close:meta-session", "close:embedding", "close:dw", "close:meta",
            "close:es", "close:qdrant",
        ])

    async def test_workflow_failure_still_closes_sessions_and_clients(self):
        self.fail_stream = True
        with self.assertRaisesRegex(RuntimeError, "workflow failed"):
            await graph_module.run_example()
        for manager in self.managers.values():
            manager.close.assert_awaited_once()
        self.assertIn("close:dw-session", self.trace)
        self.assertIn("close:meta-session", self.trace)

    async def test_initialization_failure_closes_previously_created_clients(self):
        self.managers["dw"].init.side_effect = RuntimeError("initialization failed")
        with self.assertRaisesRegex(RuntimeError, "initialization failed"):
            await graph_module.run_example()
        for name in ("qdrant", "es", "meta"):
            self.managers[name].close.assert_awaited_once()
        self.managers["dw"].close.assert_not_awaited()
        self.managers["meta"].session_factory.assert_not_called()


if __name__ == "__main__":
    unittest.main()
