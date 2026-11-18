import unittest

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from app.entities.column_info import ColumnInfo
from app.entities.column_metric import ColumnMetric
from app.entities.metric_info import MetricInfo
from app.entities.table_info import TableInfo
from app.models.base import Base
from app.models.column_info import ColumnInfoMySQL
from app.models.column_metric import ColumnMetricMySQL
from app.models.metric_info import MetricInfoMySQL
from app.models.table_info import TableInfoMySQL
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository


class SessionAdapter:
    """Exercise real ORM merge behavior without a database server or async driver."""

    def __init__(self, session):
        self.session = session

    async def merge(self, model):
        return self.session.merge(model)

    async def get(self, model, key):
        return self.session.get(model, key)

    async def execute(self, statement):
        return self.session.execute(statement)


def complete(coroutine):
    # The adapter performs synchronous SQLite I/O, so no event loop is needed.
    try:
        coroutine.send(None)
    except StopIteration as completed:
        return completed.value
    finally:
        coroutine.close()
    raise AssertionError("Unexpected asynchronous suspension")


class MetaMySQLRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        Base.metadata.create_all(self.engine)
        self.session = Session(self.engine)
        self.repository = MetaMySQLRepository(SessionAdapter(self.session))

    def tearDown(self):
        self.session.close()
        self.engine.dispose()

    def save(self, description):
        complete(self.repository.save_table_infos([
            TableInfo("orders", "orders", "fact", description)
        ]))
        complete(self.repository.save_column_infos([
            ColumnInfo("orders_amount", "amount", "float", "measure",
                       [12.5], description, ["sales"], "orders")
        ]))
        complete(self.repository.save_metric_infos([
            MetricInfo("GMV", "GMV", description, ["orders_amount"], ["sales"])
        ]))
        complete(self.repository.save_column_metrics([
            ColumnMetric("orders_amount", "GMV")
        ]))

    def test_repeated_save_updates_existing_rows(self):
        with self.session.begin():
            self.save("original")
        with self.session.begin():
            self.save("updated")

        for model in (TableInfoMySQL, ColumnInfoMySQL, MetricInfoMySQL,
                      ColumnMetricMySQL):
            self.assertEqual(self.session.scalar(
                select(func.count()).select_from(model)), 1)
        for model, key in ((TableInfoMySQL, "orders"),
                           (ColumnInfoMySQL, "orders_amount"),
                           (MetricInfoMySQL, "GMV")):
            self.assertEqual(self.session.get(model, key).description, "updated")
        self.assertIsNotNone(self.session.get(
            ColumnMetricMySQL, ("orders_amount", "GMV")))

    def test_save_keeps_transaction_under_callers_control(self):
        with self.assertRaisesRegex(RuntimeError, "abort"):
            with self.session.begin():
                self.save("rolled back")
                raise RuntimeError("abort")
        for model in (TableInfoMySQL, ColumnInfoMySQL, MetricInfoMySQL,
                      ColumnMetricMySQL):
            self.assertEqual(self.session.scalar(
                select(func.count()).select_from(model)), 0)

    def test_empty_batches_are_noops(self):
        for method in (self.repository.save_table_infos,
                       self.repository.save_column_infos,
                       self.repository.save_metric_infos,
                       self.repository.save_column_metrics):
            complete(method([]))
        self.assertFalse(self.session.in_transaction())

    def test_column_lookup_accepts_legacy_and_dotted_references(self):
        with self.session.begin():
            self.save("original")
        legacy = complete(self.repository.get_column_info_by_id("orders_amount"))
        dotted = complete(self.repository.get_column_info_by_id("orders.amount"))
        self.assertEqual(legacy, dotted)
        self.assertEqual(dotted.id, "orders_amount")
        self.assertEqual(dotted.table_id, "orders")
        self.assertEqual(dotted.name, "amount")
        self.assertIsNone(complete(self.repository.get_column_info_by_id("orders.missing")))
        self.assertIsNone(complete(self.repository.get_column_info_by_id("missing_legacy_id")))
        self.assertIsNone(complete(self.repository.get_column_info_by_id("orders.amount' OR 1=1 --")))

    def test_exact_column_id_takes_precedence_over_legacy_row(self):
        with self.session.begin():
            self.save("legacy")
            complete(self.repository.save_column_infos([
                ColumnInfo("orders.amount", "amount", "float", "measure",
                           [], "canonical", [], "orders")
            ]))
        column = complete(self.repository.get_column_info_by_id("orders.amount"))
        self.assertEqual(column.description, "canonical")

    def test_ambiguous_natural_column_lookup_fails_clearly(self):
        with self.session.begin():
            complete(self.repository.save_column_infos([
                ColumnInfo(identifier, "amount", "float", "measure", [], "", [], "orders")
                for identifier in ("legacy-a", "legacy-b")
            ]))
        with self.assertRaisesRegex(ValueError, "对应多条元数据"):
            complete(self.repository.get_column_info_by_id("orders.amount"))

    def test_table_lookup_returns_entity_or_none(self):
        with self.session.begin():
            self.save("Orders")
        self.assertEqual(complete(self.repository.get_table_info_by_id("orders")),
                         TableInfo("orders", "orders", "fact", "Orders"))
        self.assertIsNone(complete(self.repository.get_table_info_by_id("missing")))

    def test_key_lookup_is_scoped_to_table_and_key_roles(self):
        with self.session.begin():
            self.save("Orders")
            complete(self.repository.save_column_infos([
                ColumnInfo("orders_id", "id", "int", "primary_key", [], "", [], "orders"),
                ColumnInfo("orders_region_id", "region_id", "int", "foreign_key", [], "", [], "orders"),
                ColumnInfo("regions_id", "id", "int", "primary_key", [], "", [], "regions"),
            ]))
        columns = complete(self.repository.get_key_columns_by_table_id("orders"))
        self.assertEqual([column.name for column in columns], ["id", "region_id"])
        self.assertTrue(all(column.table_id == "orders" for column in columns))
        self.assertEqual(complete(self.repository.get_key_columns_by_table_id("missing")), [])


if __name__ == "__main__":
    unittest.main()
