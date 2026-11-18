import unittest

from pydantic import ValidationError

from app.api.schemas.query_schema import QuerySchema


class QuerySchemaTests(unittest.TestCase):
    def test_trims_question_without_mutating_request_data(self):
        data = {"query": " \n统计华北地区的销售总额\t "}
        request = QuerySchema.model_validate(data)
        self.assertEqual(request.query, "统计华北地区的销售总额")
        self.assertEqual(request.model_dump(), {"query": "统计华北地区的销售总额"})
        self.assertEqual(data["query"], " \n统计华北地区的销售总额\t ")

    def test_preserves_internal_spaces_and_line_breaks(self):
        question = "统计 A 品牌的销售额\n按大区分组"
        self.assertEqual(QuerySchema(query=question).query, question)

    def test_missing_question_is_rejected(self):
        with self.assertRaises(ValidationError) as caught:
            QuerySchema.model_validate({})
        self.assertEqual(caught.exception.errors()[0]["loc"], ("query",))

    def test_empty_or_whitespace_only_question_is_rejected(self):
        for question in ("", " ", "\t\r\n", "\u3000"):
            with self.subTest(question=question), self.assertRaises(ValidationError):
                QuerySchema(query=question)

    def test_non_string_values_are_rejected(self):
        for question in (None, 123, True, [], {}, b"question"):
            with self.subTest(question=question), self.assertRaises(ValidationError):
                QuerySchema(query=question)

    def test_unknown_fields_are_rejected(self):
        with self.assertRaises(ValidationError) as caught:
            QuerySchema.model_validate({"query": "统计销售总额", "sql": "SELECT 1"})
        self.assertEqual(caught.exception.errors()[0]["loc"], ("sql",))


if __name__ == "__main__":
    unittest.main()
