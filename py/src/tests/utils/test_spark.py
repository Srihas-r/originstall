from pyspark.sql import DataFrame

from dslib.utils import sparkutils

from tests.spark_test_base import PySparkTest


class TestSpark(PySparkTest):
    @classmethod
    def setUpClass(cls):
        cls.spark = cls.create_testing_pyspark_session()

        q = "create table default.dslib_pyspark_test1 (test string)"
        cls.spark.sql(q)
        q = "create table default.dslib_pyspark_test2 (test string)"
        cls.spark.sql(q)

    @classmethod
    def tearDownClass(cls):
        q = "drop table default.dslib_pyspark_test1"
        cls.spark.sql(q)
        q = "drop table default.dslib_pyspark_test2"
        cls.spark.sql(q)
        cls.spark.stop()

    def test_nulls(self):
        # null df
        df = self.spark.range(0).drop("id")
        expected = df.collect()
        out = sparkutils.nulls_count(df).collect()
        self.assertEqual(expected, out)

        # null df with percent
        df = self.spark.range(0).drop("id")
        expected = df.collect()
        out = sparkutils.nulls_count(df, percent=True).collect()
        self.assertEqual(expected, out)

        data = [
            ("A1", None, 1),
            ("A2", "C2", 2),
            (None, "C3", 3),
            ("A4", None, 4),
            ("A5", "C5", 5),
        ]
        cols = ["col1", "col2", "col3"]
        df = self.create_spark_df(data, cols)

        out = sparkutils.nulls_count(df)
        self.assertIsInstance(out, DataFrame)

        data = [["Count", 1, 2, 0]]
        cols = ["Metric", "col1", "col2", "col3"]
        expected = self.create_spark_df(data, cols).collect()
        out = sparkutils.nulls_count(df).collect()
        self.assertEqual(expected, out)

        data = [["Count", 1.0, 2.0, 0.0], ["Percent", 20.0, 40.0, 0.0]]
        cols = ["Metric", "col1", "col2", "col3"]
        expected = self.create_spark_df(data, cols).collect()
        out = sparkutils.nulls_count(df, percent=True).collect()
        self.assertEqual(expected, out)

    def test_table_search(self):
        expected_message = "Expected <class 'str'> object"
        with self.assertRaisesRegex(TypeError, expected_message):
            sparkutils.table_search("search", 12, self.spark)

        expected_message = "Input parameter cannot be empty."
        with self.assertRaisesRegex(ValueError, expected_message):
            sparkutils.table_search("search", "", self.spark)

        expected_message = "Expected <class 'str'> object"
        with self.assertRaisesRegex(TypeError, expected_message):
            sparkutils.table_search(12, "schema", self.spark)

        expected_message = "Input parameter cannot be empty."
        with self.assertRaisesRegex(ValueError, expected_message):
            sparkutils.table_search("", "schema", self.spark)

        expected_message = "Please provide the correct database name."
        with self.assertRaisesRegex(ValueError, expected_message):
            sparkutils.table_search("search", "dslib", self.spark)

        # regex query test 1
        out = sparkutils.table_search("dslib.*test", "default", self.spark)
        self.assertIsInstance(out, DataFrame)

        data = [["dslib_pyspark_test1"], ["dslib_pyspark_test2"]]
        cols = ["tableName"]
        expected = self.create_spark_df(data, cols).select("tableName").collect()
        out = out.select("tableName").collect()
        self.assertEqual(expected, out)

        # regex query test 2
        out = (
            sparkutils.table_search("dslib", "default", self.spark)
            .select("tableName")
            .collect()
        )
        self.assertEqual(expected, out)

        # regex query test 3
        data = [["dslib_pyspark_test1"]]
        cols = ["tableName"]
        expected = self.create_spark_df(data, cols).select("tableName").collect()
        out = (
            sparkutils.table_search("dslib.*1", "default", self.spark)
            .select("tableName")
            .collect()
        )
        self.assertEqual(expected, out)


if __name__ == "__main__":
    unittest.main()
