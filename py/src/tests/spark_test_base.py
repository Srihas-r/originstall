import logging
import unittest

from pyspark.sql import SparkSession


class PySparkTest(unittest.TestCase):
    @classmethod
    def create_testing_pyspark_session(self):
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("dslib-testing-pyspark-context")
            .enableHiveSupport()
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark

    @classmethod
    def setUpClass(self):
        self.spark = self.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(self):
        self.spark.stop()

    @classmethod
    def create_spark_df(self, data, cols):
        df = self.spark.createDataFrame(data, cols)
        return df
