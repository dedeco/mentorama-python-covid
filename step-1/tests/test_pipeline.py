from pyspark.sql import SparkSession

from main.job.pipeline import PySparkJob

job = PySparkJob()


def test_init_spark_session():
    assert isinstance(job.spark, SparkSession)
