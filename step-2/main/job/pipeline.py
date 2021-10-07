from pyspark.sql.functions import col,count
from pyspark.sql import SparkSession, DataFrame

from main.base import PySparkJobInterface


class PySparkJob(PySparkJobInterface):

    def __init__(self):
        super().__init__()

    def init_spark_session(self) -> SparkSession:
        return SparkSession\
            .builder\
            .appName("Covid 19 Vacctionation Progress")\
            .getOrCreate()

    def count_available_vaccines(self, vaccines: DataFrame) -> int:
        return vaccines\
            .groupBy(col('vaccines'))\
            .agg(count('vaccines').alias('qty'))\
            .count()


