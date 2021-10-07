from pyspark.sql import SparkSession

from main.base import PySparkJobInterface


class PySparkJob(PySparkJobInterface):

    def __init__(self):
        super().__init__()

    def init_spark_session(self) -> SparkSession:
        return SparkSession\
            .builder\
            .appName("Covid 19 Vacctionation Progress")\
            .getOrCreate()

