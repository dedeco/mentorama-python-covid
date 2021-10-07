import abc

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class PySparkJobInterface(abc.ABC):

    def __init__(self):
        self.spark = self.init_spark_session()

    @abc.abstractmethod
    def init_spark_session(self) -> SparkSession:
        raise NotImplementedError

    def read_csv(self, input_path: str) -> DataFrame:
        return self.spark\
            .read\
            .options(header=True, inferSchema=True)\
            .csv(input_path)

    def stop(self) -> None:
        self.spark.stop()