import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StructType
from pyspark.sql import SparkSession, DataFrame

from main.base import PySparkJobInterface
from dateutil import parser


class PySparkJob(PySparkJobInterface):

    def __init__(self):
        super().__init__()

    def init_spark_session(self) -> SparkSession:
        return SparkSession \
            .builder \
            .appName("Covid 19 Vaccionation Progress") \
            .getOrCreate()

    def count_available_vaccines(self, vaccines: DataFrame) -> int:
        return vaccines \
            .groupBy(F.col('vaccines')) \
            .agg(F.count('vaccines').alias('qty')) \
            .count()

    def find_earliest_used_vaccine(self, vaccines: DataFrame) -> str:
        def user_defined_string_to_date(date_col):
            return parser.parse(date_col)

        user_defined_string_to_date_udf = F.udf(user_defined_string_to_date, DateType())

        vaccines = vaccines.withColumn('date', user_defined_string_to_date_udf('date'))

        return vaccines \
            .select(F.col('vaccines')) \
            .orderBy(vaccines.date.asc()) \
            .first() \
            .vaccines

    def total_vaccinations_per_country(self, vaccines: DataFrame) -> DataFrame:
        empty = self.spark.createDataFrame([], StructType([]))
        return vaccines\
        .groupBy(F.col('country'))\
        .agg(F.sum('total_vaccinations').alias('sum_total_vaccinations'))\
        .filter(F.col('sum_total_vaccinations') > 0.0)