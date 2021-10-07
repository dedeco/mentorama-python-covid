from pyspark.sql.functions import col, count, udf
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession, DataFrame

from main.base import PySparkJobInterface
from dateutil import parser


class PySparkJob(PySparkJobInterface):

    def __init__(self):
        super().__init__()

    def init_spark_session(self) -> SparkSession:
        return SparkSession \
            .builder \
            .appName("Covid 19 Vacctionation Progress") \
            .getOrCreate()

    def count_available_vaccines(self, vaccines: DataFrame) -> int:
        return vaccines \
            .groupBy(col('vaccines')) \
            .agg(count('vaccines').alias('qty')) \
            .count()

    def find_earliest_used_vaccine(self, vaccines: DataFrame) -> str:
        def user_defined_string_to_date(date_col):
            return parser.parse(date_col)

        user_defined_string_to_date_udf = udf(user_defined_string_to_date, DateType())

        vaccines = vaccines.withColumn('date', user_defined_string_to_date_udf('date'))

        return vaccines \
            .select(col('vaccines')) \
            .orderBy(vaccines.date.asc()) \
            .first() \
            .vaccines