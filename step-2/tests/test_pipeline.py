from pyspark.sql import SparkSession

from main.job.pipeline import PySparkJob

job = PySparkJob()
schema = ["country", "date", "total_vaccinations", "vaccines"]
sample1 = [
    ("UK", "3/1/21", 1397251, "Oxford/AstraZeneca, Pfizer/BioNTech"),
    ("UK", "9/1/21", 10000, "Oxford/AstraZeneca, Pfizer/BioNTech"),
    ("US", "9/1/21", 10000, "Moderna, Pfizer/BioNTech"),
    ("Russia", "2/1/21", 800000, "Sputnik V"),
]


def create_sample(sample):
    return job.spark.createDataFrame(data=sample, schema=schema)


def test_init_spark_session():
    assert isinstance(job.spark, SparkSession)


def test_count_available_vaccines():
    assert job.count_available_vaccines(create_sample(sample1)) == 3
