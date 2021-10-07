import pytest
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

sample2 = [
    ['Italy', '3/1/21', 123211.0, 'Moderna, Pfizer/BioNTech'],
    ['Cayman Islands', '9/1/21', 0.0, 'Pfizer/BioNTech'],
    ['Northern Ireland', '29/12/20', 0.0, 'Oxford/AstraZeneca, Pfizer/BioNTech'],
    ['Costa Rica', '18/1/21', 29389.0, 'Pfizer/BioNTech'],
    ['Luxembourg', '11/1/21', 1943.0, 'Pfizer/BioNTech']
]

sample3 = [
    ['Slovenia', '25/1/21', 61679.0, 'Pfizer/BioNTech'],
    ['Iceland', '27/1/21', 15522.0, 'Moderna, Pfizer/BioNTech'],
    ['Spain', '25/1/21', 1291216.0, 'Moderna, Pfizer/BioNTech'],
    ['Romania', '13/1/21', 154268.0, 'Pfizer/BioNTech'],
    ['Austria', '23/1/21', 136348.0, 'Pfizer/BioNTech'],
    ['Canada', '8/1/21', 0.0, 'Moderna, Pfizer/BioNTech'],
    ['Austria', '17/1/21', 55450.0, 'Pfizer/BioNTech'],
    ['United States', '9/1/21', 0.0, 'Moderna, Pfizer/BioNTech'],
    ['Romania', '18/1/21', 235239.0, 'Pfizer/BioNTech'],
    ['Scotland', '28/12/20', 0.0, 'Oxford/AstraZeneca, Pfizer/BioNTech']
]


def create_sample(sample):
    return job.spark.createDataFrame(data=sample, schema=schema)


@pytest.mark.filterwarnings("ignore")
def test_init_spark_session():
    assert isinstance(job.spark, SparkSession)


@pytest.mark.filterwarnings("ignore:DeprecationWarning")
def test_count_available_vaccines():
    assert job.count_available_vaccines(create_sample(sample1)) == 3


def test_which_earliest_used_vaccine():
    assert job.find_earliest_used_vaccine(create_sample(sample1)) == "Sputnik V"
    assert job.find_earliest_used_vaccine(create_sample(sample2)) == "Oxford/AstraZeneca, Pfizer/BioNTech"
    assert job.find_earliest_used_vaccine(create_sample(sample3)) == "Oxford/AstraZeneca, Pfizer/BioNTech"


def test_total_vaccinations_per_country():
    total_vaccines = job.total_vaccinations_per_country(create_sample(sample1)).rdd.collectAsMap()
    assert total_vaccines == {"Russia": 800000, "US": 10000, "UK": 1407251}

    total_vaccinations = job.total_vaccinations_per_country(create_sample(sample2)).rdd.collectAsMap()
    assert total_vaccinations == {'Costa Rica': 29389.0, 'Italy': 123211.0, 'Luxembourg': 1943.0}

    total_vaccinations = job.total_vaccinations_per_country(create_sample(sample3)).rdd.collectAsMap()
    assert total_vaccinations == {'Austria': 191798.0, 'Iceland': 15522.0, 'Romania': 389507.0, 'Slovenia': 61679.0,
                                  'Spain': 1291216.0}