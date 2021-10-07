import sys
import pyspark.sql.functions as F

from main.job.pipeline import PySparkJob


def main():
    job = PySparkJob()

    # Load input data to Dataframe
    print("Reading data...")
    vaccines = job.read_csv(sys.argv[1])
    print(f"Size of dataframe {vaccines.count()}")

    print("Distinct Vaccines... (Qty)")
    nb_vaccines = job.count_available_vaccines(vaccines)
    print(nb_vaccines)

    print("Earliest used vaccine...")
    earliest_vacine = job.find_earliest_used_vaccine(vaccines)
    print(earliest_vacine)

    print("Total Vaccinations by country...")
    total_vaccination = job.total_vaccinations_per_country(vaccines)
    total_vaccination.sort(F.col("sum_total_vaccinations").desc()).show(truncate=False)
    job.stop()


if __name__ == "__main__":
    main()
