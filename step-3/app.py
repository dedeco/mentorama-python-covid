import sys

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

    job.stop()


if __name__ == "__main__":
    main()
