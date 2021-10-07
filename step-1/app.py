import sys

from main.job.pipeline import PySparkJob


def main():
    job = PySparkJob()

    # Load input data to Dataframe
    print("Reading data...")
    vaccines = job.read_csv(sys.argv[1])

    print(f"Size of dataframe {vaccines.count()}")

    job.stop()


if __name__ == "__main__":
    main()
