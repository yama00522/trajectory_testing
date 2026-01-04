from threading import Thread
# from csv_creator import CsvCreator
from csv_merger import CsvMerger


def main():
    # csv_creator = CsvCreator("./output_csv")

    merger = CsvMerger(
        input_dir="./output_csv",
        output_dir="./merged_csv",
        num_files=20,
    )

    # Thread(target=csv_creator.run, daemon=True).start()
    Thread(target=merger.run, daemon=True).start()

    while True:
        pass


if __name__ == "__main__":
    main()
