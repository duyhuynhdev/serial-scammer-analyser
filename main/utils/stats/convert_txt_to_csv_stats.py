import os
import csv

# Intput and output directories
TXT_DIR = "pancake/data/txt"
CSV_DIR = "pancake/data/csv"

# Define the header
HEADER = ["pool_address", "y", "x", "z", "true_profit", "naive_profit"]


def convert_txt_to_csv_stats(txt_file_path, csv_file_path):
    with open(txt_file_path, "r") as txt_file, open(
        csv_file_path, "w", newline=""
    ) as csv_file:
        writer = csv.writer(csv_file)

        writer.writerow(HEADER)

        row = []
        for line in txt_file:
            if "Pool address" in line:
                row.append(line.strip().split()[-1])
            if "Value of y" in line or "Value of x" in line or "Value of z" in line:
                row.append(float(line.strip().split()[-1]))

            if len(row) == 4:
                row.append(row[1] - row[2] - row[3])
                row.append(row[1] - row[2])
                writer.writerow(row)
                row = []


def main():
    for filename in os.listdir(TXT_DIR):
        if filename.endswith(".txt"):
            txt_file_path = os.path.join(TXT_DIR, filename)
            csv_file_name = filename.replace(".txt", ".csv")
            csv_file_path = os.path.join(CSV_DIR, csv_file_name)
            convert_txt_to_csv_stats(txt_file_path, csv_file_path)


if __name__ == "__main__":
    main()
