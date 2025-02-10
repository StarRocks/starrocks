import argparse
import csv
import re
import sys

def parse_log_to_csv(log_file):
    """
    Parses the log file and converts it into a CSV file.
    
    :param log_file: Path to the log file.
    :param output_csv: Path to the output CSV file.
    """
    # Define the regex patterns to extract key-value pairs and MAPE values
    entry_pattern = re.compile(
        r"predict_value:\s*(\w+),\s*dataset:\s*([\w.]+),\s*transform:\s*(\w+),\s*testset:\s*([\w.]+),\s*eval:\s*([\w.]+)"
    )
    mape_pattern = re.compile(r"MAPE on test set:\s*([\d.]+)%")
    
    # Prepare a list to hold parsed rows
    rows = []
    
    with open(log_file, 'r') as file:
        current_entry = None
        for line in file:
            # Match the main entry
            entry_match = entry_pattern.match(line)
            if entry_match:
                # Start a new entry
                current_entry = {
                    "predict_value": entry_match.group(1),
                    "trainset": entry_match.group(2),
                    "transform": entry_match.group(3),
                    "testset": entry_match.group(4),
                    "eval": entry_match.group(5),
                    "MAPE_train": None,
                    "MAPE_test": None,
                }
                rows.append(current_entry)
            else:
                # Match MAPE values for the current entry
                mape_match = mape_pattern.search(line)
                if mape_match and current_entry:
                    if current_entry["MAPE_train"] is None:
                        current_entry["MAPE_train"] = mape_match.group(1)
                    else:
                        current_entry["MAPE_test"] = mape_match.group(1)
    
    # Write to stdout
    fieldnames = ["predict_value", "trainset", "transform", "eval", "testset", "MAPE_train", "MAPE_test"]
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

parser = argparse.ArgumentParser(description='Parse log file and convert it into a CSV file.')
parser.add_argument('--log_file', type=str, help='Path to the log file.')
args = parser.parse_args()

parse_log_to_csv(args.log_file)