import csv
import sys

"""
Optimized parsing of fe.features.log into a CSV, handling vectors:
- vector_fields are expanded into multiple features
- onehot_fields are preserved for model training to prevent file explosion
"""

# example format
# 2025-01-15 13:00:33.884+08:00 [features] digest=1b64182e69992b9634e9e2bea7eee9b6
# |cpuCostNs=2786678871|memCostBytes=110637104|scanBytes=89842758|scanRows=4631290|returnRows=5
# |spilledBytes=0|time=500|state=EOF|catalog=default_catalog|database=tpcds_2
# |tables=[8036075,8037139,8037089]|env=[1,16,30923764531]
# |var=[0]|operators=[39,11,76,1558,56,116,0,14,40,3,41,1650,59,60,0,42,22,84,3,41,8,112,856,56,148,0,56,8,0,43,0,0,0,0,0,0,44,10,62,328,48,97,0,1000,1000,8,6,60,0,0,0,0,0,0,61,0,0,0,0,0,0,62,2,8,1356,22,31,200,63,1,14,678,0,0,0,64,0,0,0,0,0,0,65,0,0,0,0,0,0,66,0,0,0,0,0,0,67,0,0,0,0,0,0,68,0,0,0,0,0,0,69,0,0,0,0,0,0,70,0,0,0,0,0,0,71,0,0,0,0,0,0,72,0,0,0,0,0,0,73,0,0,0,0,0,0,74,0,0,0,0,0,0,75,0,0,0,0,0,0,76,0,0,0,0,0,0,77,0,0,0,0,0,0]

# Extract required fields as global variables
required_fields = ["digest", "cpuCostNs", "memCostBytes", "scanBytes", "scanRows", "returnRows", "spilledBytes", "time", "tables", "env", "var", "operators"]
vector_fields = ["tables", "env", "var", "operators"]

def parse_line(line):
    # Split the line into two parts: the prefix and the fields
    prefix, fields = line.split("] ", 1)
    fields = fields.split("|")
    # Extract fields into a map
    field_map = {}
    for field in fields:
        if field:  # Check if field is not empty
            key, value = field.split("=")
            field_map[key] = value

    if field_map.get("state") == "ERR":
        return None
        
    # Extract and reorder fields according to required_fields
    reordered_fields = []
    for field in required_fields:
        value = field_map.get(field, "")
        if field in vector_fields:
            value = [item for item in value.strip("[]").split(",")]
        reordered_fields.append(value)
    return reordered_fields


def process_file(input_file, output_file):
    """
    Process the input file and write the parsing result to a CSV file.
    """
    with open(input_file, "r") as infile, open(output_file, "w", newline="") as outfile:
        writer = csv.writer(outfile)
        header_written = False

        rows = 0
        for line in infile:
            rows += 1
            parsed = parse_line(line.strip())
            if parsed:
                # Write the header
                if not header_written:
                    # Modify header for vector fields to expand into multiple fields
                    modified_header = []
                    for field in required_fields:
                        if field in vector_fields:
                            # Use the length from the parsed line
                            max_length = len(parsed[required_fields.index(field)])
                            modified_header.extend([f"{field}_{i}" for i in range(max_length)])
                        else:
                            modified_header.append(field)
                    writer.writerow(modified_header)
                    header_written = True
                # Write the data
                # Flatten arrays in parsed data before writing
                flattened_parsed = []
                for item in parsed:
                    if isinstance(item, list):
                        flattened_parsed.extend(item)
                    else:
                        flattened_parsed.append(item)
                writer.writerow(flattened_parsed)
        print(f"Parsed {rows} rows")


# Main function
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python data_clean.py <input_file> <output_file>")
        sys.exit()
    input_file = sys.argv[1]  # Read the input file name from the command line
    output_file = sys.argv[2]  # Read the output file name from the command line

    print(f"Processing file: {input_file}")
    process_file(input_file, output_file)
    print(f"Data has been written to: {output_file}")
