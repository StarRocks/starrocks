#!/usr/bin/env python3
"""
Generate Avro C++ reader test files.

Examples:
  gen_avro_file.py --multiblock
  gen_avro_file.py --direct-reader-matrix
  gen_avro_file.py --all --output-dir be/test/formats/test_data/avro/cpp
"""
import argparse
import json
import io
import os

import avro.schema
import avro.datafile
import avro.io

MULTIBLOCK_SCHEMA_JSON = json.dumps({
    "type": "record",
    "name": "SplitTestRecord",
    "fields": [
        {"name": "id",    "type": "int"},
        {"name": "name",  "type": "string"}
    ]
})

DIRECT_READER_MATRIX_SCHEMA_JSON = json.dumps({
    "type": "record",
    "name": "DirectReaderMatrixRecord",
    "fields": [
        {"name": "bool_from_int", "type": "int"},
        {"name": "bool_from_long", "type": "long"},
        {"name": "bool_from_float", "type": "float"},
        {"name": "bool_from_double", "type": "double"},
        {"name": "int_from_bool", "type": "boolean"},
        {"name": "enum_as_string", "type": {
            "type": "enum",
            "name": "Color",
            "symbols": ["RED", "BLUE"]
        }},
        {"name": "fixed_as_string", "type": {
            "type": "fixed",
            "name": "Fixed4",
            "size": 4
        }},
        {"name": "nullable_int", "type": ["null", "int"], "default": None}
    ]
})


def write_records(schema_json, records, out_path, flush_every=None):
    schema = avro.schema.parse(schema_json)
    buf = io.BytesIO()
    writer = avro.datafile.DataFileWriter(buf, avro.io.DatumWriter(), schema)
    for i, record in enumerate(records):
        writer.append(record)
        if flush_every is not None and (i + 1) % flush_every == 0:
            writer.flush()
    writer.flush()

    raw = buf.getvalue()
    with open(out_path, "wb") as f:
        f.write(raw)

    reader = avro.datafile.DataFileReader(io.BytesIO(raw), avro.io.DatumReader())
    read_back = list(reader)
    reader.close()
    assert read_back == records, f"Read-back mismatch for {out_path}"
    print(f"Written {len(raw)} bytes to {out_path}; verified {len(read_back)} records")


def write_multiblock(out_path):
    records = [{"id": i, "name": f"name_{i}"} for i in range(100)]
    write_records(MULTIBLOCK_SCHEMA_JSON, records, out_path, flush_every=10)


def write_direct_reader_matrix(out_path):
    records = [
        {
            "bool_from_int": 1,
            "bool_from_long": 0,
            "bool_from_float": 2.5,
            "bool_from_double": 0.0,
            "int_from_bool": True,
            "enum_as_string": "BLUE",
            "fixed_as_string": b"ABCD",
            "nullable_int": None,
        },
        {
            "bool_from_int": 0,
            "bool_from_long": 9,
            "bool_from_float": 0.0,
            "bool_from_double": 3.25,
            "int_from_bool": False,
            "enum_as_string": "RED",
            "fixed_as_string": b"WXYZ",
            "nullable_int": 7,
        },
    ]
    write_records(DIRECT_READER_MATRIX_SCHEMA_JSON, records, out_path)


def parse_args():
    parser = argparse.ArgumentParser(description="Generate Avro C++ reader test files.")
    parser.add_argument("--output-dir", default=os.path.dirname(__file__),
                        help="Directory for generated Avro files.")
    parser.add_argument("--multiblock", action="store_true",
                        help="Generate multiblock.avro for split boundary tests.")
    parser.add_argument("--direct-reader-matrix", action="store_true",
                        help="Generate direct_reader_matrix.avro for direct scalar conversion tests.")
    parser.add_argument("--all", action="store_true", help="Generate every Avro test file.")
    parser.add_argument("legacy_multiblock_output", nargs="?",
                        help="Deprecated compatibility path for generating only multiblock.avro.")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.legacy_multiblock_output:
        write_multiblock(args.legacy_multiblock_output)
        return

    generate_multiblock = args.all or args.multiblock
    generate_direct_reader_matrix = args.all or args.direct_reader_matrix
    if not generate_multiblock and not generate_direct_reader_matrix:
        generate_multiblock = True

    if generate_multiblock:
        write_multiblock(os.path.join(args.output_dir, "multiblock.avro"))
    if generate_direct_reader_matrix:
        write_direct_reader_matrix(os.path.join(args.output_dir, "direct_reader_matrix.avro"))


if __name__ == "__main__":
    main()
