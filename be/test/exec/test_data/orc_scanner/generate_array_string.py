# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
import pyorc


with open("./array_string.orc", "wb") as data:
    with pyorc.Writer(data, "struct<col_int:int,col_array_string:array<string>>") as writer:
        writer.write((1, ('{"key1":1}', '{"key2":2}')))
        writer.write((2, ('{"key3":3}', '{"key4":4}')))