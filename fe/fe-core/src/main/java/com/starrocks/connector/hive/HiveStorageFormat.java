// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.hive;

import com.starrocks.connector.exception.StarRocksConnectorException;

import java.util.Map;

import static com.starrocks.connector.hive.HiveClassNames.AVRO_INPUT_FORMAT_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.AVRO_OUTPUT_FORMAT_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.AVRO_SERDE_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.COLUMNAR_SERDE_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.LAZY_BINARY_COLUMNAR_SERDE_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.LAZY_SIMPLE_SERDE_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.MAPRED_PARQUET_INPUT_FORMAT_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.ORC_INPUT_FORMAT_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.ORC_OUTPUT_FORMAT_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.ORC_SERDE_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.RCFILE_INPUT_FORMAT_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.RCFILE_OUTPUT_FORMAT_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.SEQUENCE_INPUT_FORMAT_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.SEQUENCE_OUTPUT_FORMAT_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.TEXT_INPUT_FORMAT_CLASS;
import static com.starrocks.connector.hive.HiveMetastoreOperations.FILE_FORMAT;
import static java.util.Objects.requireNonNull;

public enum HiveStorageFormat {
    ORC(
            ORC_SERDE_CLASS,
            ORC_INPUT_FORMAT_CLASS,
            ORC_OUTPUT_FORMAT_CLASS),
    PARQUET(
            PARQUET_HIVE_SERDE_CLASS,
            MAPRED_PARQUET_INPUT_FORMAT_CLASS,
            MAPRED_PARQUET_OUTPUT_FORMAT_CLASS),
    TEXTFILE(
            LAZY_SIMPLE_SERDE_CLASS,
            TEXT_INPUT_FORMAT_CLASS,
            HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS),
    AVRO(
            AVRO_SERDE_CLASS,
            AVRO_INPUT_FORMAT_CLASS,
            AVRO_OUTPUT_FORMAT_CLASS
    ),
    RCBINARY(
            LAZY_BINARY_COLUMNAR_SERDE_CLASS,
            RCFILE_INPUT_FORMAT_CLASS,
            RCFILE_OUTPUT_FORMAT_CLASS
    ),
    RCTEXT(
            COLUMNAR_SERDE_CLASS,
            RCFILE_INPUT_FORMAT_CLASS,
            RCFILE_OUTPUT_FORMAT_CLASS
    ),
    SEQUENCE(
            LAZY_SIMPLE_SERDE_CLASS,
            SEQUENCE_INPUT_FORMAT_CLASS,
            SEQUENCE_OUTPUT_FORMAT_CLASS
    ),
    UNSUPPORTED("UNSUPPORTED", "UNSUPPORTED", "UNSUPPORTED");

    private final String serde;
    private final String inputFormat;
    private final String outputFormat;

    public static HiveStorageFormat get(String format) {
        for (HiveStorageFormat storageFormat : HiveStorageFormat.values()) {
            if (storageFormat.name().equalsIgnoreCase(format)) {
                return storageFormat;
            }
        }
        return UNSUPPORTED;
    }

    public static void check(Map<String, String> properties) {
        if (properties.containsKey("format") && !properties.containsKey(FILE_FORMAT)) {
            throw new StarRocksConnectorException(
                    "Please use 'file_format' instead of 'format' in the table properties");
        }

        String fileFormat = properties.getOrDefault(FILE_FORMAT, "parquet");
        HiveStorageFormat storageFormat = get(fileFormat);
        if (storageFormat == UNSUPPORTED) {
            throw new StarRocksConnectorException("Unsupported hive storage format %s", fileFormat);
        }
    }

    HiveStorageFormat(String serde, String inputFormat, String outputFormat) {
        this.serde = requireNonNull(serde, "serde is null");
        this.inputFormat = requireNonNull(inputFormat, "inputFormat is null");
        this.outputFormat = requireNonNull(outputFormat, "outputFormat is null");
    }

    public String getSerde() {
        return serde;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public String getOutputFormat() {
        return outputFormat;
    }
}
