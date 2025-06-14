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

import com.google.common.collect.ImmutableMap;
import com.starrocks.thrift.THdfsFileFormat;

import static com.starrocks.connector.hive.HiveClassNames.AVRO_INPUT_FORMAT_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static com.starrocks.connector.hive.HiveClassNames.MAPRED_PARQUET_INPUT_FORMAT_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.ORC_INPUT_FORMAT_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.RCFILE_INPUT_FORMAT_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.SEQUENCE_INPUT_FORMAT_CLASS;
import static com.starrocks.connector.hive.HiveClassNames.TEXT_INPUT_FORMAT_CLASS;

public enum RemoteFileInputFormat {
    PARQUET,
    ORC,
    TEXTFILE,
    AVRO,
    RCBINARY,
    RCTEXT,
    SEQUENCE,
    UNKNOWN;
    private static final ImmutableMap<String, RemoteFileInputFormat> CLASS_NAME_TO_INPUT_FORMAT =
            new ImmutableMap.Builder<String, RemoteFileInputFormat>()
                    .put(MAPRED_PARQUET_INPUT_FORMAT_CLASS, PARQUET)
                    .put(ORC_INPUT_FORMAT_CLASS, ORC)
                    .put(TEXT_INPUT_FORMAT_CLASS, TEXTFILE)
                    .put(AVRO_INPUT_FORMAT_CLASS, AVRO)
                    .put(RCFILE_INPUT_FORMAT_CLASS, RCBINARY)
                    .put(SEQUENCE_INPUT_FORMAT_CLASS, SEQUENCE)
                    .build();
    private static final ImmutableMap<String, Boolean> INPUT_FORMAT_SPLITTABLE =
            new ImmutableMap.Builder<String, Boolean>()
                    .put(MAPRED_PARQUET_INPUT_FORMAT_CLASS, true)
                    .put(HUDI_PARQUET_INPUT_FORMAT, true)
                    .put(ORC_INPUT_FORMAT_CLASS, true)
                    .put(TEXT_INPUT_FORMAT_CLASS, true)
                    .put(AVRO_INPUT_FORMAT_CLASS, true)
                    .put(RCFILE_INPUT_FORMAT_CLASS, true)
                    .put(SEQUENCE_INPUT_FORMAT_CLASS, true)
                    .build();

    private static final ImmutableMap<RemoteFileInputFormat, Boolean> INPUT_FORMAT_BACKEND_SPLITTABLE =
            new ImmutableMap.Builder<RemoteFileInputFormat, Boolean>()
                    .put(PARQUET, true)
                    .put(ORC, true)
                    .build();

    public static RemoteFileInputFormat fromHdfsInputFormatClass(String className) {
        return CLASS_NAME_TO_INPUT_FORMAT.getOrDefault(className, UNKNOWN);
    }

    public static boolean isSplittable(String className) {
        return INPUT_FORMAT_SPLITTABLE.getOrDefault(className, false);
    }

    public boolean isBackendSplittable() {
        return INPUT_FORMAT_BACKEND_SPLITTABLE.getOrDefault(this, false);
    }

    public boolean isTextFormat() {
        switch (this) {
            case TEXTFILE:
                return true;
            default:
                return false;
        }
    }

    public THdfsFileFormat toThrift() {
        switch (this) {
            case PARQUET:
                return THdfsFileFormat.PARQUET;
            case ORC:
                return THdfsFileFormat.ORC;
            case TEXTFILE:
                return THdfsFileFormat.TEXT;
            case AVRO:
                return THdfsFileFormat.AVRO;
            case RCBINARY:
                return THdfsFileFormat.RC_BINARY;
            case RCTEXT:
                return THdfsFileFormat.RC_TEXT;
            case SEQUENCE:
                return THdfsFileFormat.SEQUENCE_FILE;
            default:
                return THdfsFileFormat.UNKNOWN;
        }
    }
}
