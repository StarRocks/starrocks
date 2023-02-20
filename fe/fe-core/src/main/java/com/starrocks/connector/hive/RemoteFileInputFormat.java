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

public enum RemoteFileInputFormat {
    UNKNOWN,
    PARQUET,
    ORC,
    TEXT;

    private static final ImmutableMap<String, RemoteFileInputFormat> VALID_INPUT_FORMATS =
            new ImmutableMap.Builder<String, RemoteFileInputFormat>()
                    .put("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", PARQUET)
                    .put("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", ORC)
                    .put("org.apache.hadoop.mapred.TextInputFormat", TEXT)
                    .build();
    private static final ImmutableMap<String, Boolean> FILE_FORMAT_SPLITTABLE_INFOS =
            new ImmutableMap.Builder<String, Boolean>()
                    .put("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", true)
                    .put("org.apache.hudi.hadoop.HoodieParquetInputFormat", true)
                    .put("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", true)
                    .put("org.apache.hadoop.mapred.TextInputFormat", true)
                    .build();

    public static RemoteFileInputFormat fromHdfsInputFormatClass(String className) {
        return VALID_INPUT_FORMATS.get(className);
    }

    public static boolean isSplittable(String className) {
        return FILE_FORMAT_SPLITTABLE_INFOS.containsKey(className) && FILE_FORMAT_SPLITTABLE_INFOS.get(className);
    }

    public THdfsFileFormat toThrift() {
        switch (this) {
            case PARQUET:
                return THdfsFileFormat.PARQUET;
            case ORC:
                return THdfsFileFormat.ORC;
            case TEXT:
                return THdfsFileFormat.TEXT;
            default:
                break;
        }
        return null;
    }
}
