// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.collect.ImmutableMap;
import com.starrocks.thrift.THdfsFileFormat;

public enum HdfsFileFormat {
    UNKNOWN,
    PARQUET,
    ORC,
    TEXT;

    private static final ImmutableMap<String, HdfsFileFormat> VALID_INPUT_FORMATS =
            new ImmutableMap.Builder<String, HdfsFileFormat>()
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

    public static HdfsFileFormat fromHdfsInputFormatClass(String className) {
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
