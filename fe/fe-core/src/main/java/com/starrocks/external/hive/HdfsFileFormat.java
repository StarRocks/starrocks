// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.hive;

import com.google.common.collect.ImmutableMap;
import com.starrocks.thrift.THdfsFileFormat;

public enum HdfsFileFormat {
    UNKNOWN("unknown"),
    PARQUET("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
    ORC("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"),
    TEXT("org.apache.hadoop.mapred.TextInputFormat");

    private static final ImmutableMap<String, HdfsFileFormat> validInputFormats =
            new ImmutableMap.Builder<String, HdfsFileFormat>()
                    .put("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", PARQUET)
                    .put("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", ORC)
                    .put("org.apache.hadoop.mapred.TextInputFormat", TEXT)
                    .build();
    private static final ImmutableMap<String, Boolean> fileFormatSplittableInfos =
            new ImmutableMap.Builder<String, Boolean>()
                    .put("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", true)
                    .put("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", true)
                    .put("org.apache.hadoop.mapred.TextInputFormat", true)
                    .build();

    private final String inputFormat;

    HdfsFileFormat(String inputFormat) {
        this.inputFormat = inputFormat;
    }

    public static HdfsFileFormat fromHdfsInputFormatClass(String className) {
        return validInputFormats.get(className);
    }

    public static boolean isSplittable(String className) {
        return fileFormatSplittableInfos.containsKey(className) && fileFormatSplittableInfos.get(className);
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
