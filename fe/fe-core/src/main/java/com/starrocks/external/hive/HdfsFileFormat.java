// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.collect.ImmutableMap;
import com.starrocks.thrift.THdfsFileFormat;

public enum HdfsFileFormat {
    UNKNOWN,
    PARQUET,
    ORC,
    TEXT;

    private static final ImmutableMap<String, HdfsFileFormat> SUPPORTED_FILE_FORMATS =
            new ImmutableMap.Builder<String, HdfsFileFormat>()
                    .put("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", PARQUET)
                    .put("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", ORC)
                    .put("org.apache.hadoop.mapred.TextInputFormat", TEXT)
                    .build();

    public static HdfsFileFormat fromHdfsInputFormatClass(String className) {
        HdfsFileFormat format = SUPPORTED_FILE_FORMATS.getOrDefault(className, null);
        if (format == null) {
            throw new RuntimeException(String.format("Unsupported file format: %s", className));
        }
        return format;
    }

    public boolean isSplittable(String fileName) {
        if (this != TEXT) {
            return true;
        }

        // if text file name is with any extension
        // probably it's compressed file format.
        String extension = null;
        int i = fileName.lastIndexOf('.');
        if (i > 0) {
            extension = fileName.substring(i + 1);
        }
        return (extension == null);
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
