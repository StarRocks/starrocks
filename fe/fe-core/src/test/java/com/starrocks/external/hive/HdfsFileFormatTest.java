// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import org.junit.Assert;
import org.junit.Test;

public class HdfsFileFormatTest {
    @Test
    public void testParquetFormat() {
        Assert.assertSame(HdfsFileFormat.PARQUET, HdfsFileFormat
                .fromHdfsInputFormatClass("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"));
        Assert.assertSame(HdfsFileFormat.ORC,
                HdfsFileFormat.fromHdfsInputFormatClass("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"));
    }
}
