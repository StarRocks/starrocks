// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import org.junit.Assert;
import org.junit.Test;

public class HdfsFileFormatTest {
    @Test
    public void testNormal() {
        Assert.assertSame(HdfsFileFormat.PARQUET, HdfsFileFormat
                .fromHdfsInputFormatClass("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"));
        Assert.assertSame(HdfsFileFormat.ORC,
                HdfsFileFormat.fromHdfsInputFormatClass("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"));
    }

    @Test
    public void testIsSplittable() {
        Assert.assertTrue(HdfsFileFormat.PARQUET.isSplittable(""));
        Assert.assertTrue(HdfsFileFormat.PARQUET.isSplittable("x.gz"));
        Assert.assertTrue(HdfsFileFormat.ORC.isSplittable(""));
        Assert.assertTrue(HdfsFileFormat.ORC.isSplittable("x.gz"));
        Assert.assertTrue(HdfsFileFormat.TEXT.isSplittable(""));
        Assert.assertTrue(HdfsFileFormat.TEXT.isSplittable("part-000"));
        Assert.assertTrue(!HdfsFileFormat.TEXT.isSplittable("part-000.gz"));
    }
}

