// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

<<<<<<< HEAD
import com.starrocks.connector.hive.RemoteFileInputFormat;
=======
import com.starrocks.thrift.THdfsFileFormat;
>>>>>>> a5e1935534 ([Enhancement] Add unkown file format in scan range (#33034))
import org.junit.Assert;
import org.junit.Test;

public class RemoteFileInputFormatTest {
    @Test
    public void testParquetFormat() {
        Assert.assertSame(RemoteFileInputFormat.PARQUET, RemoteFileInputFormat
                .fromHdfsInputFormatClass("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"));
        Assert.assertSame(RemoteFileInputFormat.ORC,
                RemoteFileInputFormat.fromHdfsInputFormatClass("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"));
    }

    @Test
    public void testUnknownFormat() {
        RemoteFileInputFormat format = RemoteFileInputFormat.UNKNOWN;
        Assert.assertEquals(THdfsFileFormat.UNKNOWN, format.toThrift());
    }
}
