// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external;

import com.google.common.collect.ImmutableList;
import com.starrocks.common.Config;
import com.starrocks.external.hive.HdfsFileBlockDesc;
import com.starrocks.external.hive.HdfsFileDesc;
import com.starrocks.external.hive.HdfsFileFormat;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.external.hive.TextFileFormatDesc;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Test;
import org.wildfly.common.Assert;

public class RemoteScanLocationsTest {
    @Mocked
    HdfsFileBlockDesc hdfsFileBlockDesc;

    @Test
    public void testSplittableFiles() {
        RemoteScanRangeLocations instance = new RemoteScanRangeLocations();
        HivePartition partition = new HivePartition(HdfsFileFormat.PARQUET, ImmutableList.of(), "hdfs://nn/xxx/");
        TextFileFormatDesc hiveTextFileDesc = new TextFileFormatDesc("a", "b", "c", "d");
        long size = 3 * Config.hive_max_split_size;
        new Expectations() {
            {
                hdfsFileBlockDesc.getReplicaHostIds();
                result = new long[] {1, 2, 3};
                hdfsFileBlockDesc.getDataNodeIp(anyLong);
                result = String.format("192.168.1.1");
                hdfsFileBlockDesc.getOffset();
                result = 0;
                hdfsFileBlockDesc.getLength();
                result = size;
            }
        };
        HdfsFileDesc fileDesc =
                new HdfsFileDesc("0000", size, HdfsFileFormat.PARQUET, hiveTextFileDesc,
                        ImmutableList.of(hdfsFileBlockDesc),
                        ImmutableList.of());
        instance.addHiveHdfsFiles(10, partition, ImmutableList.of(fileDesc));
        Assert.assertTrue(instance.getScanRangeLocationsSize() == 3);
    }

    @Test
    public void testUnsplittableFiles() {
        RemoteScanRangeLocations instance = new RemoteScanRangeLocations();
        HivePartition partition = new HivePartition(HdfsFileFormat.TEXT, ImmutableList.of(), "hdfs://nn/xxx/");
        TextFileFormatDesc hiveTextFileDesc = new TextFileFormatDesc("a", "b", "c", "d");
        long size = 3 * Config.hive_max_split_size;
        HdfsFileDesc fileDesc =
                new HdfsFileDesc("0000.gz", size, HdfsFileFormat.TEXT, hiveTextFileDesc,
                        ImmutableList.of(hdfsFileBlockDesc),
                        ImmutableList.of());
        instance.addHiveHdfsFiles(10, partition, ImmutableList.of(fileDesc));
        Assert.assertTrue(instance.getScanRangeLocationsSize() == 1);
    }
}
