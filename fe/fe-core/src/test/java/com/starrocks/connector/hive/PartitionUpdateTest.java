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

import com.google.common.collect.Lists;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.thrift.THiveFileInfo;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class PartitionUpdateTest {

    @Test
    public void testGetPartitionUpdate() {
        String stagingDir = "hdfs://hadoop01:9000/tmp/starrocks/queryid";
        String tableLocation = "hdfs://hadoop01:9000/user/hive/warehouse/test.db/t1";
        THiveFileInfo fileInfo = new THiveFileInfo();
        fileInfo.setFile_name("myfile.parquet");
        fileInfo.setPartition_path("hdfs://hadoop01:9000/tmp/starrocks/queryid/k2=2");
        fileInfo.setRecord_count(10);
        fileInfo.setFile_size_in_bytes(100);
        List<String> partitionColNames = Lists.newArrayList();
        partitionColNames.add("k2");
        PartitionUpdate pu = PartitionUpdate.get(fileInfo, stagingDir, tableLocation, partitionColNames);
        pu.setUpdateMode(PartitionUpdate.UpdateMode.NEW);
        Assertions.assertEquals("k2=2", pu.getName());
        Assertions.assertEquals(Lists.newArrayList("myfile.parquet"), pu.getFileNames());
        Assertions.assertEquals(10, pu.getRowCount());
        Assertions.assertEquals(100, pu.getTotalSizeInBytes());
        Assertions.assertEquals(PartitionUpdate.UpdateMode.NEW, pu.getUpdateMode());
        Assertions.assertEquals("hdfs://hadoop01:9000/tmp/starrocks/queryid/k2=2", pu.getWritePath().toString());
        Assertions.assertEquals("hdfs://hadoop01:9000/user/hive/warehouse/test.db/t1/k2=2", pu.getTargetPath().toString());
        Assertions.assertFalse(pu.isS3Url());

        THiveFileInfo fileInfo1 = new THiveFileInfo();
        ExceptionChecker.expectThrowsWithMsg(
                IllegalStateException.class,
                "Missing partition path",
                () -> PartitionUpdate.get(fileInfo1, stagingDir, tableLocation, partitionColNames));
    }

    @Test
    public void testMergePartitionUpdates() {
        PartitionUpdate first = new PartitionUpdate(
                "k2=1",
                new Path("s3://starrocks-dla/writePath/k2=1"),
                new Path("s3://starrocks-dla/writePath/k2=1"),
                Lists.newArrayList("file1", "file2"),
                1,
                10);

        PartitionUpdate second = new PartitionUpdate(
                "k2=1",
                new Path("s3://starrocks-dla/writePath/k2=1"),
                new Path("s3://starrocks-dla/writePath/k2=1"),
                Lists.newArrayList("file3", "file4"),
                1,
                10);

        List<PartitionUpdate> puList = PartitionUpdate.merge(Lists.newArrayList(first, second));
        Assertions.assertEquals(1, puList.size());
        PartitionUpdate merged = puList.get(0);
        Assertions.assertEquals("k2=1", merged.getName());
        Assertions.assertEquals(Lists.newArrayList("file1", "file2", "file3", "file4"), merged.getFileNames());
        Assertions.assertEquals(2, merged.getRowCount());
        Assertions.assertEquals(20, merged.getTotalSizeInBytes());
        Assertions.assertEquals("s3://starrocks-dla/writePath/k2=1", merged.getWritePath().toString());
        Assertions.assertEquals("s3://starrocks-dla/writePath/k2=1", merged.getTargetPath().toString());
        Assertions.assertTrue(merged.isS3Url());
    }
}
