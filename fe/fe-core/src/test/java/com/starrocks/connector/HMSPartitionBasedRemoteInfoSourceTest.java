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

package com.starrocks.connector;

import com.starrocks.connector.hive.Partition;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HMSPartitionBasedRemoteInfoSourceTest {
    @Test
    public void testGetHiveRemoteFiles() {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        int partitionCount = 1024;
        List<String> partitionNames = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            partitionNames.add("dt=" + String.valueOf(i));
        }
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().
                setPartitionNames(partitionNames).build();

        String hdfsPath = "hdfs://hadoop01/tmp/";
        class Counter {
            int getRemoteFiles = 0;
            int getPartitionValues = 0;
        }
        final Counter counter = new Counter();

        HMSPartitionBasedRemoteInfoSource source = new HMSPartitionBasedRemoteInfoSource(executorService, params,
                partition -> {
                    counter.getRemoteFiles++;
                    // a single file under a partition.
                    RemoteFileInfo remoteFileInfo =
                            new RemoteFileInfo(RemoteFileInputFormat.PARQUET,
                                    List.of(new RemoteFileDesc("aa", "lz4", 0, 0, null)),
                                    partition.getFullPath());
                    return remoteFileInfo;
                },
                ps -> {
                    counter.getPartitionValues++;
                    System.out.println("getPartitionValues: " + String.join(",", ps.getPartitionNames()));
                    List<Partition> res = new ArrayList<>();
                    for (String name : ps.getPartitionNames()) {
                        Partition partition =
                                new Partition(null, RemoteFileInputFormat.PARQUET, null,
                                        hdfsPath + name, false);
                        res.add(partition);
                    }
                    return res;
                });

        source.setMaxOutputQueueSize(HMSPartitionBasedRemoteInfoSource.HMS_PARTITION_BATCH_SIZE_MIN);
        source.setMaxRunningTaskCount(4);
        source.run();
        // we fetch MIN files(partitions) + buffer MIN files(partitions), and getPartitionValues should be only 2.
        for (int i = 0; i < HMSPartitionBasedRemoteInfoSource.HMS_PARTITION_BATCH_SIZE_MIN; i++) {
            RemoteFileInfo remoteFileInfo = source.getOutput();
            System.out.println(remoteFileInfo.getFullPath());
        }
        Assert.assertEquals(2, counter.getPartitionValues);
        Assert.assertTrue(HMSPartitionBasedRemoteInfoSource.HMS_PARTITION_BATCH_SIZE_MIN <= counter.getRemoteFiles);
    }
}
