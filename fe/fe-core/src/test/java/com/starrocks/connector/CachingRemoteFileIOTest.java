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

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.RemoteFileBlockDesc;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemotePathKey;
import com.starrocks.connector.hive.HiveRemoteFileIO;
import com.starrocks.connector.hive.MockedRemoteFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.connector.hive.MockedRemoteFileSystem.TEST_FILES;

public class CachingRemoteFileIOTest {

    @Test
    public void testGetHiveRemoteFiles() {
        HiveRemoteFileIO hiveRemoteFileIO = new HiveRemoteFileIO(new Configuration());
        FileSystem fs = new MockedRemoteFileSystem(TEST_FILES);
        hiveRemoteFileIO.setFileSystem(fs);
        FeConstants.runningUnitTest = true;
        ExecutorService executor = Executors.newFixedThreadPool(5);
        CachingRemoteFileIO cachingFileIO = new CachingRemoteFileIO(hiveRemoteFileIO, executor, 10, 10, 10);

        String tableLocation = "hdfs://127.0.0.1:10000/hive.db/hive_tbl";
        RemotePathKey pathKey = RemotePathKey.of(tableLocation, false);
        Map<RemotePathKey, List<RemoteFileDesc>> remoteFileInfos = cachingFileIO.getRemoteFiles(pathKey);
        List<RemoteFileDesc> fileDescs = remoteFileInfos.get(pathKey);
        Assert.assertNotNull(fileDescs);
        Assert.assertEquals(1, fileDescs.size());
        RemoteFileDesc fileDesc = fileDescs.get(0);
        Assert.assertNotNull(fileDesc);
        Assert.assertEquals("000000_0", fileDesc.getFileName());
        Assert.assertEquals("", fileDesc.getCompression());
        Assert.assertEquals(20, fileDesc.getLength());
        Assert.assertFalse(fileDesc.isSplittable());
        Assert.assertNull(fileDesc.getTextFileFormatDesc());

        List<RemoteFileBlockDesc> blockDescs = fileDesc.getBlockDescs();
        Assert.assertEquals(1, blockDescs.size());
        RemoteFileBlockDesc blockDesc = blockDescs.get(0);
        Assert.assertEquals(0, blockDesc.getOffset());
        Assert.assertEquals(20, blockDesc.getLength());
        Assert.assertEquals(2, blockDesc.getReplicaHostIds().length);

        CachingRemoteFileIO queryLevelCache = CachingRemoteFileIO.createQueryLevelInstance(cachingFileIO, 5);
        Assert.assertEquals(1, queryLevelCache.getRemoteFiles(pathKey).size());

        Map<RemotePathKey, List<RemoteFileDesc>> presentRemoteFileInfos =
                cachingFileIO.getPresentRemoteFiles(Lists.newArrayList(pathKey));
        Assert.assertEquals(1, presentRemoteFileInfos.size());
    }
}
