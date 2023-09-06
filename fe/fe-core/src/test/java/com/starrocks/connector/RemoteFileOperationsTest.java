// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.RemoteFileBlockDesc;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.RemotePathKey;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.HiveMetastoreTest;
import com.starrocks.connector.hive.HiveRemoteFileIO;
import com.starrocks.connector.hive.MockedRemoteFileSystem;
import com.starrocks.connector.hive.Partition;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.connector.hive.MockedRemoteFileSystem.HDFS_HIVE_TABLE;

public class RemoteFileOperationsTest {
    @Test
    public void testGetHiveRemoteFiles() {
        HiveRemoteFileIO hiveRemoteFileIO = new HiveRemoteFileIO(new Configuration());
        FileSystem fs = new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
        hiveRemoteFileIO.setFileSystem(fs);
        FeConstants.runningUnitTest = true;
        ExecutorService executorToRefresh = Executors.newFixedThreadPool(5);
        ExecutorService executorToLoad = Executors.newFixedThreadPool(5);

        CachingRemoteFileIO cachingFileIO = new CachingRemoteFileIO(hiveRemoteFileIO, executorToRefresh, 10, 10, 10);
        RemoteFileOperations ops = new RemoteFileOperations(cachingFileIO, executorToLoad, false, true);

        String tableLocation = "hdfs://127.0.0.1:10000/hive.db/hive_tbl";
        RemotePathKey pathKey = RemotePathKey.of(tableLocation, false);

        HiveMetaClient client = new HiveMetastoreTest.MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog");
        List<String> partitionNames = Lists.newArrayList("col1=1", "col1=2");
        Map<String, Partition> partitions = metastore.getPartitionsByNames("db1", "table1", partitionNames);

        List<RemoteFileInfo> remoteFileInfos = ops.getRemoteFiles(Lists.newArrayList(partitions.values()));
        Assert.assertEquals(2, remoteFileInfos.size());
        Assert.assertTrue(remoteFileInfos.get(0).toString().contains("emoteFileInfo{format=ORC, files=["));

        RemoteFileInfo fileInfo = remoteFileInfos.get(0);
        Assert.assertEquals(RemoteFileInputFormat.ORC, fileInfo.getFormat());
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive.db/hive_tbl/col1=1", fileInfo.getFullPath());

        List<RemoteFileDesc> fileDescs = remoteFileInfos.get(0).getFiles();
        Assert.assertNotNull(fileDescs);
        Assert.assertEquals(1, fileDescs.size());

        RemoteFileDesc fileDesc = fileDescs.get(0);
        Assert.assertNotNull(fileDesc);
        Assert.assertNotNull(fileDesc.getTextFileFormatDesc());
        Assert.assertEquals("", fileDesc.getCompression());
        Assert.assertEquals(20, fileDesc.getLength());
        Assert.assertTrue(fileDesc.isSplittable());

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

        Assert.assertEquals(2, ops.getPresentFilesInCache(partitions.values()).size());
    }
}
