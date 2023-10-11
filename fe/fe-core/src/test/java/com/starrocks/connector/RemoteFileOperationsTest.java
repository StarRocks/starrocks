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
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.HiveMetastoreTest;
import com.starrocks.connector.hive.HiveRemoteFileIO;
import com.starrocks.connector.hive.HiveWriteUtils;
import com.starrocks.connector.hive.MockedRemoteFileSystem;
import com.starrocks.connector.hive.Partition;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.starrocks.connector.hive.MockedRemoteFileSystem.HDFS_HIVE_TABLE;
import static io.airlift.concurrent.MoreFutures.getFutureValue;

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
        RemoteFileOperations ops = new RemoteFileOperations(cachingFileIO, executorToLoad, executorToLoad,
                false, true, new Configuration());

        String tableLocation = "hdfs://127.0.0.1:10000/hive.db/hive_tbl";
        RemotePathKey pathKey = RemotePathKey.of(tableLocation, false);

        HiveMetaClient client = new HiveMetastoreTest.MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
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

        ops.refreshPartitionFilesCache(new Path(tableLocation));
    }

    @Test
    public void asyncRenameFilesTest() {
        HiveRemoteFileIO hiveRemoteFileIO = new HiveRemoteFileIO(new Configuration());
        FileSystem fs = new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
        hiveRemoteFileIO.setFileSystem(fs);
        FeConstants.runningUnitTest = true;
        ExecutorService executorToRefresh = Executors.newFixedThreadPool(5);
        ExecutorService executorToLoad = Executors.newFixedThreadPool(5);

        CachingRemoteFileIO cachingFileIO = new CachingRemoteFileIO(hiveRemoteFileIO, executorToRefresh, 10, 10, 10);
        RemoteFileOperations ops = new RemoteFileOperations(cachingFileIO, executorToLoad, executorToLoad,
                false, true, new Configuration());

        List<CompletableFuture<?>> futures = new ArrayList<>();
        Path writePath = new Path("hdfs://hadoop01:9000/tmp/starrocks/queryid");
        Path targetPath = new Path("hdfs://hadoop01:9000/user/hive/warehouse/test.db/t1");
        List<String> fileNames = Lists.newArrayList("file1");
        ExceptionChecker.expectThrowsWithMsg(
                StarRocksConnectorException.class,
                "Failed to move data files to target location." +
                        " Failed to get file system on path hdfs://hadoop01:9000/tmp/starrocks/queryid",
                () -> ops.asyncRenameFiles(futures, new AtomicBoolean(true), writePath, targetPath, fileNames));


        RemoteFileOperations ops1 = new RemoteFileOperations(cachingFileIO, executorToLoad, Executors.newSingleThreadExecutor(),
                false, true, new Configuration());

        FileSystem mockedFs = new MockedRemoteFileSystem(HDFS_HIVE_TABLE) {
            @Override
            public boolean exists(Path path) {
                return true;
            }
        };

        new MockUp<FileSystem>() {
            @Mock
            public FileSystem get(URI uri, Configuration conf) throws IOException {
                return mockedFs;
            }
        };

        ExceptionChecker.expectThrowsWithMsg(
                StarRocksConnectorException.class,
                "Failed to move data files from hdfs://hadoop01:9000/tmp/starrocks/queryid/file1 to" +
                        " target location hdfs://hadoop01:9000/user/hive/warehouse/test.db/t1/file1." +
                        " msg: target location already exists",
                () -> {
                    ops1.asyncRenameFiles(futures, new AtomicBoolean(false), writePath, targetPath, fileNames);
                    getFutureValue(futures.get(0), StarRocksConnectorException.class);
                });

        new MockUp<FileSystem>() {
            @Mock
            public FileSystem get(URI uri, Configuration conf) throws IOException {
                return fs;
            }
        };

        ExceptionChecker.expectThrowsWithMsg(
                StarRocksConnectorException.class,
                "Failed to move data files from hdfs://hadoop01:9000/tmp/starrocks/queryid/file1 to" +
                        " target location hdfs://hadoop01:9000/user/hive/warehouse/test.db/t1/file1." +
                        " msg: rename operation failed",
                () -> {
                    futures.clear();
                    ops.asyncRenameFiles(futures, new AtomicBoolean(false), writePath, targetPath, fileNames);
                    getFutureValue(futures.get(0), StarRocksConnectorException.class);
                });
    }

    @Test
    public void testRenameDir() {
        HiveRemoteFileIO hiveRemoteFileIO = new HiveRemoteFileIO(new Configuration());
        FileSystem fs = new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
        hiveRemoteFileIO.setFileSystem(fs);
        FeConstants.runningUnitTest = true;
        ExecutorService executorToRefresh = Executors.newSingleThreadExecutor();
        ExecutorService executorToLoad = Executors.newSingleThreadExecutor();
        CachingRemoteFileIO cachingFileIO = new CachingRemoteFileIO(hiveRemoteFileIO, executorToRefresh, 10, 10, 10);
        RemoteFileOperations ops = new RemoteFileOperations(cachingFileIO, executorToLoad, executorToLoad,
                false, true, new Configuration());
        new MockUp<HiveWriteUtils>() {
            @Mock
            public boolean pathExists(Path path, Configuration conf) {
                return true;
            }
        };

        Path writePath = new Path("hdfs://hadoop01:9000/tmp/starrocks/queryid");
        Path targetPath = new Path("hdfs://hadoop01:9000/user/hive/warehouse/test.db/t1");

        ExceptionChecker.expectThrowsWithMsg(
                StarRocksConnectorException.class,
                "Unable to rename from hdfs://hadoop01:9000/tmp/starrocks/queryid to " +
                        "hdfs://hadoop01:9000/user/hive/warehouse/test.db/t1. msg: target directory already exists",
                () -> ops.renameDirectory(writePath, targetPath, () -> {}));
    }

    @Test
    public void testRenameDirFailed() {
        HiveRemoteFileIO hiveRemoteFileIO = new HiveRemoteFileIO(new Configuration());
        FileSystem fs = new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
        hiveRemoteFileIO.setFileSystem(fs);
        FeConstants.runningUnitTest = true;
        ExecutorService executorToRefresh = Executors.newSingleThreadExecutor();
        ExecutorService executorToLoad = Executors.newSingleThreadExecutor();
        CachingRemoteFileIO cachingFileIO = new CachingRemoteFileIO(hiveRemoteFileIO, executorToRefresh, 10, 10, 10);
        RemoteFileOperations ops = new RemoteFileOperations(cachingFileIO, executorToLoad, executorToLoad,
                false, true, new Configuration());


        Path writePath = new Path("hdfs://hadoop01:9000/tmp/starrocks/queryid");
        Path targetPath = new Path("hdfs://hadoop01:9000/user/hive/warehouse/test.db/t1");
        FileSystem mockedFs = new MockedRemoteFileSystem(HDFS_HIVE_TABLE) {
            @Override
            public boolean exists(Path path) {
                if (path.equals(targetPath.getParent())) {
                    return true;
                } else {
                    return false;
                }
            }
        };

        new MockUp<FileSystem>() {
            @Mock
            public FileSystem get(URI uri, Configuration conf) throws IOException {
                return mockedFs;
            }
        };

        ExceptionChecker.expectThrowsWithMsg(
                StarRocksConnectorException.class,
                "Failed to rename",
                () -> ops.renameDirectory(writePath, targetPath, () -> {}));
    }

    @Test
    public void testRemoveNotCurrentQueryFiles() {
        HiveRemoteFileIO hiveRemoteFileIO = new HiveRemoteFileIO(new Configuration());
        FileSystem fs = new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
        hiveRemoteFileIO.setFileSystem(fs);
        FeConstants.runningUnitTest = true;
        ExecutorService executorToRefresh = Executors.newSingleThreadExecutor();
        ExecutorService executorToLoad = Executors.newSingleThreadExecutor();
        CachingRemoteFileIO cachingFileIO = new CachingRemoteFileIO(hiveRemoteFileIO, executorToRefresh, 10, 10, 10);
        RemoteFileOperations ops = new RemoteFileOperations(cachingFileIO, executorToLoad, executorToLoad,
                false, true, new Configuration());
        Path targetPath = new Path("hdfs://hadoop01:9000/user/hive/warehouse/test.db/t1");

        ExceptionChecker.expectThrowsWithMsg(
                StarRocksConnectorException.class,
                "Failed to delete partition",
                () -> ops.removeNotCurrentQueryFiles(targetPath, "aaa"));

        new MockUp<FileSystem>() {
            @Mock
            public FileSystem get(URI uri, Configuration conf) throws IOException {
                return fs;
            }
        };
        ExceptionChecker.expectThrowsWithMsg(
                StarRocksConnectorException.class,
                "file name or query id is invalid",
                () -> ops.removeNotCurrentQueryFiles(targetPath, "aaa"));
    }
}
