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
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.HiveMetastoreTest;
import com.starrocks.connector.hive.HiveRemoteFileIO;
import com.starrocks.connector.hive.HiveUtils;
import com.starrocks.connector.hive.MockedRemoteFileSystem;
import com.starrocks.connector.hive.Partition;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.connector.hudi.HudiRemoteFileIO;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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

        CachingRemoteFileIO cachingFileIO = new CachingRemoteFileIO(hiveRemoteFileIO, executorToRefresh, 10, 10, 0.1);
        RemoteFileOperations ops = new RemoteFileOperations(cachingFileIO, executorToLoad, executorToLoad,
                false, true, new Configuration());

        String tableLocation = "hdfs://127.0.0.1:10000/hive.db/hive_tbl";
        RemotePathKey pathKey = RemotePathKey.of(tableLocation, false);

        HiveMetaClient client = new HiveMetastoreTest.MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        List<String> partitionNames = Lists.newArrayList("col1=1", "col1=2");
        Map<String, Partition> partitions = metastore.getPartitionsByNames("db1", "table1", partitionNames);

        List<RemoteFileInfo> remoteFileInfos =
                ops.getRemoteFiles(new HudiTable(), Lists.newArrayList(partitions.values()),
                        GetRemoteFilesParams.newBuilder().build());
        Assertions.assertEquals(2, remoteFileInfos.size());
        Assertions.assertTrue(remoteFileInfos.get(0).toString().contains("emoteFileInfo{format=ORC, files=["));

        RemoteFileInfo fileInfo = remoteFileInfos.get(0);
        Assertions.assertEquals(RemoteFileInputFormat.ORC, fileInfo.getFormat());
        Assertions.assertEquals("hdfs://127.0.0.1:10000/hive.db/hive_tbl/col1=1", fileInfo.getFullPath());

        List<RemoteFileDesc> fileDescs = remoteFileInfos.get(0).getFiles();
        Assertions.assertNotNull(fileDescs);
        Assertions.assertEquals(1, fileDescs.size());

        RemoteFileDesc fileDesc = fileDescs.get(0);
        Assertions.assertNotNull(fileDesc);
        Assertions.assertNotNull(fileDesc.getTextFileFormatDesc());
        Assertions.assertEquals("", fileDesc.getCompression());
        Assertions.assertEquals(20, fileDesc.getLength());
        Assertions.assertTrue(fileDesc.isSplittable());

        List<RemoteFileBlockDesc> blockDescs = fileDesc.getBlockDescs();
        Assertions.assertEquals(1, blockDescs.size());
        RemoteFileBlockDesc blockDesc = blockDescs.get(0);
        Assertions.assertEquals(0, blockDesc.getOffset());
        Assertions.assertEquals(20, blockDesc.getLength());
        Assertions.assertEquals(2, blockDesc.getReplicaHostIds().length);

        CachingRemoteFileIO queryLevelCache = CachingRemoteFileIO.createQueryLevelInstance(cachingFileIO, 0.1);
        Assertions.assertEquals(1, queryLevelCache.getRemoteFiles(pathKey).size());

        Map<RemotePathKey, List<RemoteFileDesc>> presentRemoteFileInfos =
                cachingFileIO.getPresentRemoteFiles(Lists.newArrayList(pathKey));
        Assertions.assertEquals(1, presentRemoteFileInfos.size());

        Assertions.assertEquals(2, ops.getPresentFilesInCache(partitions.values()).size());

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

        CachingRemoteFileIO cachingFileIO = new CachingRemoteFileIO(hiveRemoteFileIO, executorToRefresh, 10, 10, 0.1);
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
    public void testEnsureDirectoryExistsCreatesWhenMissing() {
        AtomicInteger createCount = new AtomicInteger(0);
        new MockUp<HiveUtils>() {
            @Mock
            public boolean pathExists(Path path, Configuration conf) {
                return false;
            }

            @Mock
            public void createDirectory(Path path, Configuration conf) {
                createCount.incrementAndGet();
            }
        };

        HiveRemoteFileIO hiveRemoteFileIO = new HiveRemoteFileIO(new Configuration());
        hiveRemoteFileIO.setFileSystem(new MockedRemoteFileSystem(HDFS_HIVE_TABLE));
        FeConstants.runningUnitTest = true;
        ExecutorService executorToRefresh = Executors.newSingleThreadExecutor();
        ExecutorService executorToLoad = Executors.newSingleThreadExecutor();
        CachingRemoteFileIO cachingFileIO = new CachingRemoteFileIO(hiveRemoteFileIO, executorToRefresh, 10, 10, 0.1);
        RemoteFileOperations ops = new RemoteFileOperations(cachingFileIO, executorToLoad, executorToLoad,
                false, true, new Configuration());

        Path p = new Path("hdfs://127.0.0.1:9000/warehouse/missing/part");
        ops.ensureDirectoryExists(p);
        Assertions.assertEquals(1, createCount.get());
    }

    @Test
    public void testEnsureDirectoryExistsNoOpWhenPresent() {
        AtomicInteger createCount = new AtomicInteger(0);
        new MockUp<HiveUtils>() {
            @Mock
            public boolean pathExists(Path path, Configuration conf) {
                return true;
            }

            @Mock
            public void createDirectory(Path path, Configuration conf) {
                createCount.incrementAndGet();
            }
        };

        HiveRemoteFileIO hiveRemoteFileIO = new HiveRemoteFileIO(new Configuration());
        hiveRemoteFileIO.setFileSystem(new MockedRemoteFileSystem(HDFS_HIVE_TABLE));
        FeConstants.runningUnitTest = true;
        ExecutorService executorToRefresh = Executors.newSingleThreadExecutor();
        ExecutorService executorToLoad = Executors.newSingleThreadExecutor();
        CachingRemoteFileIO cachingFileIO = new CachingRemoteFileIO(hiveRemoteFileIO, executorToRefresh, 10, 10, 0.1);
        RemoteFileOperations ops = new RemoteFileOperations(cachingFileIO, executorToLoad, executorToLoad,
                false, true, new Configuration());

        ops.ensureDirectoryExists(new Path("hdfs://127.0.0.1:9000/warehouse/exists"));
        Assertions.assertEquals(0, createCount.get());
    }

    @Test
    public void testRenameDir() {
        HiveRemoteFileIO hiveRemoteFileIO = new HiveRemoteFileIO(new Configuration());
        FileSystem fs = new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
        hiveRemoteFileIO.setFileSystem(fs);
        FeConstants.runningUnitTest = true;
        ExecutorService executorToRefresh = Executors.newSingleThreadExecutor();
        ExecutorService executorToLoad = Executors.newSingleThreadExecutor();
        CachingRemoteFileIO cachingFileIO = new CachingRemoteFileIO(hiveRemoteFileIO, executorToRefresh, 10, 10, 0.1);
        RemoteFileOperations ops = new RemoteFileOperations(cachingFileIO, executorToLoad, executorToLoad,
                false, true, new Configuration());
        new MockUp<HiveUtils>() {
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
                () -> ops.renameDirectory(writePath, targetPath, () -> {
                }));
    }

    @Test
    public void testRenameDirFailed() {
        HiveRemoteFileIO hiveRemoteFileIO = new HiveRemoteFileIO(new Configuration());
        FileSystem fs = new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
        hiveRemoteFileIO.setFileSystem(fs);
        FeConstants.runningUnitTest = true;
        ExecutorService executorToRefresh = Executors.newSingleThreadExecutor();
        ExecutorService executorToLoad = Executors.newSingleThreadExecutor();
        CachingRemoteFileIO cachingFileIO = new CachingRemoteFileIO(hiveRemoteFileIO, executorToRefresh, 10, 10, 0.1);
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
                () -> ops.renameDirectory(writePath, targetPath, () -> {
                }));
    }

    @Test
    public void testRemoveNotCurrentQueryFiles() {
        HiveRemoteFileIO hiveRemoteFileIO = new HiveRemoteFileIO(new Configuration());
        FileSystem fs = new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
        hiveRemoteFileIO.setFileSystem(fs);
        FeConstants.runningUnitTest = true;
        ExecutorService executorToRefresh = Executors.newSingleThreadExecutor();
        ExecutorService executorToLoad = Executors.newSingleThreadExecutor();
        CachingRemoteFileIO cachingFileIO = new CachingRemoteFileIO(hiveRemoteFileIO, executorToRefresh, 10, 10, 0.1);
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

    @Test
    public void testAnonPartitionInfo() {
        PartitionInfo x = new PartitionInfo() {
            @Override
            public long getModifiedTime() {
                return 0;
            }
        };
        Assertions.assertEquals(0, x.getVersion());
    }

    @Test
    public void testRemotePathKeySetFileScanContext() {
        RemotePathKey pathKey = new RemotePathKey("hello", true);
        Assertions.assertNull(pathKey.getTableLocation());
        Assertions.assertNull(pathKey.getScanContext());

        RemoteFileScanContext scanContext = null;
        scanContext = HudiRemoteFileIO.getScanContext(pathKey, "tableLocation");
        Assertions.assertNotNull(scanContext);
        pathKey.setScanContext(scanContext);
        Assertions.assertEquals(pathKey.getTableLocation(), "tableLocation");
        Assertions.assertTrue(pathKey.getScanContext() == scanContext);

        RemoteFileScanContext scanContext1 = HudiRemoteFileIO.getScanContext(pathKey, "null");
        Assertions.assertTrue(pathKey.getScanContext() == scanContext1);
    }

    @Test
    public void testAsyncGetHiveRemoteFiles() {
        HiveRemoteFileIO hiveRemoteFileIO = new HiveRemoteFileIO(new Configuration());
        FileSystem fs = new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
        hiveRemoteFileIO.setFileSystem(fs);
        FeConstants.runningUnitTest = true;
        ExecutorService executorToRefresh = Executors.newFixedThreadPool(5);
        ExecutorService executorToLoad = Executors.newFixedThreadPool(5);

        CachingRemoteFileIO cachingFileIO = new CachingRemoteFileIO(hiveRemoteFileIO, executorToRefresh, 10, 10, 0.1);
        RemoteFileOperations ops = new RemoteFileOperations(cachingFileIO, executorToLoad, executorToLoad,
                false, true, new Configuration());

        String tableLocation = "hdfs://127.0.0.1:10000/hive.db/hive_tbl";
        // RemotePathKey pathKey = RemotePathKey.of(tableLocation, false);

        HiveMetaClient client = new HiveMetastoreTest.MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        List<String> partitionNames = Lists.newArrayList("col1=1", "col1=2");
        // Map<String, Partition> partitions = metastore.getPartitionsByNames("db1", "table1", partitionNames);

        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().
                setPartitionNames(partitionNames).build();

        RemoteFileInfoSource remoteFileInfoSource = ops.getRemoteFilesAsync(new HiveTable(), params,
                (p) -> {
                    Map<String, Partition> map = metastore.getPartitionsByNames("db1", "table1", p.getPartitionNames());
                    List<Partition> res = new ArrayList<>();
                    for (String key : p.getPartitionNames()) {
                        res.add(map.get(key));
                    }
                    return res;
                });
        List<RemoteFileInfo> remoteFileInfos = remoteFileInfoSource.getAllOutputs();
        remoteFileInfos.sort(Comparator.comparing(RemoteFileInfo::getFullPath));
        Assertions.assertEquals(2, remoteFileInfos.size());
        Assertions.assertTrue(remoteFileInfos.get(0).toString().contains("emoteFileInfo{format=ORC, files=["));

        RemoteFileInfo fileInfo = remoteFileInfos.get(0);
        Assertions.assertEquals(RemoteFileInputFormat.ORC, fileInfo.getFormat());
        Assertions.assertEquals("hdfs://127.0.0.1:10000/hive.db/hive_tbl/col1=1", fileInfo.getFullPath());

        List<RemoteFileDesc> fileDescs = remoteFileInfos.get(0).getFiles();
        Assertions.assertNotNull(fileDescs);
        Assertions.assertEquals(1, fileDescs.size());

        RemoteFileDesc fileDesc = fileDescs.get(0);
        Assertions.assertNotNull(fileDesc);
        Assertions.assertNotNull(fileDesc.getTextFileFormatDesc());
        Assertions.assertEquals("", fileDesc.getCompression());
        Assertions.assertEquals(20, fileDesc.getLength());
        Assertions.assertTrue(fileDesc.isSplittable());

        List<RemoteFileBlockDesc> blockDescs = fileDesc.getBlockDescs();
        Assertions.assertEquals(1, blockDescs.size());
        RemoteFileBlockDesc blockDesc = blockDescs.get(0);
        Assertions.assertEquals(0, blockDesc.getOffset());
        Assertions.assertEquals(20, blockDesc.getLength());
        Assertions.assertEquals(2, blockDesc.getReplicaHostIds().length);
    }

    @Test
    public void testGetRemoteFilesParamsPartitionExponentially() {
        List<String> partitionNames = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            partitionNames.add("partition" + i);
        }
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().setPartitionNames(partitionNames).build();
        List<GetRemoteFilesParams> result = params.partitionExponentially(2, 8);
        Assertions.assertEquals(4, result.size());
        Assertions.assertEquals(2, result.get(0).getPartitionNames().size());
        Assertions.assertEquals(4, result.get(1).getPartitionNames().size());
        Assertions.assertEquals(8, result.get(2).getPartitionNames().size());
        Assertions.assertEquals(2, result.get(3).getPartitionNames().size());
    }
}
