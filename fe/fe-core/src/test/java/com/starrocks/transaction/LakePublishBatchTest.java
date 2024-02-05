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


package com.starrocks.transaction;

import com.google.common.collect.Lists;
import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.AwsDefaultCredentialInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.lake.Utils;
import com.starrocks.pseudocluster.PseudoBackend;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.SharedDataStorageVolumeMgr;
import com.starrocks.server.SharedNothingStorageVolumeMgr;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.system.ComputeNode;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LakePublishBatchTest {
    private static PseudoCluster cluster;
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    private static final String DB = "db_for_test";
    private static final String TABLE = "table_for_test";
    private TransactionState.TxnCoordinator transactionSource =
            new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "localfe");

    @BeforeClass
    public static void setUp() throws Exception {
        Config.lake_enable_batch_publish_version = true;
        Config.lake_batch_publish_min_version_num = 2;

        new MockUp<PublishVersionDaemon>() {
            @Mock
            public void runOneCycle() {

            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public long getPrimaryComputeNodeIdByShard(long shardId, long workerGroupId) {
                return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(true).get(0);
            }

            @Mock
            public FilePathInfo allocateFilePath(String storageVolumeId, long dbId, long tableId) {
                FilePathInfo.Builder builder = FilePathInfo.newBuilder();
                FileStoreInfo.Builder fsBuilder = builder.getFsInfoBuilder();

                S3FileStoreInfo.Builder s3FsBuilder = fsBuilder.getS3FsInfoBuilder();
                s3FsBuilder.setBucket("test-bucket");
                s3FsBuilder.setRegion("test-region");
                s3FsBuilder.setCredential(AwsCredentialInfo.newBuilder()
                        .setDefaultCredential(AwsDefaultCredentialInfo.newBuilder().build()));
                S3FileStoreInfo s3FsInfo = s3FsBuilder.build();

                fsBuilder.setFsType(FileStoreType.S3);
                fsBuilder.setFsKey("test-bucket");
                fsBuilder.setFsName("test-fsname");
                fsBuilder.setS3FsInfo(s3FsInfo);
                FileStoreInfo fsInfo = fsBuilder.build();

                builder.setFsInfo(fsInfo);
                builder.setFullPath("s3://test-bucket/1/");
                FilePathInfo pathInfo = builder.build();
                return pathInfo;
            }
        };

        PseudoCluster.getOrCreateWithRandomPort(true, 3);

        cluster = PseudoCluster.getInstance();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB).useDatabase(DB);

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        new MockUp<SharedNothingStorageVolumeMgr>() {
            S3FileStoreInfo s3FileStoreInfo = S3FileStoreInfo.newBuilder().setBucket("default-bucket")
                    .setRegion(Config.aws_s3_region).setEndpoint(Config.aws_s3_endpoint)
                    .setCredential(AwsCredentialInfo.newBuilder()
                            .setDefaultCredential(AwsDefaultCredentialInfo.newBuilder().build()).build()).build();
            FileStoreInfo fsInfo = FileStoreInfo.newBuilder().setFsName(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME)
                    .setFsKey("1").setFsType(FileStoreType.S3)
                    .setS3FsInfo(s3FileStoreInfo).build();

            @Mock
            public StorageVolume getStorageVolumeByName(String svName) throws AnalysisException {
                return StorageVolume.fromFileStoreInfo(fsInfo);
            }

            @Mock
            public StorageVolume getStorageVolume(String svKey) throws AnalysisException {
                return StorageVolume.fromFileStoreInfo(fsInfo);
            }

            @Mock
            public String getStorageVolumeIdOfTable(long tableId) {
                return fsInfo.getFsKey();
            }
        };

        String sql = "create table " + TABLE +
                " (dt date NOT NULL, pk bigint NOT NULL, v0 string not null) primary KEY (dt, pk) " +
                "PARTITION BY RANGE(`dt`) (\n" +
                "    PARTITION p20210820 VALUES [('2021-08-20'), ('2021-08-21')),\n" +
                "    PARTITION p20210821 VALUES [('2021-08-21'), ('2021-08-22')),\n" +
                "    PARTITION p20210929 VALUES [('2021-09-29'), ('2021-09-30')),\n" +
                "    PARTITION p20210930 VALUES [('2021-09-30'), ('2021-10-01'))\n" +
                ")" +
                "DISTRIBUTED BY HASH(pk) BUCKETS 3" +
                " PROPERTIES(\"replication_num\" = \"" + 3 +
                "\", \"storage_medium\" = \"SSD\")";
        starRocksAssert.withTable(sql);
    }

    @Test
    @Order(1)
    public void testNormal() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getDb(DB);
        Table table = db.getTable(TABLE);
        List<TabletCommitInfo> transTablets1 = Lists.newArrayList();
        List<TabletCommitInfo> transTablets2 = Lists.newArrayList();

        int num = 0;
        for (Partition partition : table.getPartitions()) {
            MaterializedIndex baseIndex = partition.getBaseIndex();
            for (Long tabletId : baseIndex.getTabletIdsInOrder()) {
                for (Long backendId : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds()) {
                    TabletCommitInfo tabletCommitInfo = new TabletCommitInfo(tabletId, backendId);
                    if (num % 2 == 0) {
                        transTablets1.add(tabletCommitInfo);
                    } else {
                        transTablets2.add(tabletCommitInfo);
                    }
                }
            }
            num++;
        }

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        long transactionId1 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable1,
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter1 = globalTransactionMgr.commitTransaction(db.getId(), transactionId1, transTablets1,
                Lists.newArrayList(), null);

        long transactionId2 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable2,
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter2 = globalTransactionMgr.commitTransaction(db.getId(), transactionId2, transTablets2,
                Lists.newArrayList(), null);

        long transactionId3 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable3,
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter3 = globalTransactionMgr.commitTransaction(db.getId(), transactionId3, transTablets1,
                Lists.newArrayList(), null);

        long transactionId4 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable4,
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter4 = globalTransactionMgr.commitTransaction(db.getId(), transactionId4, transTablets2,
                Lists.newArrayList(), null);

        PublishVersionDaemon publishVersionDaemon = new PublishVersionDaemon();
        publishVersionDaemon.runAfterCatalogReady();

        Assert.assertTrue(waiter1.await(10, TimeUnit.SECONDS));
        Assert.assertTrue(waiter2.await(10, TimeUnit.SECONDS));
        Assert.assertTrue(waiter3.await(10, TimeUnit.SECONDS));
        Assert.assertTrue(waiter4.await(10, TimeUnit.SECONDS));
    }

    @Test
    @Order(2)
    public void testTransformBatchToSingle() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getDb(DB);
        Table table = db.getTable(TABLE);
        List<TabletCommitInfo> transTablets1 = Lists.newArrayList();
        List<TabletCommitInfo> transTablets2 = Lists.newArrayList();

        int num = 0;
        for (Partition partition : table.getPartitions()) {
            MaterializedIndex baseIndex = partition.getBaseIndex();
            for (Long tabletId : baseIndex.getTabletIdsInOrder()) {
                for (Long backendId : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds()) {
                    TabletCommitInfo tabletCommitInfo = new TabletCommitInfo(tabletId, backendId);
                    if (num % 2 == 0) {
                        transTablets1.add(tabletCommitInfo);
                    } else {
                        transTablets2.add(tabletCommitInfo);
                    }
                }
            }
            num++;
        }

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        long transactionId1 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        "label1",
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter1 = globalTransactionMgr.commitTransaction(db.getId(), transactionId1, transTablets1,
                Lists.newArrayList(), null);

        long transactionId2 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        "label2",
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter2 = globalTransactionMgr.commitTransaction(db.getId(), transactionId2, transTablets2,
                Lists.newArrayList(), null);

        PublishVersionDaemon publishVersionDaemon = new PublishVersionDaemon();
        publishVersionDaemon.runAfterCatalogReady();

        Assert.assertTrue(waiter1.await(10, TimeUnit.SECONDS));
        Assert.assertTrue(waiter2.await(10, TimeUnit.SECONDS));

        Config.lake_enable_batch_publish_version = false;
        long transactionId3 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        "label3",
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter3 = globalTransactionMgr.commitTransaction(db.getId(), transactionId3, transTablets1,
                Lists.newArrayList(), null);

        publishVersionDaemon.runAfterCatalogReady();
        Assert.assertTrue(waiter3.await(10, TimeUnit.SECONDS));

        Config.lake_enable_batch_publish_version = true;
    }

    @Test
    @Order(3)
    public void testPublishTransactionState() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getDb(DB);
        Table table = db.getTable(TABLE);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();

        for (Partition partition : table.getPartitions()) {
            MaterializedIndex baseIndex = partition.getBaseIndex();
            for (Long tabletId : baseIndex.getTabletIdsInOrder()) {
                for (Long backendId : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds()) {
                    TabletCommitInfo tabletCommitInfo = new TabletCommitInfo(tabletId, backendId);
                    transTablets.add(tabletCommitInfo);
                }
            }
        }

        // test publish transactionStateBatch which size is one
        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        Config.lake_batch_publish_min_version_num = 1;
        long transactionId9 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable9,
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        VisibleStateWaiter waiter9 = globalTransactionMgr.commitTransaction(db.getId(), transactionId9, transTablets,
                Lists.newArrayList(), null);

        PublishVersionDaemon publishVersionDaemon = new PublishVersionDaemon();
        publishVersionDaemon.runAfterCatalogReady();
        Assert.assertTrue(waiter9.await(10, TimeUnit.SECONDS));
    }

    @Test
    @Order(4)
    public void testPublishDbDroped() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getDb(DB);
        Table table = db.getTable(TABLE);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        for (Partition partition : table.getPartitions()) {
            MaterializedIndex baseIndex = partition.getBaseIndex();
            for (Long tabletId : baseIndex.getTabletIdsInOrder()) {
                for (Long backendId : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds()) {
                    TabletCommitInfo tabletCommitInfo = new TabletCommitInfo(tabletId, backendId);
                    transTablets.add(tabletCommitInfo);
                }
            }
        }

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();

        long transactionId5 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable5,
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        globalTransactionMgr.commitTransaction(db.getId(), transactionId5, transTablets,
                Lists.newArrayList(), null);

        long transactionId6 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable6,
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        globalTransactionMgr.commitTransaction(db.getId(), transactionId6, transTablets,
                Lists.newArrayList(), null);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public Database getDb(long dbId) {
                return null;
            }
        };

        PublishVersionDaemon publishVersionDaemon = new PublishVersionDaemon();
        publishVersionDaemon.runAfterCatalogReady();

        TransactionState transactionState1 = globalTransactionMgr.getDatabaseTransactionMgr(db.getId()).
                getTransactionState(transactionId5);
        TransactionState transactionState2 = globalTransactionMgr.getDatabaseTransactionMgr(db.getId()).
                getTransactionState(transactionId6);

        // wait publish complete
        Thread.sleep(1000);
        Assert.assertEquals(transactionState1.getTransactionStatus(), TransactionStatus.ABORTED);
        Assert.assertEquals(transactionState2.getTransactionStatus(), TransactionStatus.ABORTED);
    }

    @Test
    @Order(5)
    public void testPublishTableDropped() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getDb(DB);
        Table table = db.getTable(TABLE);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        for (Partition partition : table.getPartitions()) {
            MaterializedIndex baseIndex = partition.getBaseIndex();
            for (Long tabletId : baseIndex.getTabletIdsInOrder()) {
                for (Long backendId : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds()) {
                    TabletCommitInfo tabletCommitInfo = new TabletCommitInfo(tabletId, backendId);
                    transTablets.add(tabletCommitInfo);
                }
            }
        }

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();

        long transactionId7 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable7,
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        globalTransactionMgr.commitTransaction(db.getId(), transactionId7, transTablets,
                Lists.newArrayList(), null);

        long transactionId8 = globalTransactionMgr.
                beginTransaction(db.getId(), Lists.newArrayList(table.getId()),
                        GlobalStateMgrTestUtil.testTxnLable8,
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        // commit a transaction
        globalTransactionMgr.commitTransaction(db.getId(), transactionId8, transTablets,
                Lists.newArrayList(), null);

        new MockUp<Database>() {
            @Mock
            public Table getTable(String tableName) {
                return null;
            }
        };

        PublishVersionDaemon publishVersionDaemon = new PublishVersionDaemon();
        publishVersionDaemon.runAfterCatalogReady();

        TransactionState transactionState1 = globalTransactionMgr.getDatabaseTransactionMgr(db.getId()).
                getTransactionState(transactionId7);
        TransactionState transactionState2 = globalTransactionMgr.getDatabaseTransactionMgr(db.getId()).
                getTransactionState(transactionId8);

        // wait publish complete
        Thread.sleep(1000);
        Assert.assertEquals(transactionState1.getTransactionStatus(), TransactionStatus.VISIBLE);
        Assert.assertEquals(transactionState2.getTransactionStatus(), TransactionStatus.VISIBLE);
    }

    @Test
    @Order(6)
    public void testPublishLogVersion() throws Exception {
        new MockUp<Utils>() {
            @Mock
            public ComputeNode chooseNode(LakeTablet tablet) {
                PseudoBackend pseudoBackend = cluster.getBackends().stream().findAny().get();
                return new ComputeNode(pseudoBackend.getId(), pseudoBackend.getHost(), 9055);
            }
        };

        List<Tablet> tablets = new ArrayList<>();
        tablets.add(new LakeTablet(1L));
        Utils.publishLogVersion(tablets, 1, 1);
    }
}
