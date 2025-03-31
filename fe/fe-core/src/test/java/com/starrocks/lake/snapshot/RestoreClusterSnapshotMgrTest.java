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

package com.starrocks.lake.snapshot;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.journal.JournalEntity;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.Storage;
import com.starrocks.persist.TableStorageInfos;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.RunMode;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class RestoreClusterSnapshotMgrTest {
    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;
    protected static String DB_NAME = "test";

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);

        new MockUp<FileUtils>() {
            @Mock
            public boolean deleteQuietly(File file) {
                return true;
            }
        };

        new MockUp<NodeMgr>() {
            @Mock
            public void dropFrontend(FrontendNodeType role, String host, int port) throws DdlException {
                return;
            }

            @Mock
            public void addFrontend(FrontendNodeType role, String host, int editLogPort) throws DdlException {
                return;
            }

        };

        new MockUp<SystemInfoService>() {
            @Mock
            public void dropBackend(String host, int heartbeatPort, String warehouse, boolean needCheckWithoutForce)
                    throws DdlException {
                return;
            }

            @Mock
            public void dropComputeNode(String host, int heartbeatPort, String warehouse) throws DdlException {
                return;
            }

            @Mock
            public void addComputeNode(String host, int heartbeatPort, String warehouse) throws DdlException {
                return;
            }
        };
    }

    @Test
    public void testDownloadSnapshotFailed() throws Exception {
        new MockUp<HdfsUtil>() {
            @Mock
            public void copyToLocal(String srcPath, String destPath, Map<String, String> properties)
                    throws StarRocksException {
                throw new StarRocksException("Copy failed");
            }
        };

        Assert.assertThrows(StarRocksException.class, () -> {
            RestoreClusterSnapshotMgr.init("src/test/resources/conf/cluster_snapshot.yaml",
                    new String[] { "-cluster_snapshot" });
        });

        Assert.assertFalse(RestoreClusterSnapshotMgr.isRestoring());
        RestoreClusterSnapshotMgr.finishRestoring();
        Assert.assertFalse(RestoreClusterSnapshotMgr.isRestoring());
    }

    @Test
    public void testUpdateStorageVolume() throws Exception {
        new MockUp<HdfsUtil>() {
            @Mock
            public void copyToLocal(String srcPath, String destPath, Map<String, String> properties)
                    throws StarRocksException {
                return;
            }
        };

        new MockUp<Storage>() {
            @Mock
            public long getImageJournalId() {
                return 10L;
            }
        };

        new MockUp<EditLog>() {
            @Mock
            public void logUpdateTableStorageInfos(Invocation invocation, TableStorageInfos tableStorageInfos) {
                invocation.proceed();
                ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                try (DataOutputStream out = new DataOutputStream(byteOut)) {
                    tableStorageInfos.write(out);
                    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(byteOut.toByteArray()))) {
                        TableStorageInfos newTableStorageInfos = TableStorageInfos.read(in);
                        GlobalStateMgr.getCurrentState().getEditLog().loadJournal(GlobalStateMgr.getCurrentState(),
                                new JournalEntity(OperationType.OP_UPDATE_TABLE_STORAGE_INFOS, newTableStorageInfos));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        RestoreClusterSnapshotMgr.init("src/test/resources/conf/cluster_snapshot.yaml",
                new String[] { "-cluster_snapshot" });

        Assert.assertTrue(RestoreClusterSnapshotMgr.getRestoredSnapshotInfo().getSnapshotName()
                .equals("automated_cluster_snapshot_1704038400000"));
        Assert.assertTrue(RestoreClusterSnapshotMgr.getRestoredSnapshotInfo().getFeJournalId() == 10L);
        Assert.assertTrue(RestoreClusterSnapshotMgr.getRestoredSnapshotInfo().getStarMgrJournalId() == 10L);
        Assert.assertTrue(RestoreClusterSnapshotMgr.isRestoring());

        ClusterSnapshotConfig.StorageVolume sv1 = RestoreClusterSnapshotMgr.getConfig().getStorageVolumes().get(0);
        ClusterSnapshotConfig.StorageVolume sv2 = RestoreClusterSnapshotMgr.getConfig().getStorageVolumes().get(1);
        RestoreClusterSnapshotMgr.getConfig().getStorageVolumes().remove(0);

        GlobalStateMgr.getCurrentState().getStorageVolumeMgr().createStorageVolume(sv2.getName(), sv1.getType(),
                Collections.singletonList(sv1.getLocation()), sv1.getProperties(), Optional.of(true),
                sv1.getComment());

        String sql = "create table single_partition_duplicate_key (key1 int, key2 varchar(10))\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1', 'storage_volume' = '" + sv2.getName() + "'); ";
        starRocksAssert.withTable(sql);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(DB_NAME, "single_partition_duplicate_key");

        String oldStoragePath = table.getTableProperty().getStorageInfo().getFilePathInfo().getFullPath();

        RestoreClusterSnapshotMgr.finishRestoring();
        Assert.assertFalse(RestoreClusterSnapshotMgr.isRestoring());

        StorageVolume storageVolume = GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                .getStorageVolumeByName(sv2.getName());

        Assert.assertEquals(storageVolume.getName(), sv2.getName());
        Assert.assertEquals(storageVolume.getType(), sv2.getType());
        Assert.assertEquals(storageVolume.getLocations().get(0), sv2.getLocation());
        Assert.assertEquals(storageVolume.getComment(), sv2.getComment());

        String newStoragePath = table.getTableProperty().getStorageInfo().getFilePathInfo().getFullPath();
        Assert.assertNotEquals(oldStoragePath, newStoragePath);
        Assert.assertTrue(oldStoragePath.startsWith(sv1.getLocation()));
        Assert.assertTrue(newStoragePath.startsWith(sv2.getLocation()));
    }
}
