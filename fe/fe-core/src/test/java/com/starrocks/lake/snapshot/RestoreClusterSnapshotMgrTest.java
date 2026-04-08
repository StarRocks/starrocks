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

import com.starrocks.analysis.BrokerDesc;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.persist.ImageLoader;
import com.starrocks.persist.Storage;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.RunMode;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RestoreClusterSnapshotMgrTest {
    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;
    protected static String DB_NAME = "test";

    @BeforeAll
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

        Assertions.assertThrows(StarRocksException.class,
                () -> RestoreClusterSnapshotMgr.init("src/test/resources/conf/cluster_snapshot.yaml", true));

        Assertions.assertFalse(RestoreClusterSnapshotMgr.isRestoring());
        RestoreClusterSnapshotMgr.finishRestoring();
        Assertions.assertFalse(RestoreClusterSnapshotMgr.isRestoring());
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

        RestoreClusterSnapshotMgr.init("src/test/resources/conf/cluster_snapshot.yaml", true);

        Assertions.assertTrue(RestoreClusterSnapshotMgr.getRestoredSnapshotInfo().getSnapshotName()
                .equals("automated_cluster_snapshot_1704038400000"));
        Assertions.assertTrue(RestoreClusterSnapshotMgr.getRestoredSnapshotInfo().getFeJournalId() == 10L);
        Assertions.assertTrue(RestoreClusterSnapshotMgr.getRestoredSnapshotInfo().getStarMgrJournalId() == 10L);
        Assertions.assertTrue(RestoreClusterSnapshotMgr.isRestoring());

        RestoreClusterSnapshotMgr.getConfig().getComputeNodes().get(0).setCNGroup(null);
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
        Assertions.assertFalse(RestoreClusterSnapshotMgr.isRestoring());

        StorageVolume storageVolume = GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                .getStorageVolumeByName(sv2.getName());

        Assertions.assertEquals(storageVolume.getName(), sv2.getName());
        Assertions.assertEquals(storageVolume.getType(), sv2.getType());
        Assertions.assertEquals(storageVolume.getLocations().get(0), sv2.getLocation());
        Assertions.assertEquals(storageVolume.getComment(), sv2.getComment());

        String newStoragePath = table.getTableProperty().getStorageInfo().getFilePathInfo().getFullPath();
        Assertions.assertNotEquals(oldStoragePath, newStoragePath);
        Assertions.assertTrue(oldStoragePath.startsWith(sv1.getLocation()));
        Assertions.assertTrue(newStoragePath.startsWith(sv2.getLocation()));
    }

    @Test
    public void testManualRestoreCollectSnapshotInfo() throws Exception {
        new MockUp<ImageLoader>() {
            @Mock
            public long getImageJournalId() {
                return 20L;
            }
        };

        new MockUp<Storage>() {
            @Mock
            public long getImageJournalId() {
                return 30L;
            }
        };

        RestoreClusterSnapshotMgr.init("src/test/resources/conf/cluster_snapshot_manual.yaml", true);

        Assertions.assertTrue(RestoreClusterSnapshotMgr.isRestoring());
        RestoredSnapshotInfo restoredSnapshotInfo = RestoreClusterSnapshotMgr.getRestoredSnapshotInfo();
        Assertions.assertTrue(restoredSnapshotInfo.getSnapshotName() == null);
        Assertions.assertEquals(20L, restoredSnapshotInfo.getFeJournalId());
        Assertions.assertEquals(30L, restoredSnapshotInfo.getStarMgrJournalId());

        RestoreClusterSnapshotMgr.finishRestoring();
        Assertions.assertFalse(RestoreClusterSnapshotMgr.isRestoring());
    }

    @Test
    public void testAutoDiscoveryEmptySnapshotList() throws Exception {
        new MockUp<HdfsUtil>() {
            @Mock
            public List<FileStatus> listFileMeta(String path, BrokerDesc brokerDesc, boolean skipDir)
                    throws StarRocksException {
                return new ArrayList<>();
            }
        };

        Assertions.assertThrows(StarRocksException.class,
                () -> RestoreClusterSnapshotMgr.init("src/test/resources/conf/cluster_snapshot_auto.yaml", true));
        Assertions.assertFalse(RestoreClusterSnapshotMgr.isRestoring());
    }

    @Test
    public void testSelectSnapshotWithMetaFile() throws Exception {
        // Three snapshot directories: snap_1 (oldest), snap_2 (middle, has meta), snap_3 (newest, no meta)
        String basePath = "s3://defaultbucket/test/f7265e80-631c-44d3-a8ac-cf7cdc7adec811019/meta/image";
        String snap1 = basePath + "/automated_cluster_snapshot_1000000000000";
        String snap2 = basePath + "/automated_cluster_snapshot_2000000000000";
        String snap3 = basePath + "/automated_cluster_snapshot_3000000000000";

        List<FileStatus> fileStatusList = new ArrayList<>();
        // Unsorted order to verify sorting
        fileStatusList.add(new FileStatus(0, true, 0, 0, 0, new Path(snap1)));
        fileStatusList.add(new FileStatus(0, true, 0, 0, 0, new Path(snap3)));
        fileStatusList.add(new FileStatus(0, true, 0, 0, 0, new Path(snap2)));

        List<String> copiedSrcPaths = new ArrayList<>();
        new MockUp<HdfsUtil>() {
            @Mock
            public List<FileStatus> listFileMeta(String path, BrokerDesc brokerDesc, boolean skipDir)
                    throws StarRocksException {
                return fileStatusList;
            }

            @Mock
            public boolean checkPathExist(String remotePath, BrokerDesc brokerDesc) throws StarRocksException {
                // Only snap2 has meta file
                return remotePath.equals(snap2 + "/" + ClusterSnapshotUtils.SNAPSHOT_META_FILE_NAME);
            }

            @Mock
            public void copyToLocal(String srcPath, String destPath, Map<String, String> properties)
                    throws StarRocksException {
                copiedSrcPaths.add(srcPath);
            }
        };

        new MockUp<Storage>() {
            @Mock
            public long getImageJournalId() {
                return 10L;
            }
        };

        RestoreClusterSnapshotMgr.init("src/test/resources/conf/cluster_snapshot_auto.yaml", true);
        Assertions.assertTrue(RestoreClusterSnapshotMgr.isRestoring());

        // Should have selected snap2 (newest with meta file), not snap3 (newest overall)
        Assertions.assertEquals(1, copiedSrcPaths.size());
        Assertions.assertTrue(copiedSrcPaths.get(0).contains("automated_cluster_snapshot_2000000000000"));

        RestoredSnapshotInfo info = RestoreClusterSnapshotMgr.getRestoredSnapshotInfo();
        Assertions.assertEquals("automated_cluster_snapshot_2000000000000", info.getSnapshotName());

        try {
            RestoreClusterSnapshotMgr.finishRestoring();
        } catch (Exception e) {
            // Storage volume update may fail in test environment; the singleton is still cleaned up
        }
        Assertions.assertFalse(RestoreClusterSnapshotMgr.isRestoring());
    }

    @Test
    public void testSelectSnapshotFallbackNoMetaFile() throws Exception {
        // Three snapshot directories, none has meta file
        String basePath = "s3://defaultbucket/test/f7265e80-631c-44d3-a8ac-cf7cdc7adec811019/meta/image";
        String snap1 = basePath + "/automated_cluster_snapshot_1000000000000";
        String snap2 = basePath + "/automated_cluster_snapshot_2000000000000";
        String snap3 = basePath + "/automated_cluster_snapshot_3000000000000";

        List<FileStatus> fileStatusList = new ArrayList<>();
        fileStatusList.add(new FileStatus(0, true, 0, 0, 0, new Path(snap1)));
        fileStatusList.add(new FileStatus(0, true, 0, 0, 0, new Path(snap3)));
        fileStatusList.add(new FileStatus(0, true, 0, 0, 0, new Path(snap2)));

        List<String> copiedSrcPaths = new ArrayList<>();
        new MockUp<HdfsUtil>() {
            @Mock
            public List<FileStatus> listFileMeta(String path, BrokerDesc brokerDesc, boolean skipDir)
                    throws StarRocksException {
                return fileStatusList;
            }

            @Mock
            public boolean checkPathExist(String remotePath, BrokerDesc brokerDesc) throws StarRocksException {
                return false; // No meta files exist
            }

            @Mock
            public void copyToLocal(String srcPath, String destPath, Map<String, String> properties)
                    throws StarRocksException {
                copiedSrcPaths.add(srcPath);
            }
        };

        new MockUp<Storage>() {
            @Mock
            public long getImageJournalId() {
                return 10L;
            }
        };

        RestoreClusterSnapshotMgr.init("src/test/resources/conf/cluster_snapshot_auto.yaml", true);
        Assertions.assertTrue(RestoreClusterSnapshotMgr.isRestoring());

        // Should fallback to the first directory after sorting (snap3 is newest by name)
        Assertions.assertEquals(1, copiedSrcPaths.size());
        Assertions.assertTrue(copiedSrcPaths.get(0).contains("automated_cluster_snapshot_3000000000000"));

        RestoredSnapshotInfo info = RestoreClusterSnapshotMgr.getRestoredSnapshotInfo();
        Assertions.assertEquals("automated_cluster_snapshot_3000000000000", info.getSnapshotName());

        try {
            RestoreClusterSnapshotMgr.finishRestoring();
        } catch (Exception e) {
            // Storage volume update may fail in test environment; the singleton is still cleaned up
        }
        Assertions.assertFalse(RestoreClusterSnapshotMgr.isRestoring());
    }
}
