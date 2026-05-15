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

package com.starrocks.connector.iceberg;

import com.starrocks.catalog.Database;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteMetaSplit;
import com.starrocks.connector.SerializedMetaSpec;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.connector.share.iceberg.PartitionStatsSplitBean;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import static com.starrocks.connector.iceberg.IcebergCatalogProperties.HIVE_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;

public class IcebergPartitionStatsMetaSpecTest extends TableTestBase {
    private static final String CATALOG_NAME = "iceberg_catalog";
    private static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();
    private static final Map<String, String> DEFAULT_CONFIG = new HashMap<>();
    private static final IcebergCatalogProperties DEFAULT_CATALOG_PROPERTIES;
    private static ConnectContext connectContext;

    static {
        DEFAULT_CONFIG.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732"); // non-existent endpoint
        DEFAULT_CONFIG.put(ICEBERG_CATALOG_TYPE, "hive");
        DEFAULT_CATALOG_PROPERTIES = new IcebergCatalogProperties(DEFAULT_CONFIG);
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    private IcebergMetadata newMetadata(org.apache.iceberg.Table nativeTable) {
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName)
                    throws StarRocksConnectorException {
                return nativeTable;
            }

            @Mock
            Database getDB(ConnectContext context, String dbName) {
                return new Database(0, dbName);
            }
        };
        IcebergHiveCatalog catalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        return new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, catalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
    }

    private static void attachStatsFile(org.apache.iceberg.Table table, long snapshotId) {
        PartitionStatisticsFile statsFile = ImmutableGenericPartitionStatisticsFile.builder()
                .snapshotId(snapshotId)
                .path("/tmp/fake-partition-stats-" + snapshotId + ".parquet")
                .fileSizeInBytes(0L)
                .build();
        table.updatePartitionStatistics().setPartitionStatistics(statsFile).commit();
        table.refresh();
    }

    private static PartitionStatsSplitBean deserializeStatsSplit(RemoteMetaSplit split) {
        Object obj = SerializationUtil.deserializeFromBase64(split.getSerializeSplit());
        if (obj instanceof PartitionStatsSplitBean) {
            return (PartitionStatsSplitBean) obj;
        }
        return null;
    }

    private static boolean anyStatsSplit(IcebergMetaSpec spec) {
        for (RemoteMetaSplit split : spec.getSplits()) {
            if (deserializeStatsSplit(split) != null) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void noStatsFile_returnsFallbackPerManifestSplits() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        mockedNativeTableB.refresh();

        IcebergMetaSpec spec = (IcebergMetaSpec) newMetadata(mockedNativeTableB)
                .getSerializedMetaSpec("db", "tb", -1, null, MetadataTableType.PARTITIONS);

        Assertions.assertFalse(spec.getSplits().isEmpty(), "expected at least one manifest split in fallback");
        Assertions.assertFalse(anyStatsSplit(spec), "fallback must not produce a stats split");
    }

    @Test
    public void statsFileMatchesCurrentSnapshot_returnsBaseSplit() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        long snapshotId = mockedNativeTableB.currentSnapshot().snapshotId();
        attachStatsFile(mockedNativeTableB, snapshotId);

        IcebergMetaSpec spec = (IcebergMetaSpec) newMetadata(mockedNativeTableB)
                .getSerializedMetaSpec("db", "tb", -1, null, MetadataTableType.PARTITIONS);

        Assertions.assertEquals(1, spec.getSplits().size(), "MODE_BASE produces a single split");
        PartitionStatsSplitBean bean = deserializeStatsSplit(spec.getSplits().get(0));
        Assertions.assertNotNull(bean, "split must deserialize to PartitionStatsSplitBean");
        Assertions.assertTrue(bean.isBaseMode());
        Assertions.assertEquals(snapshotId, bean.getStatsSnapshotId().longValue());
        Assertions.assertEquals(snapshotId, bean.getTargetSnapshotId().longValue());
        Assertions.assertFalse(bean.hasIncrementalManifests());
    }

    @Test
    public void statsFileOlderSnapshot_returnsFullSplitWithIncremental() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        long statsSnapshotId = mockedNativeTableB.currentSnapshot().snapshotId();
        attachStatsFile(mockedNativeTableB, statsSnapshotId);

        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        mockedNativeTableB.newAppend().appendFile(FILE_B_3).commit();
        mockedNativeTableB.refresh();
        long targetSnapshotId = mockedNativeTableB.currentSnapshot().snapshotId();

        IcebergMetaSpec spec = (IcebergMetaSpec) newMetadata(mockedNativeTableB)
                .getSerializedMetaSpec("db", "tb", -1, null, MetadataTableType.PARTITIONS);

        Assertions.assertEquals(1, spec.getSplits().size(), "MODE_FULL is a single split bundling stats + incrementals");
        PartitionStatsSplitBean bean = deserializeStatsSplit(spec.getSplits().get(0));
        Assertions.assertNotNull(bean);
        Assertions.assertTrue(bean.isFullMode());
        Assertions.assertEquals(statsSnapshotId, bean.getStatsSnapshotId().longValue());
        Assertions.assertEquals(targetSnapshotId, bean.getTargetSnapshotId().longValue());
        Assertions.assertTrue(bean.hasIncrementalManifests());
        Assertions.assertEquals(2, bean.getIncrementalManifests().size(),
                "two commits past the stats snapshot must contribute two incremental manifests");
    }

    @Test
    public void statsFileOrphanSnapshot_notReachable_fallback() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        long realSnapshot = mockedNativeTableB.currentSnapshot().snapshotId();
        // Pretend a stats file was written against a snapshot that's not in the current ancestor chain.
        attachStatsFile(mockedNativeTableB, realSnapshot + 0x7FFFFFFFL);

        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        mockedNativeTableB.refresh();

        IcebergMetaSpec spec = (IcebergMetaSpec) newMetadata(mockedNativeTableB)
                .getSerializedMetaSpec("db", "tb", -1, null, MetadataTableType.PARTITIONS);

        Assertions.assertFalse(anyStatsSplit(spec),
                "stats file not on the ancestor chain must not be used; fallback expected");
    }

    @Test
    public void sessionVarOff_fallback() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        attachStatsFile(mockedNativeTableB, mockedNativeTableB.currentSnapshot().snapshotId());

        ConnectContext.set(connectContext);
        SessionVariable sv = connectContext.getSessionVariable();
        boolean prev = sv.enableIcebergPartitionStats();
        sv.setEnableIcebergPartitionStats(false);
        try {
            IcebergMetaSpec spec = (IcebergMetaSpec) newMetadata(mockedNativeTableB)
                    .getSerializedMetaSpec("db", "tb", -1, null, MetadataTableType.PARTITIONS);
            Assertions.assertFalse(anyStatsSplit(spec),
                    "session var off must disable the stats path even when a stats file matches");
        } finally {
            sv.setEnableIcebergPartitionStats(prev);
        }
    }

    @Test
    public void unpartitionedTable_neverUsesStatsPath() {
        DataFile unpartitionedFile = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/unpart.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(5)
                .build();
        mockedNativeTableH.newAppend().appendFile(unpartitionedFile).commit();
        long snapshotId = mockedNativeTableH.currentSnapshot().snapshotId();
        attachStatsFile(mockedNativeTableH, snapshotId);

        IcebergMetaSpec spec = (IcebergMetaSpec) newMetadata(mockedNativeTableH)
                .getSerializedMetaSpec("db", "th", -1, null, MetadataTableType.PARTITIONS);

        Assertions.assertFalse(anyStatsSplit(spec),
                "stats path must not engage on unpartitioned tables");
    }

    @Test
    public void emptyTable_returnsEmptySpec() {
        // No commits → no current snapshot.
        SerializedMetaSpec spec = newMetadata(mockedNativeTableB)
                .getSerializedMetaSpec("db", "tb", -1, null, MetadataTableType.PARTITIONS);

        Assertions.assertEquals(IcebergMetaSpec.EMPTY, spec);
    }
}
