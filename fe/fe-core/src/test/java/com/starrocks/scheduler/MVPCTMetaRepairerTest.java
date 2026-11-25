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


package com.starrocks.scheduler;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HivePartitionDataInfo;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.hive.HiveMetadata;
import com.starrocks.scheduler.mv.pct.MVPCTMetaRepairer;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.connector.iceberg.MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME;
import static org.assertj.core.api.Assertions.assertThat;


@TestMethodOrder(MethodName.class)
public class MVPCTMetaRepairerTest extends MVTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
    }

    @Test
    public void testIsSupportedPCTRepairerIceberg() throws Exception {
        // iceberg catalog
        ConnectorPlanTestBase.mockCatalog(connectContext, MOCKED_ICEBERG_CATALOG_NAME);

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Table t1 =  metadataMgr.getTable(connectContext, MOCKED_ICEBERG_CATALOG_NAME, "partitioned_db", "t1");
        assertThat(t1).isNotNull();

        {
            BaseTableInfo baseTableInfo = new BaseTableInfo(MOCKED_ICEBERG_CATALOG_NAME, t1.getCatalogDBName(),
                    t1.getName(), t1.getTableIdentifier());
            assertThat(MVPCTMetaRepairer.isSupportedPCTRepairer(t1, baseTableInfo)).isTrue();
        }
        {
            BaseTableInfo baseTableInfo = new BaseTableInfo(MOCKED_ICEBERG_CATALOG_NAME, t1.getCatalogDBName(),
                    t1.getName(), "t1:0");
            assertThat(MVPCTMetaRepairer.isSupportedPCTRepairer(t1, baseTableInfo)).isFalse();
        }

        ConnectorPlanTestBase.dropCatalog(MOCKED_ICEBERG_CATALOG_NAME);
    }

    @Test
    public void testIsSupportedPCTRepairerHive() throws Exception {
        // hive catalog
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Table t1 =  metadataMgr.getTable(connectContext, "hive0", "tpch", "supplier");
        assertThat(t1).isNotNull();

        {
            BaseTableInfo baseTableInfo = new BaseTableInfo("hive0", t1.getCatalogDBName(),
                    t1.getName(), t1.getTableIdentifier());
            assertThat(MVPCTMetaRepairer.isSupportedPCTRepairer(t1, baseTableInfo)).isTrue();
        }
        {
            BaseTableInfo baseTableInfo = new BaseTableInfo("hive0", t1.getCatalogDBName(),
                    t1.getName(), "supplier:0");
            assertThat(MVPCTMetaRepairer.isSupportedPCTRepairer(t1, baseTableInfo)).isTrue();
        }

        ConnectorPlanTestBase.dropCatalog("hive0");
    }

    @Test
    public void testPCTRepairerWithIceberg() throws Exception {
        // iceberg catalog
        ConnectorPlanTestBase.mockCatalog(connectContext, MOCKED_ICEBERG_CATALOG_NAME);

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Table t1 =  metadataMgr.getTable(connectContext, MOCKED_ICEBERG_CATALOG_NAME, "partitioned_db", "t1");
        assertThat(t1).isNotNull();

        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `iceberg_mv1` " +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"" +
                        ")\n" +
                        "AS SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1` as a;");

        MaterializedView mv = getMv("iceberg_mv1");
        assertThat(mv).isNotNull();
        assertThat(mv.isActive()).isTrue();
        List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
        assertThat(baseTableInfos.size()).isEqualTo(1);
        BaseTableInfo baseTableInfo = baseTableInfos.get(0);
        assertThat(baseTableInfo.getTableIdentifier()).isEqualTo(t1.getTableIdentifier());
        // it's fine to refresh
        {
            refreshMaterializedView("test", "iceberg_mv1");
            assertThat(mv.isActive()).isTrue();
        }

        // it will inactive after refresh
        {
            new MockUp<BaseTableInfo>() {
                @Mock
                public String getTableIdentifier() {
                    return "xxx:0";
                }
            };
            try {
                refreshMaterializedView("test", "iceberg_mv1");
                Assertions.fail();
            } catch (Exception e) {
                assertThat(e.getMessage()).contains("base table 't1'");
                assertThat(e.getMessage()).contains(
                        "was recreated but its table type is not supported for automatic meta repair");
            }
            assertThat(mv.isActive()).isFalse();
            assertThat(mv.getInactiveReason().equals("base-table changed: t1")).isTrue();
        }

        {
            new MockUp<BaseTableInfo>() {
                @Mock
                public String getTableIdentifier() {
                    return t1.getTableIdentifier();
                }
            };
            refreshMaterializedView("test", "iceberg_mv1");
            assertThat(mv.isActive()).isTrue();
        }

        ConnectorPlanTestBase.dropCatalog(MOCKED_ICEBERG_CATALOG_NAME);
    }

    @Test
    public void testPCTRepairerWithHive() throws Exception {
        Config.enable_materialized_view_external_table_precise_refresh = true;
        // hive catalog
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Table t1 =  metadataMgr.getTable(connectContext, "hive0", "tpch", "supplier");
        assertThat(t1).isNotNull();

        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `hive_mv1` " +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"" +
                        ")\n" +
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier");

        MaterializedView mv = getMv("hive_mv1");
        assertThat(mv).isNotNull();
        assertThat(mv.isActive()).isTrue();
        List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
        assertThat(baseTableInfos.size()).isEqualTo(1);
        BaseTableInfo baseTableInfo = baseTableInfos.get(0);
        assertThat(baseTableInfo.getTableIdentifier()).isEqualTo(t1.getTableIdentifier());
        // it's fine to refresh
        {
            refreshMaterializedView("test", "hive_mv1");
            assertThat(mv.isActive()).isTrue();
        }

        // it will inactive after refresh
        {
            new MockUp<BaseTableInfo>() {
                @Mock
                public String getTableIdentifier() {
                    return "xxx:0";
                }
            };
            try {
                refreshMaterializedView("test", "hive_mv1");
            } catch (Exception e) {
                Assertions.fail(e.getMessage());
            }
            assertThat(mv.isActive()).isTrue();
        }

        {
            new MockUp<BaseTableInfo>() {
                @Mock
                public String getTableIdentifier() {
                    return t1.getTableIdentifier();
                }
            };
            refreshMaterializedView("test", "hive_mv1");
            assertThat(mv.isActive()).isTrue();
        }

        ConnectorPlanTestBase.dropCatalog("hive0");
        Config.enable_materialized_view_external_table_precise_refresh = false;
    }

    @Test
    public void testRepairFetchesPartitionInfoOnlyWhenSomePartitionIsUnchanged() {
        MaterializedView mv = Mockito.mock(MaterializedView.class);
        Mockito.when(mv.getRefreshScheme()).thenReturn(Mockito.mock(MaterializedView.MvRefreshScheme.class));
        BaseTableInfo baseTableInfo = Mockito.mock(BaseTableInfo.class);
        Mockito.when(baseTableInfo.getTableIdentifier()).thenReturn("orders:1");
        // An internal catalog ends the repair before it writes the mv's meta, which needs a real database.
        Mockito.when(baseTableInfo.isInternalCatalog()).thenReturn(true);
        Mockito.when(mv.getBaseTableRefreshInfo(baseTableInfo))
                .thenReturn(Map.of("p1", trackedFiles(1L), "p2", trackedFiles(1L)));
        HiveTable table = Mockito.mock(HiveTable.class);
        Mockito.when(table.getTableIdentifier()).thenReturn("orders:2");
        Mockito.when(table.getCatalogName()).thenReturn("hive_catalog");

        HiveMetadata hiveMetadata = Mockito.mock(HiveMetadata.class);
        MetadataMgr metadataMgr = Mockito.mock(MetadataMgr.class);
        Mockito.when(metadataMgr.getOptionalMetadata("hive_catalog"))
                .thenReturn(Optional.<ConnectorMetadata>of(hiveMetadata));
        AtomicInteger partitionInfoFetches = new AtomicInteger();
        new MockUp<PartitionUtil>() {
            @Mock
            public Map<String, PartitionInfo> getPartitionNameWithPartitionInfo(Table t, List<String> names) {
                partitionInfoFetches.incrementAndGet();
                return Map.of();
            }
        };

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        MetadataMgr originalMetadataMgr = globalStateMgr.getMetadataMgr();
        globalStateMgr.setMetadataMgr(metadataMgr);
        try {
            Mockito.when(hiveMetadata.getHivePartitionDataInfos(Mockito.eq(table), Mockito.anyList(), Mockito.anyInt()))
                    .thenReturn(Optional.of(Map.of("p1", Optional.of(new HivePartitionDataInfo(1L, 1)),
                            "p2", Optional.of(new HivePartitionDataInfo(2L, 1)))));
            new MVPCTMetaRepairer(null, mv).repairTableIfNeeded(table, baseTableInfo);
            Assertions.assertEquals(1, partitionInfoFetches.get(), "p1 is unchanged and needs the new partition info");

            Mockito.when(hiveMetadata.getHivePartitionDataInfos(Mockito.eq(table), Mockito.anyList(), Mockito.anyInt()))
                    .thenReturn(Optional.of(Map.of("p1", Optional.of(new HivePartitionDataInfo(2L, 1)),
                            "p2", Optional.of(new HivePartitionDataInfo(2L, 1)))));
            new MVPCTMetaRepairer(null, mv).repairTableIfNeeded(table, baseTableInfo);
            Assertions.assertEquals(1, partitionInfoFetches.get(), "every partition changed, nothing to fetch");
        } finally {
            globalStateMgr.setMetadataMgr(originalMetadataMgr);
        }
    }

    private static MaterializedView.BasePartitionInfo trackedFiles(long lastFileModifiedTime) {
        MaterializedView.BasePartitionInfo info = new MaterializedView.BasePartitionInfo(-1, 1, 1);
        info.setExtLastFileModifiedTime(lastFileModifiedTime);
        info.setFileNumber(1);
        return info;
    }
}