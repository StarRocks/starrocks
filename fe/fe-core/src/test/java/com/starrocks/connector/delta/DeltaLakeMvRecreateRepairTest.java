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

package com.starrocks.connector.delta;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.persist.AlterMaterializedViewBaseTableInfosLog;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.mv.pct.MVPCTMetaRepairer;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import io.delta.kernel.internal.SnapshotImpl;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Replacing a Delta table (CREATE OR REPLACE) gives it a new Delta table id. Refreshing an MV over it must repair
 * the stored table identifier instead of failing and setting the MV inactive.
 */
class DeltaLakeMvRecreateRepairTest extends MVTestBase {
    private static final String CATALOG = "delta_recreate_catalog";
    private static final String DB = "delta_db";
    private static final String TBL = CATALOG + "." + DB + ".tbl";

    private static Path tablePath;
    private static long lastVersion = -1;
    private static volatile Table currentTable;
    // what the metadata cache returns until the table is refreshed
    private static volatile Table cachedTable;

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        tablePath = Files.createTempDirectory("delta_mv_recreate");
        writeMetadataCommit("8c3a9b4e-1f2d-4c5b-9e7a-0d1c2b3a4f5e");

        CatalogMgr catalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
        if (!catalogMgr.catalogExists(CATALOG)) {
            catalogMgr.createCatalog(Table.TableType.DELTALAKE.name(), CATALOG, "",
                    Map.of("type", Table.TableType.DELTALAKE.name(), "hive.metastore.uris", "thrift://127.0.0.1:9083"));
        }
        ((MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr())
                .registerMockedMetadata(CATALOG, new LocalDeltaLakeMetadata());
    }

    @AfterAll
    public static void afterClass() throws Exception {
        ConnectorPlanTestBase.dropCatalog(CATALOG);
        try (Stream<Path> paths = Files.walk(tablePath)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
        }
    }

    private static final class LocalDeltaLakeMetadata extends ConnectorPlanTestBase.MockedDeltaLakeMetadata {
        @Override
        public Table getTable(ConnectContext context, String dbName, String tblName) {
            if (!DB.equalsIgnoreCase(dbName) || !"tbl".equalsIgnoreCase(tblName)) {
                return null;
            }
            return cachedTable != null ? cachedTable : currentTable;
        }

        @Override
        public void refreshTable(String srDbName, Table table, List<String> partitionNames,
                                 boolean onlyCachedPartitions) {
            cachedTable = null;
        }
    }

    // A commit with a metaData action, as written by CREATE (OR REPLACE) TABLE.
    private static void writeMetadataCommit(String tableId) throws Exception {
        long version = lastVersion + 1;
        Path log = Files.createDirectories(tablePath.resolve("_delta_log"));
        String protocol = version == 0 ? "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n" : "";
        Files.writeString(log.resolve(String.format("%020d.json", version)), protocol +
                "{\"metaData\":{\"id\":\"" + tableId + "\",\"format\":{\"provider\":\"parquet\"," +
                "\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[" +
                "{\\\"name\\\":\\\"col1\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}," +
                "{\\\"name\\\":\\\"col2\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\"," +
                "\"partitionColumns\":[],\"configuration\":{},\"createdTime\":1700000000000}}\n" +
                "{\"commitInfo\":{\"timestamp\":" + (1700000000000L + version) + ",\"operation\":\"CREATE OR REPLACE " +
                "TABLE\"}}\n", StandardCharsets.UTF_8);
        lastVersion = version;

        DeltaLakeEngine engine = DeltaLakeEngine.create(new Configuration(false),
                new DeltaLakeCatalogProperties(Map.of(
                        DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_JSON_META_CACHE, "false",
                        DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_CHECKPOINT_META_CACHE, "false")), null, null);
        String path = tablePath.toUri().toString();
        SnapshotImpl snapshot = (SnapshotImpl) io.delta.kernel.Table.forPath(engine, path).getLatestSnapshot(engine);
        currentTable = DeltaUtils.convertDeltaSnapshotToSRTable(CATALOG, new DeltaLakeSnapshot(DB, "tbl", engine,
                snapshot, new MetastoreTable(DB, "tbl", path, 1700000000L)));
    }

    @Test
    void replacedTableIsRepairedOnFirstRefresh() throws Exception {
        // a single attempt, as in production; unit tests retry by default, which would hide a failed first attempt
        new MockUp<QueryDebugOptions>() {
            @Mock
            public int getMaxRefreshMaterializedViewRetryNum() {
                return 1;
            }
        };
        List<AlterMaterializedViewBaseTableInfosLog> editLogs = new ArrayList<>();
        new MockUp<EditLog>() {
            @Mock
            public void logAlterMvBaseTableInfos(AlterMaterializedViewBaseTableInfosLog log) {
                editLogs.add(log);
            }
        };
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW delta_recreate_mv\n" +
                "DISTRIBUTED BY HASH(col1)\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"replication_num\" = \"1\")\n" +
                "AS SELECT col1, col2 FROM " + TBL);
        try {
            refreshMaterializedView("test", "delta_recreate_mv");
            MaterializedView mv = getMv("test", "delta_recreate_mv");
            String oldIdentifier = baseTableInfo(mv).getTableIdentifier();

            // replace the table; the metadata cache still holds the old table until the refresh reloads it
            Table oldTable = currentTable;
            writeMetadataCommit("2b7e1c5d-9a4f-4e3b-8d6c-1f0a9e8b7c6d");
            cachedTable = oldTable;
            String newIdentifier = currentTable.getTableIdentifier();
            assertNotEquals(oldIdentifier, newIdentifier);
            assertTrue(MVPCTMetaRepairer.isSupportedPCTRepairer(currentTable, baseTableInfo(mv)));

            refreshMaterializedView("test", "delta_recreate_mv");

            assertNull(cachedTable);
            assertTrue(mv.isActive(), mv.getInactiveReason());
            assertEquals(newIdentifier, baseTableInfo(mv).getTableIdentifier());
            // the repaired base table info is persisted for followers
            assertEquals(1, editLogs.size());
            assertEquals(newIdentifier, editLogs.get(0).getBaseTableInfos().stream()
                    .filter(info -> CATALOG.equals(info.getCatalogName())).findFirst().orElseThrow()
                    .getTableIdentifier());
        } finally {
            dropMv("test", "delta_recreate_mv");
        }
    }

    private static BaseTableInfo baseTableInfo(MaterializedView mv) {
        return mv.getBaseTableInfos().stream()
                .filter(info -> CATALOG.equals(info.getCatalogName())).findFirst().orElseThrow();
    }
}
