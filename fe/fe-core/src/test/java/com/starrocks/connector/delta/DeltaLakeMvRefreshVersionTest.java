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

import com.starrocks.catalog.Table;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import io.delta.kernel.internal.SnapshotImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * An MV over a Delta Lake table, refreshed through the regular refresh task, is used for rewrite only while the
 * table is at the version the refresh read.
 */
public class DeltaLakeMvRefreshVersionTest extends MVTestBase {
    private static final String CATALOG = "delta_version_catalog";
    private static final String DB = "delta_db";
    private static final String UNPARTITIONED = "tbl";
    private static final String PARTITIONED = "ptbl";

    private static final Map<String, Path> TABLE_PATHS = new HashMap<>();
    private static final Map<String, Long> LAST_VERSIONS = new HashMap<>();
    private static final Map<String, Table> CURRENT_TABLES = new HashMap<>();

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        createTable(UNPARTITIONED, "col2", "", "");
        // the data file isn't read: listing the partitions only reads the log
        createTable(PARTITIONED, "p", "\"p\"",
                "{\"add\":{\"path\":\"p=x/f.parquet\",\"partitionValues\":{\"p\":\"x\"},\"size\":1," +
                        "\"modificationTime\":1700000000000,\"dataChange\":true}}\n");

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
        ((MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr()).unregisterMockedMetadata(CATALOG);
        for (Path tablePath : TABLE_PATHS.values()) {
            try (Stream<Path> paths = Files.walk(tablePath)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
            }
        }
    }

    private static final class LocalDeltaLakeMetadata extends ConnectorPlanTestBase.MockedDeltaLakeMetadata {
        @Override
        public Table getTable(ConnectContext context, String dbName, String tblName) {
            return DB.equalsIgnoreCase(dbName) ? CURRENT_TABLES.get(tblName.toLowerCase()) : null;
        }
    }

    /**
     * A table of an int column col1 and a string column named secondColumn, which is partitioned by it if
     * partitionColumns is non-empty.
     */
    private static void createTable(String name, String secondColumn, String partitionColumns, String addActions)
            throws Exception {
        TABLE_PATHS.put(name, Files.createTempDirectory("delta_mv_version_" + name));
        writeCommit(name, 0, "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n" +
                "{\"metaData\":{\"id\":\"" + UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8)) +
                "\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"{\\\"type\\\":" +
                "\\\"struct\\\",\\\"fields\\\":[" +
                "{\\\"name\\\":\\\"col1\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true," +
                "\\\"metadata\\\":{}}," +
                "{\\\"name\\\":\\\"" + secondColumn + "\\\",\\\"type\\\":\\\"string\\\"," +
                "\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\"," +
                "\"partitionColumns\":[" + partitionColumns +
                "],\"configuration\":{},\"createdTime\":1700000000000}}\n" + addActions);
    }

    private static void writeCommit(String name, long version, String actions) throws Exception {
        Path tablePath = TABLE_PATHS.get(name);
        Path log = Files.createDirectories(tablePath.resolve("_delta_log"));
        Files.writeString(log.resolve(String.format("%020d.json", version)), actions +
                "{\"commitInfo\":{\"timestamp\":" + (1700000000000L + version) + ",\"operation\":\"WRITE\"}}\n",
                StandardCharsets.UTF_8);
        LAST_VERSIONS.put(name, version);

        DeltaLakeEngine engine = DeltaLakeEngine.create(new Configuration(false), new DeltaLakeCatalogProperties(Map.of(
                DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_JSON_META_CACHE, "false",
                DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_CHECKPOINT_META_CACHE, "false")), null, null);
        String path = tablePath.toUri().toString();
        SnapshotImpl snapshot = (SnapshotImpl) io.delta.kernel.Table.forPath(engine, path).getLatestSnapshot(engine);
        Table table = DeltaUtils.convertDeltaSnapshotToSRTable(CATALOG,
                new DeltaLakeSnapshot(DB, name, engine, snapshot, new MetastoreTable(DB, name, path, 1700000000L)));
        // a connector re-attaches the table's MVs to each loaded table object, the mocked catalog doesn't
        Table previous = CURRENT_TABLES.put(name, table);
        if (previous != null) {
            previous.getRelatedMaterializedViews().forEach(table::addRelatedMaterializedView);
        }
    }

    @Test
    public void testMvIsUsedOnlyWhileTheTableIsAtTheRefreshedVersion() throws Exception {
        assertMvIsUsedOnlyWhileTheTableIsAtTheRefreshedVersion(UNPARTITIONED, "col2");
    }

    @Test
    public void testMvOverPartitionedTableIsUsedOnlyWhileTheTableIsAtTheRefreshedVersion() throws Exception {
        assertMvIsUsedOnlyWhileTheTableIsAtTheRefreshedVersion(PARTITIONED, "p");
    }

    private void assertMvIsUsedOnlyWhileTheTableIsAtTheRefreshedVersion(String table, String secondColumn)
            throws Exception {
        String mvName = "delta_version_mv_" + table;
        String tableName = CATALOG + "." + DB + "." + table;
        String query = "SELECT col1, " + secondColumn + " FROM " + tableName + " WHERE col1 > 10";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n" +
                "DISTRIBUTED BY HASH(col1)\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"replication_num\" = \"1\")\n" +
                "AS SELECT col1, " + secondColumn + " FROM " + tableName);
        try {
            PlanTestBase.assertNotContains(getFragmentPlan(query), mvName);

            refreshMaterializedView("test", mvName);
            PlanTestBase.assertContains(getFragmentPlan(query), mvName);

            writeCommit(table, LAST_VERSIONS.get(table) + 1, "");
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, mvName);
            PlanTestBase.assertContains(plan, "DeltaLakeScanNode");

            refreshMaterializedView("test", mvName);
            PlanTestBase.assertContains(getFragmentPlan(query), mvName);
        } finally {
            dropMv("test", mvName);
        }
    }
}
