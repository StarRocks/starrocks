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
import java.util.Map;
import java.util.stream.Stream;

/**
 * An MV over a Delta Lake table, refreshed through the regular refresh task, is used for rewrite only while the
 * table is at the version the refresh read.
 */
public class DeltaLakeMvRefreshVersionTest extends MVTestBase {
    private static final String CATALOG = "delta_version_catalog";
    private static final String DB = "delta_db";
    private static final String TBL = CATALOG + "." + DB + ".tbl";
    private static final String QUERY = "SELECT col1, col2 FROM " + TBL + " WHERE col1 > 10";

    private static Path tablePath;
    private static long lastVersion;
    private static volatile Table currentTable;

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        tablePath = Files.createTempDirectory("delta_mv_version");
        writeCommit(0, "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n" +
                "{\"metaData\":{\"id\":\"8c3a9b4e-1f2d-4c5b-9e7a-0d1c2b3a4f5e\",\"format\":{\"provider\":\"parquet\"," +
                "\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[" +
                "{\\\"name\\\":\\\"col1\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}," +
                "{\\\"name\\\":\\\"col2\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\"," +
                "\"partitionColumns\":[],\"configuration\":{},\"createdTime\":1700000000000}}\n");

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
        try (Stream<Path> paths = Files.walk(tablePath)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
        }
    }

    private static final class LocalDeltaLakeMetadata extends ConnectorPlanTestBase.MockedDeltaLakeMetadata {
        @Override
        public Table getTable(ConnectContext context, String dbName, String tblName) {
            return DB.equalsIgnoreCase(dbName) && "tbl".equalsIgnoreCase(tblName) ? currentTable : null;
        }
    }

    private static void writeCommit(long version, String actions) throws Exception {
        Path log = Files.createDirectories(tablePath.resolve("_delta_log"));
        Files.writeString(log.resolve(String.format("%020d.json", version)), actions +
                "{\"commitInfo\":{\"timestamp\":" + (1700000000000L + version) + ",\"operation\":\"WRITE\"}}\n",
                StandardCharsets.UTF_8);
        lastVersion = version;

        DeltaLakeEngine engine = DeltaLakeEngine.create(new Configuration(false), new DeltaLakeCatalogProperties(Map.of(
                DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_JSON_META_CACHE, "false",
                DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_CHECKPOINT_META_CACHE, "false")), null, null);
        String path = tablePath.toUri().toString();
        SnapshotImpl snapshot = (SnapshotImpl) io.delta.kernel.Table.forPath(engine, path).getLatestSnapshot(engine);
        Table table = DeltaUtils.convertDeltaSnapshotToSRTable(CATALOG,
                new DeltaLakeSnapshot(DB, "tbl", engine, snapshot, new MetastoreTable(DB, "tbl", path, 1700000000L)));
        // a connector re-attaches the table's MVs to each loaded table object, the mocked catalog doesn't
        if (currentTable != null) {
            currentTable.getRelatedMaterializedViews().forEach(table::addRelatedMaterializedView);
        }
        currentTable = table;
    }

    @Test
    public void testMvIsUsedOnlyWhileTheTableIsAtTheRefreshedVersion() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW delta_version_mv\n" +
                "DISTRIBUTED BY HASH(col1)\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\"replication_num\" = \"1\")\n" +
                "AS SELECT col1, col2 FROM " + TBL);
        try {
            PlanTestBase.assertNotContains(getFragmentPlan(QUERY), "delta_version_mv");

            refreshMaterializedView("test", "delta_version_mv");
            PlanTestBase.assertContains(getFragmentPlan(QUERY), "delta_version_mv");

            writeCommit(lastVersion + 1, "");
            String plan = getFragmentPlan(QUERY);
            PlanTestBase.assertNotContains(plan, "delta_version_mv");
            PlanTestBase.assertContains(plan, "DeltaLakeScanNode");

            refreshMaterializedView("test", "delta_version_mv");
            PlanTestBase.assertContains(getFragmentPlan(QUERY), "delta_version_mv");
        } finally {
            dropMv("test", "delta_version_mv");
        }
    }
}
