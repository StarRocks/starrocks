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

package com.starrocks.sql.plan;

import com.google.common.cache.CacheBuilder;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.delta.DeltaLakeCatalogProperties;
import com.starrocks.connector.delta.DeltaLakeEngine;
import com.starrocks.connector.delta.DeltaLakeMetadata;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.planner.DeltaLakeScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.dump.DumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.type.IntegerType;
import io.delta.kernel.internal.SnapshotImpl;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeltaLakeLimitPlanTest extends ConnectorPlanTestBase {
    private static final String CATALOG = "deltalake_catalog";
    private static final String TABLE = CATALOG + ".deltalake_db.tbl";
    private static final int FILE_COUNT = 2048;

    @TempDir
    public Path tablePath;
    private CountingMetadata metadata;
    private SessionVariable savedSessionVariable;

    @BeforeEach
    public void prepareDeltaTable() throws Exception {
        savedSessionVariable = connectContext.getSessionVariable();
        connectContext.setSessionVariable(new SessionVariable());
        connectContext.setThreadLocalInfo();
        Tracers.register(connectContext);
        Tracers.init(Tracers.Mode.VARS, Tracers.Module.EXTERNAL, true, false);
        metadata = createMetadata(FILE_COUNT);
        ((MockedMetadataMgr) connectContext.getGlobalStateMgr().getMetadataMgr())
                .registerMockedMetadata(CATALOG, metadata);
    }

    @AfterEach
    public void cleanup() {
        connectContext.setSessionVariable(savedSessionVariable);
        Tracers.close();
    }

    private CountingMetadata createMetadata(int fileCount) throws Exception {
        return createMetadata(fileCount, false);
    }

    private CountingMetadata createMetadata(int fileCount, boolean deletionVectors) throws Exception {
        Path logDir = Files.createDirectories(tablePath.resolve("_delta_log"));
        String schema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\","
                + "\"nullable\":true,\"metadata\":{}}]}";
        StringBuilder log = new StringBuilder();
        if (deletionVectors) {
            log.append("{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,"
                    + "\"readerFeatures\":[\"deletionVectors\"],\"writerFeatures\":[\"deletionVectors\"]}}\n");
        } else {
            log.append("{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n");
        }
        log.append(GsonUtils.GSON.toJson(Map.of("metaData", Map.of(
                "id", "limit-test", "format", Map.of("provider", "parquet", "options", Map.of()),
                "schemaString", schema, "partitionColumns", List.of(), "configuration", Map.of())))).append('\n');
        for (int i = 0; i < fileCount; i++) {
            // Metadata row counts must not cap execution: include empty files as well as non-empty ones.
            String stats = GsonUtils.GSON.toJson(Map.of("numRecords", i % 2 == 0 ? 0 : 100));
            Map<String, Object> add = new HashMap<>(Map.of(
                    "path", "part-" + i + ".parquet", "partitionValues", Map.of(), "size", 1000,
                    "modificationTime", 0, "dataChange", true, "stats", stats));
            if (deletionVectors && i == 1) {
                add.put("deletionVector", Map.of("storageType", "p", "pathOrInlineDv", "file:///test/dv.bin",
                        "offset", 0, "sizeInBytes", 10, "cardinality", 100));
            }
            log.append(GsonUtils.GSON.toJson(Map.of("add", add))).append('\n');
        }
        Files.writeString(logDir.resolve("00000000000000000000.json"), log);
        DeltaLakeEngine engine = DeltaLakeEngine.create(new Configuration(), new DeltaLakeCatalogProperties(Map.of()),
                CacheBuilder.newBuilder().build(), CacheBuilder.newBuilder().build());
        SnapshotImpl snapshot = (SnapshotImpl) io.delta.kernel.Table.forPath(engine, tablePath.toUri().toString())
                .getLatestSnapshot(engine);
        DeltaLakeTable table = new DeltaLakeTable(123456, CATALOG, "deltalake_db", "tbl",
                List.of(new Column("id", IntegerType.INT)), List.of(), snapshot, engine,
                new MetastoreTable("deltalake_db", "tbl", tablePath.toUri().toString(), 0));
        return new CountingMetadata(table);
    }

    private ExecPlan plan(String sql) throws Exception {
        return StatementPlanner.plan(SqlParser.parseSingleStatement(sql,
                connectContext.getSessionVariable().getSqlMode()), connectContext);
    }

    private long convertedFiles() {
        return Tracers.getAllVars().stream().filter(v -> v.getName().equals("DELTA_LAKE.convertedFiles"))
                .mapToLong(v -> ((Number) v.getValue()).longValue()).sum();
    }

    @Test
    public void testLimitPlansOnlyRequestedBatch() throws Exception {
        ExecPlan plan = plan("select id from " + TABLE + " limit 10");
        assertEquals(0, metadata.statisticsCalls);
        assertEquals(0, convertedFiles());
        assertTrue(Tracers.printVars().contains("DELTA_LAKE.limitStatistics"));
        DeltaLakeScanNode scan = (DeltaLakeScanNode) plan.getScanNodes().get(0);
        assertEquals(10, scan.getLimit());
        assertEquals(10, scan.getCardinality());
        assertEquals(500, scan.getScanRangeLocations(500).size());
        assertEquals(500, convertedFiles());
        assertTrue(scan.hasMoreScanRanges());
        // Simulate the existing BE completion signal. Cleanup must not drain the remaining files.
        scan.setReachLimit();
        assertFalse(scan.hasMoreScanRanges());
        CompletableFuture.runAsync(scan::clear).get();
        assertEquals(500, convertedFiles());
        scan.clear();
        assertEquals(500, convertedFiles());
        assertTrue(Tracers.printScopeTimer().contains("DELTA_LAKE.readScanFileBatch"));
    }

    @Test
    public void testEmptyTable() throws Exception {
        metadata = createMetadata(0);
        ((MockedMetadataMgr) connectContext.getGlobalStateMgr().getMetadataMgr())
                .registerMockedMetadata(CATALOG, metadata);
        ExecPlan plan = plan("select id from " + TABLE + " limit 10");
        assertEquals(0, metadata.statisticsCalls);
        DeltaLakeScanNode scan = (DeltaLakeScanNode) plan.getScanNodes().get(0);
        assertTrue(scan.getScanRangeLocations(500).isEmpty());
        assertFalse(scan.hasMoreScanRanges());
        scan.clear();
        assertEquals(0, convertedFiles());
    }

    @Test
    public void testLimitDoesNotTruncateFilesAndPreservesDeletionVector() throws Exception {
        metadata = createMetadata(20, true);
        ((MockedMetadataMgr) connectContext.getGlobalStateMgr().getMetadataMgr())
                .registerMockedMetadata(CATALOG, metadata);
        ExecPlan plan = plan("select id from " + TABLE + " limit 10");
        assertEquals(0, metadata.statisticsCalls);
        DeltaLakeScanNode scan = (DeltaLakeScanNode) plan.getScanNodes().get(0);
        List<TScanRangeLocations> ranges = scan.getScanRangeLocations(500);
        // Files declaring 100 records do not stop enumeration without actual BE completion.
        assertEquals(20, ranges.size());
        assertEquals(1, ranges.stream().filter(r -> r.getScan_range().getHdfs_scan_range()
                .isSetDeletion_vector_descriptor()).count());
        assertEquals(100, ranges.stream().map(r -> r.getScan_range().getHdfs_scan_range())
                .filter(r -> r.isSetDeletion_vector_descriptor()).findFirst().orElseThrow()
                .getDeletion_vector_descriptor().getCardinality());
        scan.clear();
    }

    @Test
    public void testProjectionOffsetAndRetry() throws Exception {
        ExecPlan plan = plan("select id + 1 from " + TABLE + " limit 10 offset 20");
        assertEquals(0, metadata.statisticsCalls);
        DeltaLakeScanNode scan = (DeltaLakeScanNode) plan.getScanNodes().get(0);
        assertEquals(30, scan.getLimit());
        assertEquals(30, scan.getCardinality());
        List<TScanRangeLocations> firstBatch = scan.getScanRangeLocations(5);
        scan.setReachLimit();
        scan.prepareRetry();
        assertTrue(scan.hasMoreScanRanges());
        assertEquals(firstBatch, scan.getScanRangeLocations(5));
        scan.clear();
        assertEquals(10, convertedFiles());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select id from %s",
            "select id from %s where id > 1 limit 10",
            "select id from %s order by id limit 10",
            "select count(*) from %s limit 10",
            "select distinct id from %s limit 10",
            "select row_number() over (order by id) from %s limit 10",
            "select (select max(id) from deltalake_catalog.deltalake_db.tbl) from %s limit 10",
            "select a.id from %s a join deltalake_catalog.deltalake_db.tbl b on a.id = b.id limit 10"
    })
    public void testOtherQueriesRetainMetadataStatistics(String sql) throws Exception {
        ExecPlan plan = plan(String.format(sql, TABLE));
        assertTrue(metadata.statisticsCalls > 0);
        assertTrue(convertedFiles() >= FILE_COUNT);
        plan.getScanNodes().forEach(scan -> scan.clear());
    }

    @Test
    public void testExplicitColumnStatisticsRetainsEnumeration() throws Exception {
        connectContext.getSessionVariable().setEnableDeltaLakeColumnStatistics(true);
        ExecPlan plan = plan("select id from " + TABLE + " limit 10");
        assertTrue(metadata.statisticsCalls > 0);
        assertTrue(convertedFiles() >= FILE_COUNT);
        plan.getScanNodes().forEach(scan -> scan.clear());
    }

    @Test
    public void testNonIncrementalScanRetainsEnumeration() throws Exception {
        connectContext.getSessionVariable().setEnableConnectorIncrementalScanRanges(false);
        ExecPlan plan = plan("select id from " + TABLE + " limit 10");
        assertTrue(metadata.statisticsCalls > 0);
        assertTrue(convertedFiles() >= FILE_COUNT);
        plan.getScanNodes().forEach(scan -> scan.clear());
    }

    @Test
    public void testFastPathPreservesQueryDumpStatistics() throws Exception {
        DumpInfo savedDump = connectContext.getDumpInfo();
        QueryDumpInfo dump = new QueryDumpInfo(connectContext);
        connectContext.setDumpInfo(dump);
        try {
            ExecPlan plan = plan("select id from " + TABLE + " limit 10");
            assertEquals(0, metadata.statisticsCalls);
            assertFalse(dump.getTableStatisticsMap().isEmpty());
            assertTrue(dump.getExternalTableRowCountMap().containsValue(10L));
            plan.getScanNodes().forEach(scan -> scan.clear());
        } finally {
            connectContext.setDumpInfo(savedDump);
        }
    }

    @Test
    public void testCollectedStatisticsRetainPriority() throws Exception {
        new MockUp<MetadataMgr>() {
            @Mock
            public Statistics getTableStatisticsFromInternalStatistics(Table table,
                                                                         Map<ColumnRefOperator, Column> columns) {
                Statistics.Builder builder = Statistics.builder().setOutputRowCount(50000);
                columns.keySet().forEach(column -> builder.addColumnStatistic(column,
                        new ColumnStatistic(0, 100, 0, 4, 100)));
                return builder.build();
            }
        };
        boolean savedUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
        try {
            ExecPlan plan = plan("select id from " + TABLE + " limit 10");
            assertEquals(0, metadata.statisticsCalls);
            assertFalse(Tracers.printVars().contains("DELTA_LAKE.limitStatistics"));
            plan.getScanNodes().forEach(scan -> scan.clear());
        } finally {
            FeConstants.runningUnitTest = savedUnitTest;
        }
    }

    private static class CountingMetadata extends DeltaLakeMetadata {
        private final DeltaLakeTable table;
        private int statisticsCalls;

        CountingMetadata(DeltaLakeTable table) {
            super(new HdfsEnvironment(Map.of()), CATALOG, null, null, new ConnectorProperties(ConnectorType.DELTALAKE));
            this.table = table;
        }

        @Override
        public Table getTable(ConnectContext context, String dbName, String tableName) {
            return table;
        }

        @Override
        public Database getDb(ConnectContext context, String dbName) {
            return new Database(123456, dbName);
        }

        @Override
        public Statistics getTableStatistics(OptimizerContext session, Table table, Map<ColumnRefOperator, Column> columns,
                                             List<PartitionKey> partitionKeys, ScalarOperator predicate, long limit,
                                             TvrVersionRange versionRange) {
            statisticsCalls++;
            return super.getTableStatistics(session, table, columns, partitionKeys, predicate, limit, versionRange);
        }

        @Override
        public void clear() {
        }
    }
}
