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

package com.starrocks.alter;

import com.starrocks.catalog.Column;
import com.starrocks.common.Config;
import com.starrocks.lake.LakeTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.type.PrimitiveType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class LakeTableVarcharWidenSchemaChangeTest extends LakeFastSchemaChangeTestBase {
    private static final AtomicInteger TABLE_SUFFIX = new AtomicInteger(0);
    private boolean originalEnableFastSchemaEvolutionInSharedDataMode;

    @Override
    protected boolean isFastSchemaEvolutionV2() {
        return false;
    }

    @BeforeEach
    public void beforeEachTest() {
        connectContext.setThreadLocalInfo();
        connectContext.setDatabase(DB_NAME);
        originalEnableFastSchemaEvolutionInSharedDataMode = Config.enable_fast_schema_evolution_in_share_data_mode;
        Config.enable_fast_schema_evolution_in_share_data_mode = true;
        GlobalStateMgr.getCurrentState().getSchemaChangeHandler().clearJobs();
    }

    @AfterEach
    public void afterEachTest() {
        Config.enable_fast_schema_evolution_in_share_data_mode = originalEnableFastSchemaEvolutionInSharedDataMode;
        GlobalStateMgr.getCurrentState().getSchemaChangeHandler().clearJobs();
    }

    @Test
    public void testDistributionColumnFinalWidenCreatesAsyncFastSchemaChangeJob() throws Exception {
        LakeTable table = createDistributionTable(uniqueName("t_dist_fast"));

        AlterJobV2 job = executeAlterAndWaitFinish(table,
                "ALTER TABLE " + table.getName() + " MODIFY COLUMN k1 VARCHAR(30) KEY NOT NULL",
                true);

        Assertions.assertInstanceOf(LakeTableAsyncFastSchemaChangeJob.class, job);
        assertColumnType(table, table.getName(), "k1", PrimitiveType.VARCHAR, 30);
        assertIndexColumns(table, table.getName(), "k1", "k2", "v1");
    }

    @Test
    public void testDistributionColumnFinalNonWidenRejectedInSharedData() throws Exception {
        LakeTable table = createDistributionTable(uniqueName("t_dist_reject"));

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> alterTable(connectContext,
                        "ALTER TABLE " + table.getName() + " MODIFY COLUMN k1 INT KEY NOT NULL"));

        Assertions.assertTrue(exception.getMessage().contains("Can not modify distribution column[k1]"),
                exception.getMessage());
        assertColumnType(table, table.getName(), "k1", PrimitiveType.VARCHAR, 10);
        Assertions.assertTrue(
                GlobalStateMgr.getCurrentState().getSchemaChangeHandler()
                        .getUnfinishedAlterJobV2ByTableId(table.getId()).isEmpty());
    }

    @Test
    public void testKeyColumnNonWidenFallsBackToSchemaChangeJobInSharedData() throws Exception {
        LakeTable table = createKeyColumnTable(uniqueName("t_key_job"));

        AlterJobV2 job = executeAlterAndWaitFinish(table,
                "ALTER TABLE " + table.getName() + " MODIFY COLUMN k1 INT KEY NOT NULL",
                false);

        Assertions.assertInstanceOf(LakeTableSchemaChangeJob.class, job);
        assertColumnType(table, table.getName(), "k1", PrimitiveType.INT, 0);
        assertIndexColumns(table, table.getName(), "k1", "k2", "v1");
    }

    @Test
    public void testBaseClauseRollupColumnWidenCreatesAsyncFastSchemaChangeJob() throws Exception {
        LakeTable table = createRollupTable(uniqueName("t_rollup_fast"));

        AlterJobV2 job = executeAlterAndWaitFinish(table,
                "ALTER TABLE " + table.getName() + " MODIFY COLUMN k1 VARCHAR(30) KEY NOT NULL",
                true);

        Assertions.assertInstanceOf(LakeTableAsyncFastSchemaChangeJob.class, job);
        assertColumnType(table, table.getName(), "k1", PrimitiveType.VARCHAR, 30);
        assertColumnType(table, "r1", "k1", PrimitiveType.VARCHAR, 30);
        assertIndexColumns(table, "r1", "k2", "k1");
    }

    private static String uniqueName(String prefix) {
        return prefix + "_" + TABLE_SUFFIX.incrementAndGet();
    }

    private LakeTable createDistributionTable(String tableName) throws Exception {
        return createTable(connectContext, "CREATE TABLE " + tableName + " (\n"
                + "  k1 VARCHAR(10) NOT NULL,\n"
                + "  k2 INT NOT NULL,\n"
                + "  v1 INT\n"
                + ") DUPLICATE KEY(k1, k2)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES('replication_num'='1', 'cloud_native_fast_schema_evolution_v2'='false');");
    }

    private LakeTable createKeyColumnTable(String tableName) throws Exception {
        return createTable(connectContext, "CREATE TABLE " + tableName + " (\n"
                + "  k1 VARCHAR(10) NOT NULL,\n"
                + "  k2 INT NOT NULL,\n"
                + "  v1 INT\n"
                + ") DUPLICATE KEY(k1, k2)\n"
                + "DISTRIBUTED BY HASH(k2) BUCKETS 1\n"
                + "PROPERTIES('replication_num'='1', 'cloud_native_fast_schema_evolution_v2'='false');");
    }

    private LakeTable createRollupTable(String tableName) throws Exception {
        return createTable(connectContext, "CREATE TABLE " + tableName + " (\n"
                + "  k1 VARCHAR(10) NOT NULL,\n"
                + "  k2 INT NOT NULL,\n"
                + "  v1 INT\n"
                + ") DUPLICATE KEY(k1, k2)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "ROLLUP(\n"
                + "  r1(k2, k1)\n"
                + ")\n"
                + "PROPERTIES('replication_num'='1', 'cloud_native_fast_schema_evolution_v2'='false');");
    }

    private void assertColumnType(LakeTable table, String indexName, String columnName,
                                  PrimitiveType primitiveType, int expectedLength) {
        Column column = getColumn(table, indexName, columnName);
        Assertions.assertEquals(primitiveType, column.getPrimitiveType());
        if (primitiveType == PrimitiveType.VARCHAR) {
            Assertions.assertEquals(expectedLength, column.getStrLen());
        }
    }

    private Column getColumn(LakeTable table, String indexName, String columnName) {
        Long indexMetaId = table.getIndexMetaIdByName(indexName);
        Assertions.assertNotNull(indexMetaId, indexName);
        return table.getSchemaByIndexMetaId(indexMetaId).stream()
                .filter(column -> column.getName().equalsIgnoreCase(columnName))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "column " + columnName + " not found in index " + indexName));
    }

    private void assertIndexColumns(LakeTable table, String indexName, String... expectedColumns) {
        Long indexMetaId = table.getIndexMetaIdByName(indexName);
        Assertions.assertNotNull(indexMetaId, indexName);
        List<String> actualColumns = table.getSchemaByIndexMetaId(indexMetaId).stream()
                .map(Column::getName)
                .collect(Collectors.toList());
        Assertions.assertEquals(List.of(expectedColumns), actualColumns);
    }
}
