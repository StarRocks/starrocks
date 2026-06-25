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

package com.starrocks.alter.reshard.presplit;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.common.StarRocksException;
import com.starrocks.type.IntegerType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.jsonResultBatch;

class InternalPartitionSampleSubqueryExecutorTest {

    // ---------------------------------------------------------------------------
    // SQL-shape tests
    // ---------------------------------------------------------------------------

    @Test
    void buildsSelectFromInternalPartition() throws Exception {
        StringBuilder capturedSql = new StringBuilder();
        InternalPartitionSampleSubqueryExecutor executor = new InternalPartitionSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(partitionRequest("mydb", "mytbl", "p202401",
                List.of("k"), List.of(),
                /*partitionSizeBytes=*/ 0L,
                List.of(bigintColumn("k")), List.of()));

        // partitionSizeBytes=0 → rate = 1.0; seed=0 → order-shuffle seed = 6510615555426900570;
        // byte limit = Long.MAX_VALUE → row limit capped at SAMPLE_ROW_HARD_LIMIT = 200000.
        // No user WHERE → only the Bernoulli rand filter in the WHERE clause.
        Assertions.assertEquals(
                "SELECT `k` FROM `mydb`.`mytbl` PARTITION (`p202401`)"
                        + " WHERE rand(0) < 1.0 ORDER BY rand(6510615555426900570) LIMIT 200000",
                capturedSql.toString());
    }

    @Test
    void backtickQuotesBothDbAndTableAndPartition() throws Exception {
        StringBuilder capturedSql = new StringBuilder();
        InternalPartitionSampleSubqueryExecutor executor = new InternalPartitionSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(partitionRequest("my-db", "my-tbl", "p0",
                List.of("k"), List.of(), 0L,
                List.of(bigintColumn("k")), List.of()));

        String sql = capturedSql.toString();
        Assertions.assertTrue(sql.contains("`my-db`.`my-tbl` PARTITION (`p0`)"),
                "DB, table and partition must all be backtick-quoted: " + sql);
    }

    @Test
    void projectsPartitionColumnsAfterSortKey() throws Exception {
        StringBuilder capturedSql = new StringBuilder();
        InternalPartitionSampleSubqueryExecutor executor = new InternalPartitionSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(partitionRequest("db", "tbl", "p1",
                List.of("k"), List.of("dt"), 0L,
                List.of(bigintColumn("k")), List.of(bigintColumn("dt"))));

        Assertions.assertTrue(capturedSql.toString().contains("SELECT `k`, `dt` FROM"),
                "sort-key column then partition column in projection: " + capturedSql);
    }

    @Test
    void largePartitionSizeProducesSubUnitSamplingRate() throws Exception {
        // 10 GiB → rate < 1.0 so the WHERE carries rand(...) < <fraction>
        long tenGib = 10L * 1024L * 1024L * 1024L;
        StringBuilder capturedSql = new StringBuilder();
        InternalPartitionSampleSubqueryExecutor executor = new InternalPartitionSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(partitionRequest("db", "tbl", "p_large",
                List.of("k"), List.of(), tenGib,
                List.of(bigintColumn("k")), List.of()));

        String sql = capturedSql.toString();
        Assertions.assertFalse(sql.contains("rand(0) < 1.0"),
                "10 GiB partition must yield a sub-unit sampling rate: " + sql);
        Assertions.assertTrue(sql.contains("WHERE rand(0) <"),
                "Bernoulli filter must still appear: " + sql);
    }

    @Test
    void zeroPartitionSizeProducesValidSql() throws Exception {
        StringBuilder capturedSql = new StringBuilder();
        InternalPartitionSampleSubqueryExecutor executor = new InternalPartitionSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        Assertions.assertDoesNotThrow(() -> executor.execute(
                partitionRequest("db", "tbl", "p0",
                        List.of("k"), List.of(), 0L,
                        List.of(bigintColumn("k")), List.of())));

        Assertions.assertTrue(capturedSql.toString().startsWith("SELECT"),
                "must produce a SELECT even with zero partition size: " + capturedSql);
        Assertions.assertTrue(capturedSql.toString().contains("LIMIT"),
                "SELECT must include LIMIT: " + capturedSql);
    }

    // ---------------------------------------------------------------------------
    // Decode tests
    // ---------------------------------------------------------------------------

    @Test
    void decodesJsonRowsUsingTargetSortKeyTypes() throws Exception {
        Column targetIntColumn = new Column("k", IntegerType.INT);
        InternalPartitionSampleSubqueryExecutor executor = new InternalPartitionSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> List.of(jsonResultBatch("{\"data\":[\"42\"]}")));

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(
                partitionRequest("db", "tbl", "p0",
                        List.of("k"), List.of(), 0L,
                        List.of(targetIntColumn), List.of()));

        List<SampleRow> rows = Lists.newArrayList(execution.rows());
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("42", rows.get(0).sortKeyTuple().get(0).getStringValue(),
                "decoded value must match the JSON cell string");
    }

    @Test
    void decodesPartitionColumnsIntoPartitionSourceTuple() throws Exception {
        InternalPartitionSampleSubqueryExecutor executor = new InternalPartitionSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> List.of(
                        jsonResultBatch("{\"data\":[\"10\", \"20\"]}")));

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(
                partitionRequest("db", "tbl", "p0",
                        List.of("k"), List.of("dt"), 0L,
                        List.of(bigintColumn("k")), List.of(bigintColumn("dt"))));

        List<SampleRow> rows = Lists.newArrayList(execution.rows());
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(1, rows.get(0).sortKeyTuple().size());
        Assertions.assertEquals("10", rows.get(0).sortKeyTuple().get(0).getStringValue());
        Assertions.assertEquals(1, rows.get(0).partitionSourceTuple().size());
        Assertions.assertEquals("20", rows.get(0).partitionSourceTuple().get(0).getStringValue());
    }

    // ---------------------------------------------------------------------------
    // Error-path tests
    // ---------------------------------------------------------------------------

    @Test
    void wrongScanContextTypeThrows() {
        SampleRequest request = new SampleRequest(
                new BrokerLoadScanContext(
                        /*brokerDesc=*/ null,
                        List.of(),
                        List.of(),
                        Mockito.mock(ComputeResource.class)),
                List.of(bigintColumn("k")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);

        InternalPartitionSampleSubqueryExecutor executor = new InternalPartitionSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(request));
        Assertions.assertTrue(thrown.getMessage().contains("internal-partition "),
                "error message must contain ERROR_PREFIX: " + thrown.getMessage());
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private static SampleRequest partitionRequest(
            String dbName,
            String tableName,
            String partitionName,
            List<String> sortKeySourceColumnNames,
            List<String> partitionSourceColumnNames,
            long partitionSizeBytes,
            List<Column> sortKeyColumns,
            List<Column> partitionSourceColumns) {
        ComputeResource computeResource = Mockito.mock(ComputeResource.class);
        InternalPartitionScanContext scanContext = new InternalPartitionScanContext(
                dbName, tableName, partitionName,
                sortKeySourceColumnNames, partitionSourceColumnNames,
                partitionSizeBytes, computeResource);
        return new SampleRequest(
                scanContext, sortKeyColumns, partitionSourceColumns,
                /*sampleByteLimit=*/ Long.MAX_VALUE, /*seed=*/ 0L);
    }
}
