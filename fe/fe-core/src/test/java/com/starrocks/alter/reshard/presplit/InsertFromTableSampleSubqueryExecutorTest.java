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
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.StarRocksException;
import com.starrocks.type.IntegerType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.jsonResultBatch;

class InsertFromTableSampleSubqueryExecutorTest {

    // ---------------------------------------------------------------------------
    // SQL-shape tests
    // ---------------------------------------------------------------------------

    @Test
    void buildsSelectFromTableWithoutWhere() throws Exception {
        OlapTable sourceTable = mockOlapTable(/*dataSize=*/ 0L);
        StringBuilder capturedSql = new StringBuilder();
        InsertFromTableSampleSubqueryExecutor executor = new InsertFromTableSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(tableRequest(sourceTable, "`db`.`src`", List.of("k"), List.of(), /*where=*/ null,
                List.of(bigintColumn("k")), List.of()));

        // With seed=0 the order-shuffle seed is 0 ^ 0x5A5A5A5A5A5A5A5AL = 6510615555426900570;
        // dataSize=0 saturates the rate to 1.0; Long.MAX_VALUE byte limit pins the row limit to
        // the per-feature hard cap (TARGET_SAMPLE_ROW_COUNT * 4 = 200000). No user WHERE means
        // only the Bernoulli rand filter appears — no parenthesised predicate prefix.
        Assertions.assertEquals(
                "SELECT `k` FROM `db`.`src` WHERE rand(0) < 1.0 ORDER BY rand(6510615555426900570) LIMIT 200000",
                capturedSql.toString());
    }

    @Test
    void buildsSelectWithCopiedWherePredicate() throws Exception {
        // 10 GiB source → rate < 1.0; user predicate must be wrapped with AND rand(...)
        OlapTable sourceTable = mockOlapTable(10L * 1024L * 1024L * 1024L);
        StringBuilder capturedSql = new StringBuilder();
        InsertFromTableSampleSubqueryExecutor executor = new InsertFromTableSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(tableRequest(sourceTable, "`db`.`src`", List.of("k"), List.of(),
                "`a` > 10", List.of(bigintColumn("k")), List.of()));

        // The predicate must be wrapped and combined with the Bernoulli filter
        Assertions.assertTrue(capturedSql.toString().contains("WHERE (`a` > 10) AND rand(0) <"),
                "user WHERE must be parenthesised before AND rand: " + capturedSql);
        Assertions.assertTrue(capturedSql.toString().contains("ORDER BY rand("),
                "ORDER BY rand must follow WHERE: " + capturedSql);
    }

    @Test
    void projectsPartitionColumnsAfterSortKey() throws Exception {
        OlapTable sourceTable = mockOlapTable(0L);
        StringBuilder capturedSql = new StringBuilder();
        InsertFromTableSampleSubqueryExecutor executor = new InsertFromTableSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(tableRequest(sourceTable, "`db`.`src`", List.of("k"), List.of("dt"), /*where=*/ null,
                List.of(bigintColumn("k")), List.of(bigintColumn("dt"))));

        Assertions.assertTrue(capturedSql.toString().contains("SELECT `k`, `dt` FROM"),
                "sort-key column then partition column in projection: " + capturedSql);
    }

    @Test
    void zeroDataSizeStillBuildsValidSql() throws Exception {
        // getDataSize() == 0 → Math.max(0L, 0L) = 0 → pickSamplingRate(0) = 1.0;
        // the executor must not throw and must emit a syntactically complete SELECT.
        OlapTable sourceTable = mockOlapTable(0L);
        StringBuilder capturedSql = new StringBuilder();
        InsertFromTableSampleSubqueryExecutor executor = new InsertFromTableSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        Assertions.assertDoesNotThrow(() -> executor.execute(
                tableRequest(sourceTable, "`db`.`t`", List.of("k"), List.of(), /*where=*/ null,
                        List.of(bigintColumn("k")), List.of())));

        Assertions.assertTrue(capturedSql.toString().startsWith("SELECT"),
                "must produce a SELECT even with zero data-size: " + capturedSql);
        Assertions.assertTrue(capturedSql.toString().contains("LIMIT"),
                "SELECT must include LIMIT: " + capturedSql);
    }

    // ---------------------------------------------------------------------------
    // Decode tests
    // ---------------------------------------------------------------------------

    @Test
    void decodesJsonRowsUsingTargetSortKeyTypes() throws Exception {
        // The TARGET sort-key column declares INT type; the sample cell "5"
        // must decode to a Variant of int value 5.
        OlapTable sourceTable = mockOlapTable(0L);
        Column targetIntColumn = new Column("k", IntegerType.INT);
        InsertFromTableSampleSubqueryExecutor executor = new InsertFromTableSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> List.of(jsonResultBatch("{\"data\":[\"5\"]}")));

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(
                tableRequest(sourceTable, "`db`.`src`", List.of("k"), List.of(), /*where=*/ null,
                        List.of(targetIntColumn), List.of()));

        List<SampleRow> rows = Lists.newArrayList(execution.rows());
        Assertions.assertEquals(1, rows.size());
        // getStringValue() echoes the cell's original string form, not a numeric round-trip;
        // this asserts the cell was decoded into a Variant, not coerced to a canonical number.
        Assertions.assertEquals("5", rows.get(0).sortKeyTuple().get(0).getStringValue(),
                "decoded value must match the JSON cell string");
    }

    @Test
    void decodePartitionColumnsIntoPersistTuple() throws Exception {
        // Partition columns appear after sort key in the JSON data array;
        // they must land in the partition-source tuple, not the sort-key tuple.
        OlapTable sourceTable = mockOlapTable(0L);
        InsertFromTableSampleSubqueryExecutor executor = new InsertFromTableSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> List.of(
                        jsonResultBatch("{\"data\":[\"10\", \"20\"]}")));

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(
                tableRequest(sourceTable, "`db`.`src`", List.of("k"), List.of("dt"), /*where=*/ null,
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
        // A FilesContext wired to the table executor must produce a StarRocksException
        // with a message containing the ERROR_PREFIX so the coordinator can record it.
        SampleRequest request = new SampleRequest(
                new BrokerLoadScanContext(
                        /*brokerDesc=*/ null,
                        List.of(),
                        List.of(),
                        Mockito.mock(ComputeResource.class), "UTC"),
                List.of(bigintColumn("k")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);

        InsertFromTableSampleSubqueryExecutor executor = new InsertFromTableSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(request));
        Assertions.assertTrue(thrown.getMessage().contains("INSERT-from-table data tier"),
                "error message must contain ERROR_PREFIX: " + thrown.getMessage());
    }

    // ---------------------------------------------------------------------------
    // Source-name remap tests (base + rollup)
    // ---------------------------------------------------------------------------

    @Test
    void baseSortKeyProjectedBySourceNameFromMap() throws Exception {
        // target column "k" maps to source column "src_k"; projection must use the source name.
        OlapTable sourceTable = mockOlapTable(0L);
        StringBuilder capturedSql = new StringBuilder();
        InsertFromTableSampleSubqueryExecutor executor = new InsertFromTableSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(tableRequest(sourceTable, "`db`.`src`", List.of("src_k"), List.of(), /*where=*/ null,
                List.of(bigintColumn("k")), List.of()));

        Assertions.assertTrue(capturedSql.toString().startsWith("SELECT `src_k` FROM"),
                "base sort key must be projected by its mapped source name: " + capturedSql);
    }

    @Test
    void rollupSortKeyProjectedBySourceNameByName() throws Exception {
        // by-name identity map: base (k1,k2) + rollup (k2) all project by their own source names.
        OlapTable sourceTable = mockOlapTable(0L);
        StringBuilder capturedSql = new StringBuilder();
        InsertFromTableSampleSubqueryExecutor executor = new InsertFromTableSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(rollupRequest(sourceTable, Map.of("k1", "k1", "k2", "k2"),
                List.of(bigintColumn("k1"), bigintColumn("k2")),
                List.of(new SecondaryIndexSpec(101L, List.of(bigintColumn("k2"))))));

        Assertions.assertTrue(capturedSql.toString().startsWith("SELECT `k1`, `k2`, `k2` FROM"),
                "base (k1,k2) then rollup (k2), all by source name: " + capturedSql);
    }

    @Test
    void rollupSortKeyProjectedBySourceNameByPositionRenamed() throws Exception {
        // by-position renamed map: target k1->s1, k2->s2; rollup ORDER BY(k2) projects `s2`.
        OlapTable sourceTable = mockOlapTable(0L);
        StringBuilder capturedSql = new StringBuilder();
        InsertFromTableSampleSubqueryExecutor executor = new InsertFromTableSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(rollupRequest(sourceTable, Map.of("k1", "s1", "k2", "s2"),
                List.of(bigintColumn("k1"), bigintColumn("k2")),
                List.of(new SecondaryIndexSpec(101L, List.of(bigintColumn("k2"))))));

        Assertions.assertTrue(capturedSql.toString().startsWith("SELECT `s1`, `s2`, `s2` FROM"),
                "rollup sort key must remap to source names: " + capturedSql);
    }

    @Test
    void rollupReorderingSubsetOfBaseStillMapped() throws Exception {
        // rollup sort key reorders/subsets the base: base (k1,k2,k3) + rollup (k3,k1).
        OlapTable sourceTable = mockOlapTable(0L);
        StringBuilder capturedSql = new StringBuilder();
        InsertFromTableSampleSubqueryExecutor executor = new InsertFromTableSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(rollupRequest(sourceTable, Map.of("k1", "s1", "k2", "s2", "k3", "s3"),
                List.of(bigintColumn("k1"), bigintColumn("k2"), bigintColumn("k3")),
                List.of(new SecondaryIndexSpec(101L, List.of(bigintColumn("k3"), bigintColumn("k1"))))));

        Assertions.assertTrue(capturedSql.toString().startsWith("SELECT `s1`, `s2`, `s3`, `s3`, `s1` FROM"),
                "rollup (k3,k1) must project `s3`,`s1` after the base slice: " + capturedSql);
    }

    @Test
    void unmappedRollupColumnThrowsFailSafe() {
        // A rollup sort-key column absent from the map must fail the sample (-> load proceeds).
        OlapTable sourceTable = mockOlapTable(0L);
        InsertFromTableSampleSubqueryExecutor executor = new InsertFromTableSampleSubqueryExecutor(
                (sql, computeResource, ignoredTimeout) -> List.of());

        SampleRequest request = rollupRequest(sourceTable, Map.of("k1", "s1"),
                List.of(bigintColumn("k1")),
                List.of(new SecondaryIndexSpec(101L, List.of(bigintColumn("missing")))));

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(request));
        Assertions.assertTrue(thrown.getMessage().contains("no source-table column mapping"),
                "fail-safe throw must report an unmapped sort-key column: " + thrown.getMessage());
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private static OlapTable mockOlapTable(long dataSize) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getDataSize()).thenReturn(dataSize);
        return table;
    }

    private static SampleRequest tableRequest(
            OlapTable sourceTable,
            String sourceFromSql,
            List<String> sortKeySourceColumnNames,
            List<String> partitionSourceColumnNames,
            String wherePredicateSql,
            List<Column> sortKeyColumns,
            List<Column> partitionSourceColumns) {
        ComputeResource computeResource = Mockito.mock(ComputeResource.class);
        // Build the target->source map from the paired lists (both sort-key and partition columns):
        // sortKeyColumns[i].name -> sortKeySourceColumnNames[i], partitionSourceColumns[i].name -> ...[i].
        Map<String, String> targetToSource = new HashMap<>();
        for (int i = 0; i < sortKeyColumns.size(); i++) {
            targetToSource.put(sortKeyColumns.get(i).getName().toLowerCase(), sortKeySourceColumnNames.get(i));
        }
        for (int i = 0; i < partitionSourceColumns.size(); i++) {
            targetToSource.put(partitionSourceColumns.get(i).getName().toLowerCase(), partitionSourceColumnNames.get(i));
        }
        InsertFromTableScanContext scanContext = new InsertFromTableScanContext(
                sourceTable, sourceFromSql, targetToSource, wherePredicateSql, computeResource);
        return new SampleRequest(
                scanContext, sortKeyColumns, partitionSourceColumns,
                /*sampleByteLimit=*/ Long.MAX_VALUE, /*seed=*/ 0L);
    }

    /**
     * Builds a request whose context map covers every (lower-cased target -> source) pair
     * in {@code targetToSource}, with the given base sort key and rollup specs. Base sort-key
     * columns and each rollup spec carry TARGET columns; the executor remaps to source names.
     */
    private static SampleRequest rollupRequest(
            OlapTable sourceTable,
            Map<String, String> targetToSource,
            List<Column> baseSortKeyColumns,
            List<SecondaryIndexSpec> rollups) {
        ComputeResource computeResource = Mockito.mock(ComputeResource.class);
        InsertFromTableScanContext scanContext = new InsertFromTableScanContext(
                sourceTable, "`db`.`src`", targetToSource, /*where=*/ null, computeResource);
        return new SampleRequest(
                scanContext, baseSortKeyColumns, rollups, List.of(),
                /*sampleByteLimit=*/ Long.MAX_VALUE, /*seed=*/ 0L);
    }
}
