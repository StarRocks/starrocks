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
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.StarRocksException;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.brokerFileStatus;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.jsonResultBatch;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.varcharColumn;

/**
 * Coverage for the partition-column sampler extension: {@link FilesSampleSubqueryExecutor}
 * projects the request's {@code partitionSourceColumns} into the synthesized
 * SQL (sort-key first, partition-source trailing) and the JSON decoder splits
 * each row into a sort-key tuple and a parallel partition-source tuple. The
 * pre-extension callers that pass no partition-source columns must see no
 * behavior change.
 */
class FilesSampleSubqueryExecutorPartitionColumnTest {

    @Test
    void addsPartitionColumnToProjection() {
        Column sortKey = bigintColumn("user_id");
        Column partitionColumn = bigintColumn("ts");

        String sql = FilesSampleSubqueryExecutor.buildSampleSql(
                Map.of("path", "s3://bucket/foo", "format", "parquet"),
                List.of(sortKey),
                List.of(partitionColumn),
                /*samplingRate=*/ 0.1, /*rowLimit=*/ 100, /*seed=*/ 0L);

        Assertions.assertTrue(sql.contains("`user_id`"), "sort-key column must appear in projection: " + sql);
        Assertions.assertTrue(sql.contains("`ts`"), "partition-source column must appear in projection: " + sql);
        Assertions.assertTrue(sql.indexOf("`user_id`") < sql.indexOf("`ts`"),
                "sort-key column must precede partition-source column in projection: " + sql);
        Assertions.assertTrue(sql.contains("SELECT `user_id`, `ts` FROM FILES"),
                "projection list must be sort-key followed by partition-source, comma-separated: " + sql);
    }

    @Test
    void unpartitionedRequestKeepsCurrentSql() {
        Column sortKey = bigintColumn("user_id");

        String sql = FilesSampleSubqueryExecutor.buildSampleSql(
                Map.of("path", "s3://bucket/foo", "format", "parquet"),
                List.of(sortKey),
                List.of(),
                /*samplingRate=*/ 0.1, /*rowLimit=*/ 100, /*seed=*/ 0L);

        Assertions.assertTrue(sql.contains("`user_id`"));
        // No stray comma after the lone sort-key column.
        Assertions.assertTrue(sql.contains("SELECT `user_id` FROM FILES"),
                "single-column projection must not carry a trailing comma when no partition cols: " + sql);
    }

    @Test
    void unpartitionedRequestMatchesLegacyOverload() {
        // The legacy three-list-positional overload (no partition-source list)
        // and the new overload with an empty partition-source list must produce
        // byte-identical SQL — guards the backwards-compat surface explicitly.
        Column sortKey = bigintColumn("user_id");
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("path", "s3://bucket/foo");
        properties.put("format", "parquet");

        String legacy = FilesSampleSubqueryExecutor.buildSampleSql(
                properties, List.of(sortKey),
                /*samplingRate=*/ 0.25, /*rowLimit=*/ 200, /*seed=*/ 13L);
        String extended = FilesSampleSubqueryExecutor.buildSampleSql(
                properties, List.of(sortKey), List.of(),
                /*samplingRate=*/ 0.25, /*rowLimit=*/ 200, /*seed=*/ 13L);

        Assertions.assertEquals(legacy, extended);
    }

    @Test
    void multiplePartitionSourceColumnsAllProjected() {
        Column sortKey = bigintColumn("user_id");
        Column yearColumn = bigintColumn("year");
        Column monthColumn = bigintColumn("month");

        String sql = FilesSampleSubqueryExecutor.buildSampleSql(
                Map.of("path", "s3://bucket/foo", "format", "parquet"),
                List.of(sortKey),
                List.of(yearColumn, monthColumn),
                /*samplingRate=*/ 0.1, /*rowLimit=*/ 100, /*seed=*/ 0L);

        Assertions.assertTrue(sql.contains("`year`"));
        Assertions.assertTrue(sql.contains("`month`"));
        Assertions.assertTrue(sql.indexOf("`year`") < sql.indexOf("`month`"),
                "partition-source columns must preserve the request's declared order: " + sql);
        Assertions.assertTrue(sql.indexOf("`user_id`") < sql.indexOf("`year`"),
                "sort-key must precede all partition-source columns: " + sql);
    }

    @Test
    void propertyEscapeUnchangedWithPartitionColumns() {
        // The escape logic for property values (double-quote, backslash) must
        // still fire when the projection list is extended. Mirrors
        // InsertFromFilesSampleSubqueryExecutorTest#buildSampleSqlQuotesIdentifierAndEscapesProperties
        // with a non-empty partition-source list.
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("path", "s3://bucket/has\"quote/file*.parquet");
        properties.put("aws.s3.secret_key", "back\\slash");

        String sql = FilesSampleSubqueryExecutor.buildSampleSql(
                properties,
                List.of(bigintColumn("user_id")),
                List.of(bigintColumn("ts")),
                /*samplingRate=*/ 0.1, /*rowLimit=*/ 200_000, /*seed=*/ 42L);

        Assertions.assertTrue(sql.contains("\"path\" = \"s3://bucket/has\\\"quote/file*.parquet\""),
                "double-quote in property value must still be escaped after projection extension: " + sql);
        Assertions.assertTrue(sql.contains("\"aws.s3.secret_key\" = \"back\\\\slash\""),
                "backslash in property value must still be escaped after projection extension: " + sql);
        Assertions.assertTrue(sql.contains("rand(42)"),
                "seed must still flow into the rand() call after projection extension: " + sql);
    }

    @Test
    void decodesJsonRowsWithPartitionTuple() throws Exception {
        // Drive the full execute() path through the INSERT-from-FILES subclass so
        // the JSON envelope decoder receives a request with both sort-key arity
        // (1) and partition-source arity (1) and emits SampleRows whose two
        // tuples are split at the K boundary.
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://bucket/data/*.parquet", "format", "parquet"),
                List.of(brokerFileStatus("s3://bucket/data/a.parquet", 4L * 1024L * 1024L)));
        StringBuilder capturedSql = new StringBuilder();
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> {
                    capturedSql.append(sql);
                    return List.of(jsonResultBatch(
                            "{\"data\":[1, \"2026-05-26\"],\"meta\":["
                                    + "{\"name\":\"user_id\",\"type\":\"BIGINT\"},"
                                    + "{\"name\":\"ts\",\"type\":\"VARCHAR\"}]}",
                            "{\"data\":[2, \"2026-05-27\"]}"));
                });
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class)),
                /*sortKey=*/ List.of(bigintColumn("user_id")),
                /*partitionSourceColumns=*/ List.of(varcharColumn("ts")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(request);

        Assertions.assertTrue(capturedSql.toString().contains("SELECT `user_id`, `ts` FROM FILES"),
                "executor must project sort-key followed by partition-source column: " + capturedSql);
        List<SampleRow> rows = Lists.newArrayList(execution.rows());
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(1, rows.get(0).sortKeyTuple().size(),
                "sort-key tuple arity must match request.sortKey.size()");
        Assertions.assertEquals(1, rows.get(0).partitionSourceTuple().size(),
                "partition-source tuple arity must match request.partitionSourceColumns.size()");
        Assertions.assertEquals("1", rows.get(0).sortKeyTuple().get(0).getStringValue());
        Assertions.assertEquals("2026-05-26", rows.get(0).partitionSourceTuple().get(0).getStringValue());
        Assertions.assertEquals("2", rows.get(1).sortKeyTuple().get(0).getStringValue());
        Assertions.assertEquals("2026-05-27", rows.get(1).partitionSourceTuple().get(0).getStringValue());
    }

    @Test
    void decodedTuplesFlowThroughReservoirSamplerIntoSampleSet() throws Exception {
        // End-to-end: the ReservoirSampler must surface both tuples in parallel
        // lists on the SampleSet — proves the partition-source path is wired
        // through the FE-side accumulation layer, not just the executor.
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://bucket/data/*.parquet", "format", "parquet"),
                List.of(brokerFileStatus("s3://bucket/data/a.parquet", 4L * 1024L * 1024L)));
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of(jsonResultBatch(
                        "{\"data\":[10, \"east\"]}",
                        "{\"data\":[20, \"west\"]}")));
        ReservoirSampler sampler = new ReservoirSampler(executor);
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class)),
                /*sortKey=*/ List.of(bigintColumn("user_id")),
                /*partitionSourceColumns=*/ List.of(varcharColumn("region")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);

        SampleSet sampleSet = sampler.sample(request);

        Assertions.assertEquals(2, sampleSet.getTuples().size());
        Assertions.assertEquals(2, sampleSet.getPartitionSourceTuples().size(),
                "partitioned request must populate partitionSourceTuples 1:1 with sort-key tuples");
        Assertions.assertEquals("10", sampleSet.getTuples().get(0).getValues().get(0).getStringValue());
        Assertions.assertEquals("east",
                sampleSet.getPartitionSourceTuples().get(0).getValues().get(0).getStringValue());
        Assertions.assertEquals("20", sampleSet.getTuples().get(1).getValues().get(0).getStringValue());
        Assertions.assertEquals("west",
                sampleSet.getPartitionSourceTuples().get(1).getValues().get(0).getStringValue());
    }

    @Test
    void arityMismatchWithPartitionColumnsRequestedThrows() {
        // BE returns a row carrying sortKey-arity + partitionSource-arity + 1
        // cells; the executor must surface the projection-arity disagreement as
        // a clean StarRocksException rather than letting downstream tuple
        // assembly read past the end. Strengthens the arity-check branch in
        // extractDataArray for the partitioned-projection case.
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://bucket/data/*.parquet", "format", "parquet"),
                List.of(brokerFileStatus("s3://bucket/data/a.parquet", 1024L)));
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of(jsonResultBatch(
                        "{\"data\":[1, \"east\", \"unexpected\"]}")));
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class)),
                /*sortKey=*/ List.of(bigintColumn("user_id")),
                /*partitionSourceColumns=*/ List.of(varcharColumn("region")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class, () -> {
            SampleSubqueryExecutor.SampleExecution execution = executor.execute(request);
            // Force iterator consumption so the per-row decode runs.
            execution.rows().next();
        });
        Assertions.assertTrue(thrown.getMessage().contains("expected 2"),
                "arity-mismatch error must name the expected projection arity (sortKey + partitionSource): "
                        + thrown.getMessage());
    }

    @Test
    void nonNullablePartitionSourceNullCellNamesPartitionSourceRole() {
        // When a non-nullable partition-source column observes a null in the
        // sample, the error message must point operators at the partition-source
        // schema rather than the sort-key schema (regression guard for the
        // pre-fix shape that hard-coded "sort-key" into every null-rejection
        // message).
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://bucket/data/*.parquet", "format", "parquet"),
                List.of(brokerFileStatus("s3://bucket/data/a.parquet", 1024L)));
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of(jsonResultBatch(
                        "{\"data\":[1, null]}")));
        // bigintColumn(...) is non-nullable by default, used here as the
        // partition-source slot so a null cell hits the non-nullable branch.
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class)),
                /*sortKey=*/ List.of(bigintColumn("user_id")),
                /*partitionSourceColumns=*/ List.of(bigintColumn("region_id")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class, () -> {
            SampleSubqueryExecutor.SampleExecution execution = executor.execute(request);
            execution.rows().next();
        });
        Assertions.assertTrue(thrown.getMessage().contains("partition-source"),
                "null-on-non-nullable error must identify the partition-source role: "
                        + thrown.getMessage());
        Assertions.assertTrue(thrown.getMessage().contains("region_id"),
                "null-on-non-nullable error must name the offending column: " + thrown.getMessage());
        Assertions.assertFalse(thrown.getMessage().contains("sort-key"),
                "null-on-non-nullable error must not misattribute the role to sort-key: "
                        + thrown.getMessage());
    }

    private static TableFunctionTable mockSourceTable(
            Map<String, String> properties, List<com.starrocks.thrift.TBrokerFileStatus> fileStatuses) {
        TableFunctionTable sourceTable = Mockito.mock(TableFunctionTable.class);
        Mockito.when(sourceTable.getProperties()).thenReturn(properties);
        Mockito.when(sourceTable.loadFileList()).thenReturn(fileStatuses);
        return sourceTable;
    }
}
