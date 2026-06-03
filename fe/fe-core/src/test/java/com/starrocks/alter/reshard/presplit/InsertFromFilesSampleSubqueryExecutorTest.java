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
import com.starrocks.catalog.NullVariant;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.Variant;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.type.IntegerType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.brokerFileStatus;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.jsonResultBatch;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.nullableBigintColumn;

class InsertFromFilesSampleSubqueryExecutorTest {

    @Test
    void happyPathDecodesProjectedRows() throws Exception {
        Column sortKeyColumn = bigintColumn("sort_key");
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "oss://bucket/data/*.parquet", "format", "parquet"),
                List.of(brokerFileStatus("oss://bucket/data/a.parquet", 4L * 1024L * 1024L)));

        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of(jsonResultBatch(
                        "{\"data\":[100],\"meta\":[{\"name\":\"sort_key\",\"type\":\"BIGINT\"}]}",
                        "{\"data\":[200]}",
                        "{\"data\":[300]}")));

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(
                bigintRequest(sourceTable, sortKeyColumn));

        List<List<Variant>> rows = Lists.newArrayList(execution.rows());
        Assertions.assertEquals(3, rows.size());
        Assertions.assertEquals("100", rows.get(0).get(0).getStringValue());
        Assertions.assertEquals("200", rows.get(1).get(0).getStringValue());
        Assertions.assertEquals("300", rows.get(2).get(0).getStringValue());
        Assertions.assertEquals(4L * 1024L * 1024L, execution.estimates().totalBytes());
    }

    @Test
    void emptyResultYieldsEmptyIteratorAndCarriesByteEstimate() throws Exception {
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://b/c/*.parquet", "format", "parquet"),
                List.of(brokerFileStatus("s3://b/c/x.parquet", 1024L)));

        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of());

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(
                bigintRequest(sourceTable, bigintColumn("sort_key")));

        Assertions.assertTrue(Lists.newArrayList(execution.rows()).isEmpty());
        Assertions.assertEquals(1024L, execution.estimates().totalBytes());
    }

    @Test
    void wrongScanContextTypeThrows() {
        SampleRequest request = new SampleRequest(
                new BrokerLoadScanContext(
                        /*brokerDesc=*/ null,
                        List.of(),
                        List.of(),
                        Mockito.mock(ComputeResource.class)),
                List.of(bigintColumn("sort_key")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);

        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of());

        Assertions.assertThrows(StarRocksException.class, () -> executor.execute(request));
    }

    @Test
    void queryRunnerStarRocksExceptionPropagatesVerbatim() {
        TableFunctionTable sourceTable = mockSourceTable(Map.of("format", "parquet"), List.of());
        StarRocksException injected = new StarRocksException("planner blew up");
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> {
                    throw injected;
                });

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(sourceTable, bigintColumn("sort_key"))));
        Assertions.assertSame(injected, thrown);
    }

    @Test
    void queryRunnerRuntimeExceptionIsWrapped() {
        TableFunctionTable sourceTable = mockSourceTable(Map.of("format", "parquet"), List.of());
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> {
                    throw new IllegalStateException("BE crashed");
                });

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(sourceTable, bigintColumn("sort_key"))));
        Assertions.assertTrue(thrown.getMessage().contains("BE crashed"),
                "wrapped message should preserve original cause: " + thrown.getMessage());
    }

    @Test
    void nullSortKeyValueOnNonNullColumnThrows() {
        // bigintColumn("sort_key") is non-null by default — a null sample
        // violates the schema invariant, sampler must reject.
        TableFunctionTable sourceTable = mockSourceTable(Map.of("format", "parquet"), List.of());
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of(jsonResultBatch("{\"data\":[null]}")));

        Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(sourceTable, bigintColumn("sort_key"))));
    }

    @Test
    void nullSortKeyValueOnNullableColumnDecodesToNullVariant() throws Exception {
        // ORDER BY can include nullable trailing columns; rejecting null cells
        // here would force SAMPLE_FAILED on every load with nulls. Variant
        // carries a first-class NullVariant subtype whose compareTo sorts
        // lower than any non-null value, so BoundaryPlanner handles it.
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://b/x/*.parquet", "format", "parquet"),
                List.of(brokerFileStatus("s3://b/x/a.parquet", 1024L)));
        Column nullableSortKey = nullableBigintColumn("trailing");
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of(jsonResultBatch(
                        "{\"data\":[null]}", "{\"data\":[42]}")));

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class)),
                List.of(nullableSortKey), /*sampleByteLimit=*/ Long.MAX_VALUE, /*seed=*/ 0L));

        List<List<Variant>> rows = Lists.newArrayList(execution.rows());
        Assertions.assertEquals(2, rows.size());
        Assertions.assertInstanceOf(NullVariant.class, rows.get(0).get(0),
                "nullable column null cell must decode to NullVariant");
        Assertions.assertEquals("42", rows.get(1).get(0).getStringValue());
    }

    @Test
    void multiColumnRowThrows() {
        TableFunctionTable sourceTable = mockSourceTable(Map.of("format", "parquet"), List.of());
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of(jsonResultBatch("{\"data\":[1,2]}")));

        Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(sourceTable, bigintColumn("sort_key"))));
    }

    @Test
    void malformedJsonThrows() {
        TableFunctionTable sourceTable = mockSourceTable(Map.of("format", "parquet"), List.of());
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of(jsonResultBatch("not json at all")));

        Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(sourceTable, bigintColumn("sort_key"))));
    }

    @Test
    void buildSampleSqlQuotesIdentifierAndEscapesProperties() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("path", "s3://bucket/has\"quote/file*.parquet");
        properties.put("aws.s3.secret_key", "back\\slash");
        Column sortKeyColumn = new Column("weird`name", IntegerType.BIGINT);

        String sql = FilesSampleSubqueryExecutor.buildSampleSql(
                properties, List.of(sortKeyColumn), /*samplingRate=*/ 0.1, /*rowLimit=*/ 200_000, /*seed=*/ 42L);

        Assertions.assertTrue(sql.contains("`weird``name`"), "backtick in identifier must be doubled: " + sql);
        // Both double-quote AND backslash must be escaped inside the property's
        // double-quoted literal, otherwise a crafted backslash before the
        // closing quote can break out of the string and inject SQL.
        Assertions.assertTrue(sql.contains("\"path\" = \"s3://bucket/has\\\"quote/file*.parquet\""),
                "double-quote in property value must be escaped: " + sql);
        Assertions.assertTrue(sql.contains("\"aws.s3.secret_key\" = \"back\\\\slash\""),
                "backslash in property value must be escaped: " + sql);
        Assertions.assertTrue(sql.contains("rand(42)"), "seed must be embedded in rand() call: " + sql);
        Assertions.assertTrue(sql.contains("ORDER BY rand("),
                "ORDER BY rand(...) must precede LIMIT to mitigate truncation bias: " + sql);
        Assertions.assertTrue(sql.contains("LIMIT 200000"), "row limit must appear in SQL: " + sql);
    }

    @Test
    void buildSampleSqlScalesRateForLargeInput() {
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://bucket/large/*.parquet", "format", "parquet"),
                List.of(brokerFileStatus("s3://bucket/large/x.parquet", /*size=*/ 10L * 1024L * 1024L * 1024L)));

        StringBuilder capturedSql = new StringBuilder();
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        Assertions.assertDoesNotThrow(() ->
                executor.execute(bigintRequest(sourceTable, bigintColumn("sort_key"))));

        // For 10 GiB input and the executor's target row count, the rate
        // must shrink well below 1.0 — otherwise the rate cap is broken.
        Assertions.assertTrue(capturedSql.toString().contains("rand(0)"), capturedSql.toString());
        Assertions.assertFalse(capturedSql.toString().contains("< 1.0 ORDER BY"),
                "rate should NOT saturate at 1.0 for 10 GiB input: " + capturedSql);
    }

    @Test
    void buildSampleSqlSaturatesRateForTinyInput() {
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://bucket/tiny/x.parquet", "format", "parquet"),
                List.of(brokerFileStatus("s3://bucket/tiny/x.parquet", /*size=*/ 32L)));

        StringBuilder capturedSql = new StringBuilder();
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        Assertions.assertDoesNotThrow(() ->
                executor.execute(bigintRequest(sourceTable, bigintColumn("sort_key"))));

        // Tiny inputs (well under TARGET * AVERAGE_ROW_BYTES) must saturate
        // the rate at 1.0 so the executor reads everything available.
        Assertions.assertTrue(capturedSql.toString().contains("rand(0) < 1.0 ORDER BY"),
                "rate should saturate at 1.0 for tiny input: " + capturedSql);
    }

    @Test
    void compositeSortKeyProjectsAllColumnsAndDecodesTuples() throws Exception {
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://bucket/data/*.parquet", "format", "parquet"),
                List.of(brokerFileStatus("s3://bucket/data/a.parquet", 4L * 1024L * 1024L)));
        StringBuilder capturedSql = new StringBuilder();
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> {
                    capturedSql.append(sql);
                    return List.of(jsonResultBatch(
                            "{\"data\":[100, 200],\"meta\":[{\"name\":\"tenant\"},{\"name\":\"position\"}]}",
                            "{\"data\":[100, 300]}"));
                });
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class)),
                List.of(bigintColumn("tenant"), bigintColumn("position")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(request);

        Assertions.assertTrue(capturedSql.toString().contains("SELECT `tenant`, `position` FROM FILES"),
                "both sort-key columns must appear in the projection: " + capturedSql);
        List<List<Variant>> rows = Lists.newArrayList(execution.rows());
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(2, rows.get(0).size(), "each decoded row carries a value per sort-key column");
        Assertions.assertEquals("100", rows.get(0).get(0).getStringValue());
        Assertions.assertEquals("200", rows.get(0).get(1).getStringValue());
        Assertions.assertEquals("100", rows.get(1).get(0).getStringValue());
        Assertions.assertEquals("300", rows.get(1).get(1).getStringValue());
    }

    @Test
    void compositeSortKeyArityMismatchInResultThrows() {
        TableFunctionTable sourceTable = mockSourceTable(Map.of("format", "parquet"), List.of());
        // Sampler claims 2 sort-key columns but server returns 1-value rows — surfaced
        // as a clean StarRocksException (mapped to SkipReason.SAMPLE_FAILED) rather
        // than letting downstream tuple compare blow up.
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of(jsonResultBatch("{\"data\":[100]}")));
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class)),
                List.of(bigintColumn("tenant"), bigintColumn("position")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);

        Assertions.assertThrows(StarRocksException.class, () -> executor.execute(request));
    }

    @Test
    void smallByteLimitShrinksRowLimitBelowHardCap() throws Exception {
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://b/x/*.parquet", "format", "parquet"),
                List.of(brokerFileStatus("s3://b/x/x.parquet", 32L)));

        StringBuilder capturedSql = new StringBuilder();
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        // 25_600 bytes / 256 bytes-per-row estimate = 100 rows, well below the 200_000 hard cap.
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class)),
                List.of(bigintColumn("sort_key")),
                /*sampleByteLimit=*/ 25_600L,
                /*seed=*/ 0L);
        executor.execute(request);

        Assertions.assertTrue(capturedSql.toString().contains("LIMIT 100"),
                "small byte cap must shrink LIMIT below the per-feature hard cap: " + capturedSql);
    }

    @Test
    void runnerReceivesComputeResourceFromScanContext() throws Exception {
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://b/x/*.parquet", "format", "parquet"),
                List.of(brokerFileStatus("s3://b/x/x.parquet", 1024L)));
        ComputeResource expectedComputeResource = Mockito.mock(ComputeResource.class);

        List<ComputeResource> capturedResources = new ArrayList<>();
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> {
                    capturedResources.add(computeResource);
                    return List.of();
                });
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, expectedComputeResource),
                List.of(bigintColumn("sort_key")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);

        executor.execute(request);
        Assertions.assertEquals(1, capturedResources.size());
        Assertions.assertSame(expectedComputeResource, capturedResources.get(0));
    }

    @Test
    void configureSampleContextAlignsWarehouseBeforeSettingResource() {
        // The production runner pins the ConnectContext to the load's compute
        // resource. Warehouse-id alignment MUST come first: ConnectContext
        // discards the resource on a mismatched warehouse during planning.
        ConnectContext sampleContext = Mockito.mock(ConnectContext.class);
        ComputeResource computeResource = Mockito.mock(ComputeResource.class);
        Mockito.when(computeResource.getWarehouseId()).thenReturn(42L);

        ConnectContext returned = FilesSampleSubqueryExecutor.configureSampleContext(
                sampleContext, computeResource);

        Assertions.assertSame(sampleContext, returned);
        InOrder inOrder = Mockito.inOrder(sampleContext);
        inOrder.verify(sampleContext).setCurrentWarehouseId(42L);
        inOrder.verify(sampleContext).setCurrentComputeResource(computeResource);
        inOrder.verify(sampleContext).setNeedQueued(false);
        inOrder.verify(sampleContext).setStartTime();
    }

    @Test
    void directoryEntriesDoNotContributeToByteTotal() throws Exception {
        TBrokerFileStatus directoryEntry = new TBrokerFileStatus(
                "s3://b/dir", /*isDir=*/ true, /*size=*/ 999_999L, /*isSplitable=*/ false);
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://b/dir/*.parquet", "format", "parquet"),
                List.of(directoryEntry, brokerFileStatus("s3://b/dir/x.parquet", 512L)));

        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of());

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(
                bigintRequest(sourceTable, bigintColumn("sort_key")));
        Assertions.assertEquals(512L, execution.estimates().totalBytes());
    }

    private static TableFunctionTable mockSourceTable(
            Map<String, String> properties, List<TBrokerFileStatus> fileStatuses) {
        TableFunctionTable sourceTable = Mockito.mock(TableFunctionTable.class);
        Mockito.when(sourceTable.getProperties()).thenReturn(properties);
        Mockito.when(sourceTable.loadFileList()).thenReturn(fileStatuses);
        return sourceTable;
    }

    private static SampleRequest bigintRequest(TableFunctionTable sourceTable, Column sortKeyColumn) {
        return new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class)),
                List.of(sortKeyColumn),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);
    }

}
