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
import com.starrocks.catalog.DecimalVariant;
import com.starrocks.catalog.NullVariant;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.brokerFileStatus;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.jsonResultBatch;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.nullableBigintColumn;

class InsertFromFilesSampleSubqueryExecutorTest {

    private static final long GIB = 1L << 30;
    private long savedByteLimit;
    private int savedMinFiles;

    @BeforeEach
    void setScanLimits() {
        savedByteLimit = Config.tablet_pre_split_data_tier_scan_byte_limit;
        savedMinFiles = Config.tablet_pre_split_data_tier_min_scan_files;
        Config.tablet_pre_split_data_tier_scan_byte_limit = 3 * GIB;
        Config.tablet_pre_split_data_tier_min_scan_files = 1;
    }

    @AfterEach
    void restoreScanLimits() {
        Config.tablet_pre_split_data_tier_scan_byte_limit = savedByteLimit;
        Config.tablet_pre_split_data_tier_min_scan_files = savedMinFiles;
    }

    @Test
    void aSubsetReplacesOnlyThePathPropertyAndLeavesTheStatementsMapAlone() throws Exception {
        Map<String, String> properties = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        properties.put("path", "s3://b/d/*");
        properties.put("format", "parquet");
        properties.put("aws.s3.region", "us-west-2");
        TableFunctionTable sourceTable = mockSourceTable(properties, tenFiles("s3://b/d/f"));
        StringBuilder capturedSql = new StringBuilder();

        SampleSubqueryExecutor.SampleExecution execution = capturingExecutor(capturedSql).execute(
                bigintRequest(sourceTable, bigintColumn("sort_key")));

        String sql = capturedSql.toString();
        Assertions.assertTrue(sql.contains(
                "\"path\" = \"s3://b/d/f2.parquet,s3://b/d/f5.parquet,s3://b/d/f7.parquet\""), sql);
        Assertions.assertTrue(sql.contains("\"format\" = \"parquet\""), sql);
        Assertions.assertTrue(sql.contains("\"aws.s3.region\" = \"us-west-2\""), sql);
        Assertions.assertTrue(sql.contains("rand(0) < " + AbstractSqlSampleSubqueryExecutor.pickSamplingRate(3 * GIB)
                + " ORDER BY"), sql);
        Assertions.assertEquals("s3://b/d/*", properties.get("path"), "the statement's own map is not modified");
        Assertions.assertEquals(10 * GIB, execution.estimates().totalBytes());
    }

    @Test
    void userSpelledPathKeyIsReplacedNotDuplicated() throws Exception {
        Map<String, String> properties = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        properties.put("PATH", "s3://b/d/*");
        properties.put("format", "parquet");
        StringBuilder capturedSql = new StringBuilder();

        capturingExecutor(capturedSql).execute(bigintRequest(
                mockSourceTable(properties, tenFiles("s3://b/d/f")), bigintColumn("sort_key")));

        String sql = capturedSql.toString();
        Assertions.assertTrue(sql.contains("\"PATH\" = \"s3://b/d/f2.parquet,"), sql);
        Assertions.assertFalse(sql.contains("s3://b/d/*"), sql);
        Assertions.assertFalse(sql.contains("\"path\" ="), sql);
    }

    @Test
    void aSelectedPathFilesCannotReadExactlyKeepsTheStatementsProperties() throws Exception {
        List<TBrokerFileStatus> files = tenFiles("s3://b/d/f");
        files.set(5, brokerFileStatus("s3://b/d/f5[x].parquet", GIB));
        StringBuilder capturedSql = new StringBuilder();

        SampleSubqueryExecutor.SampleExecution execution = capturingExecutor(capturedSql).execute(bigintRequest(
                mockSourceTable(Map.of("path", "s3://b/d/*", "format", "parquet"), files), bigintColumn("sort_key")));

        Assertions.assertTrue(capturedSql.toString().contains("\"path\" = \"s3://b/d/*\""), capturedSql.toString());
        Assertions.assertTrue(capturedSql.toString().contains(
                "rand(0) < " + AbstractSqlSampleSubqueryExecutor.pickSamplingRate(10 * GIB) + " ORDER BY"));
        Assertions.assertEquals(10 * GIB, execution.estimates().totalBytes());
    }

    @Test
    void inputWithinTheScanLimitKeepsTheStatementsPropertiesAndSql() throws Exception {
        Map<String, String> properties = Map.of("path", "s3://b/d/*", "format", "parquet");
        StringBuilder sqlWithinLimit = new StringBuilder();
        StringBuilder sqlWithLimitDisabled = new StringBuilder();
        Config.tablet_pre_split_data_tier_scan_byte_limit = 20 * GIB;

        capturingExecutor(sqlWithinLimit).execute(bigintRequest(
                mockSourceTable(properties, tenFiles("s3://b/d/f")), bigintColumn("sort_key")));
        Config.tablet_pre_split_data_tier_scan_byte_limit = 0L;
        capturingExecutor(sqlWithLimitDisabled).execute(bigintRequest(
                mockSourceTable(properties, tenFiles("s3://b/d/f")), bigintColumn("sort_key")));

        Assertions.assertTrue(sqlWithinLimit.toString().contains("\"path\" = \"s3://b/d/*\""));
        Assertions.assertEquals(sqlWithLimitDisabled.toString(), sqlWithinLimit.toString());
    }

    @Test
    void theStatementsWhereClauseStillAppliesToASubset() throws Exception {
        StringBuilder capturedSql = new StringBuilder();

        capturingExecutor(capturedSql).execute(new SampleRequest(
                new InsertFromFilesScanContext(mockSourceTable(Map.of("path", "s3://b/d/*", "format", "parquet"),
                        tenFiles("s3://b/d/f")), Mockito.mock(ComputeResource.class), "UTC", Map.of(), "`k` > 5"),
                List.of(bigintColumn("k")), Long.MAX_VALUE, 0L));

        Assertions.assertTrue(capturedSql.toString().contains("s3://b/d/f2.parquet,"), capturedSql.toString());
        Assertions.assertTrue(capturedSql.toString().contains("WHERE (`k` > 5) AND rand(0) <"), capturedSql.toString());
    }

    @Test
    void aPartitionColumnReadFromThePathIsStratifiedThroughTheColumnMapping() throws Exception {
        Config.tablet_pre_split_data_tier_scan_byte_limit = 5 * GIB;
        List<TBrokerFileStatus> files = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            files.add(brokerFileStatus("s3://b/file_dt=2026-09-11/f" + i + ".parquet", GIB));
        }
        for (int i = 0; i < 2; i++) {
            files.add(brokerFileStatus("s3://b/file_dt=2026-09-10/f" + i + ".parquet", GIB));
        }
        TableFunctionTable sourceTable = mockSourceTable(Map.of("path", "s3://b/*/*", "format", "parquet"), files);
        Mockito.when(sourceTable.getColumnsFromPath()).thenReturn(List.of("file_dt"));
        Column dt = new Column("dt", DateType.DATE);

        SampleSubqueryExecutor.SampleExecution execution = capturingExecutor(new StringBuilder()).execute(
                new SampleRequest(new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class),
                        "UTC", Map.of("sort_key", "sort_key", "dt", "file_dt"), null),
                        List.of(bigintColumn("sort_key")), List.of(dt), Long.MAX_VALUE, 0L));

        List<Estimates.PartitionSourceBytes> breakdown = execution.estimates().partitionSourceBytes();
        Assertions.assertEquals(2, breakdown.size());
        Assertions.assertEquals(8 * GIB, breakdown.get(0).bytes());
        Assertions.assertEquals(2 * GIB, breakdown.get(1).bytes());
    }

    @Test
    void aPartitionColumnFedByALiteralIsNotStratified() throws Exception {
        TableFunctionTable sourceTable = mockSourceTable(Map.of("path", "s3://b/d/*", "format", "parquet"),
                tenFiles("s3://b/dt=2026-09-10/f"));
        Mockito.when(sourceTable.getColumnsFromPath()).thenReturn(List.of("dt"));
        Column dt = new Column("dt", DateType.DATE);
        StringBuilder capturedSql = new StringBuilder();

        SampleSubqueryExecutor.SampleExecution execution = capturingExecutor(capturedSql).execute(
                new SampleRequest(new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class),
                        "UTC", Map.of("sort_key", "sort_key"), null, Map.of("dt", "'2026-09-10'")),
                        List.of(bigintColumn("sort_key")), List.of(dt), Long.MAX_VALUE, 0L));

        Assertions.assertTrue(execution.estimates().partitionSourceBytes().isEmpty());
        Assertions.assertTrue(capturedSql.toString().contains("s3://b/dt=2026-09-10/f2.parquet,"),
                capturedSql.toString());
    }

    @Test
    void aPartitionColumnReadFromTheFilesKeepsTheStatementsProperties() throws Exception {
        TableFunctionTable sourceTable = mockSourceTable(Map.of("path", "s3://b/d/*", "format", "parquet"),
                tenFiles("s3://b/d/f"));
        Column dt = new Column("dt", DateType.DATE);
        StringBuilder capturedSql = new StringBuilder();

        capturingExecutor(capturedSql).execute(new SampleRequest(new InsertFromFilesScanContext(sourceTable,
                Mockito.mock(ComputeResource.class), "UTC", Map.of("sort_key", "sort_key", "dt", "dt"), null),
                List.of(bigintColumn("sort_key")), List.of(dt), Long.MAX_VALUE, 0L));

        Assertions.assertTrue(capturedSql.toString().contains("\"path\" = \"s3://b/d/*\""), capturedSql.toString());
    }

    @Test
    void pathAndLiteralPartitionColumnsTogetherKeepTheStatementsProperties() throws Exception {
        TableFunctionTable sourceTable = mockSourceTable(Map.of("path", "s3://b/*/*", "format", "parquet"),
                tenFiles("s3://b/dt=2026-09-10/f"));
        Mockito.when(sourceTable.getColumnsFromPath()).thenReturn(List.of("dt"));
        StringBuilder capturedSql = new StringBuilder();

        capturingExecutor(capturedSql).execute(new SampleRequest(new InsertFromFilesScanContext(sourceTable,
                Mockito.mock(ComputeResource.class), "UTC", Map.of("sort_key", "sort_key", "dt", "dt"), null,
                Map.of("region", "'us'")),
                List.of(bigintColumn("sort_key")),
                List.of(new Column("dt", DateType.DATE), new Column("region", VarcharType.VARCHAR)),
                Long.MAX_VALUE, 0L));

        Assertions.assertTrue(capturedSql.toString().contains("\"path\" = \"s3://b/*/*\""), capturedSql.toString());
    }

    private static List<TBrokerFileStatus> tenFiles(String prefix) {
        List<TBrokerFileStatus> files = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            files.add(brokerFileStatus(prefix + i + ".parquet", GIB));
        }
        return files;
    }

    private static InsertFromFilesSampleSubqueryExecutor capturingExecutor(StringBuilder capturedSql) {
        return new InsertFromFilesSampleSubqueryExecutor((sql, computeResource, ignoredQueryTimeoutSeconds) -> {
            capturedSql.append(sql);
            return List.of();
        });
    }

    @Test
    void happyPathDecodesProjectedRows() throws Exception {
        Column sortKeyColumn = bigintColumn("sort_key");
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "oss://bucket/data/*.parquet", "format", "parquet"),
                List.of(brokerFileStatus("oss://bucket/data/a.parquet", 4L * 1024L * 1024L)));

        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of(jsonResultBatch(
                        "{\"data\":[100],\"meta\":[{\"name\":\"sort_key\",\"type\":\"BIGINT\"}]}",
                        "{\"data\":[200]}",
                        "{\"data\":[300]}")));

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(
                bigintRequest(sourceTable, sortKeyColumn));

        List<SampleRow> rows = Lists.newArrayList(execution.rows());
        Assertions.assertEquals(3, rows.size());
        Assertions.assertEquals("100", rows.get(0).sortKeyTuple().get(0).getStringValue());
        Assertions.assertEquals("200", rows.get(1).sortKeyTuple().get(0).getStringValue());
        Assertions.assertEquals("300", rows.get(2).sortKeyTuple().get(0).getStringValue());
        Assertions.assertTrue(rows.get(0).partitionSourceTuple().isEmpty(),
                "unpartitioned request must leave the partition-source tuple empty");
        Assertions.assertEquals(4L * 1024L * 1024L, execution.estimates().totalBytes());
    }

    @Test
    void emptyResultYieldsEmptyIteratorAndCarriesByteEstimate() throws Exception {
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://b/c/*.parquet", "format", "parquet"),
                List.of(brokerFileStatus("s3://b/c/x.parquet", 1024L)));

        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

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
                        Mockito.mock(ComputeResource.class), "UTC"),
                List.of(bigintColumn("sort_key")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);

        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        Assertions.assertThrows(StarRocksException.class, () -> executor.execute(request));
    }

    @Test
    void queryRunnerStarRocksExceptionPropagatesVerbatim() {
        TableFunctionTable sourceTable = mockSourceTable(Map.of("format", "parquet"), List.of());
        StarRocksException injected = new StarRocksException("planner blew up");
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
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
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
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
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) ->
                        List.of(jsonResultBatch("{\"data\":[null]}")));

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
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of(jsonResultBatch(
                        "{\"data\":[null]}", "{\"data\":[42]}")));

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class), "UTC"),
                List.of(nullableSortKey), /*sampleByteLimit=*/ Long.MAX_VALUE, /*seed=*/ 0L));

        List<SampleRow> rows = Lists.newArrayList(execution.rows());
        Assertions.assertEquals(2, rows.size());
        Assertions.assertInstanceOf(NullVariant.class, rows.get(0).sortKeyTuple().get(0),
                "nullable column null cell must decode to NullVariant");
        Assertions.assertEquals("42", rows.get(1).sortKeyTuple().get(0).getStringValue());
    }

    @Test
    void multiColumnRowThrows() {
        TableFunctionTable sourceTable = mockSourceTable(Map.of("format", "parquet"), List.of());
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) ->
                        List.of(jsonResultBatch("{\"data\":[1,2]}")));

        Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(sourceTable, bigintColumn("sort_key"))));
    }

    @Test
    void malformedJsonThrows() {
        TableFunctionTable sourceTable = mockSourceTable(Map.of("format", "parquet"), List.of());
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) ->
                        List.of(jsonResultBatch("not json at all")));

        Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(sourceTable, bigintColumn("sort_key"))));
    }

    @Test
    void buildSampleSqlQuotesIdentifierAndEscapesProperties() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("path", "s3://bucket/has\"quote/file*.parquet");
        properties.put("aws.s3.secret_key", "back\\slash");
        Column sortKeyColumn = new Column("weird`name", IntegerType.BIGINT);

        String propertiesClause = FilesSampleSubqueryExecutor.buildPropertiesClause(properties);
        String fromClauseSql = "FILES(" + propertiesClause + ")";
        List<String> sortKeyIdents = List.of(SqlUtils.getIdentSql(sortKeyColumn.getName()));
        String sql = AbstractSqlSampleSubqueryExecutor.buildSampleSql(
                fromClauseSql, /*whereClauseSqlOrNull=*/ null, sortKeyIdents, List.of(),
                /*samplingRate=*/ 0.1, /*rowLimit=*/ 200_000, /*seed=*/ 42L);

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
    void partitionProjectionKeepsDistinctLiteralValues() {
        String sql = AbstractSqlSampleSubqueryExecutor.buildSampleSql(
                "FILES(\"path\" = \"s3://bucket/*.parquet\")", /*whereClauseSqlOrNull=*/ null,
                List.of("CAST('A' AS varchar)"), List.of("CAST('a' AS varchar)"),
                /*samplingRate=*/ 1.0, /*rowLimit=*/ 100, /*seed=*/ 0L);

        Assertions.assertTrue(sql.startsWith(
                        "SELECT CAST('A' AS varchar), CAST('a' AS varchar) FROM FILES("),
                "partition and sort-key literals with different values need separate result cells: " + sql);
    }

    @Test
    void buildSampleSqlScalesRateForLargeInput() {
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://bucket/large/*.parquet", "format", "parquet"),
                List.of(brokerFileStatus("s3://bucket/large/x.parquet", /*size=*/ 10L * 1024L * 1024L * 1024L)));

        StringBuilder capturedSql = new StringBuilder();
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
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
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
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
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of(jsonResultBatch(
                            "{\"data\":[100, 200],\"meta\":[{\"name\":\"tenant\"},{\"name\":\"position\"}]}",
                            "{\"data\":[100, 300]}"));
                });
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class), "UTC"),
                List.of(bigintColumn("tenant"), bigintColumn("position")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(request);

        Assertions.assertTrue(capturedSql.toString().contains("SELECT `tenant`, `position` FROM FILES"),
                "both sort-key columns must appear in the projection: " + capturedSql);
        List<SampleRow> rows = Lists.newArrayList(execution.rows());
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(2, rows.get(0).sortKeyTuple().size(),
                "each decoded row carries a value per sort-key column");
        Assertions.assertEquals("100", rows.get(0).sortKeyTuple().get(0).getStringValue());
        Assertions.assertEquals("200", rows.get(0).sortKeyTuple().get(1).getStringValue());
        Assertions.assertEquals("100", rows.get(1).sortKeyTuple().get(0).getStringValue());
        Assertions.assertEquals("300", rows.get(1).sortKeyTuple().get(1).getStringValue());
    }

    @Test
    void overlappingPartitionAndKeyRolesProjectOnceAndDecodeEveryTuple() throws Exception {
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://bucket/data/*.parquet", "format", "parquet"),
                List.of(brokerFileStatus("s3://bucket/data/a.parquet", 1024L)));
        StringBuilder capturedSql = new StringBuilder();
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of(jsonResultBatch("{\"data\":[20260921, 11]}"));
                });
        Column partitionAndSortKey = bigintColumn("dt");
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class), "UTC"),
                List.of(partitionAndSortKey, bigintColumn("exp_id")),
                List.of(partitionAndSortKey),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(request);

        Assertions.assertTrue(capturedSql.toString().contains(
                        "SELECT `dt`, `exp_id` FROM FILES"),
                "the overlapping partition column must be projected only once: " + capturedSql);
        Assertions.assertFalse(capturedSql.toString().contains("`exp_id`, `dt` FROM FILES"),
                "the partition projection must reuse the earlier dt result: " + capturedSql);
        SampleRow row = Lists.newArrayList(execution.rows()).get(0);
        Assertions.assertEquals("20260921", row.sortKeyTuple().get(0).getStringValue());
        Assertions.assertEquals("11", row.sortKeyTuple().get(1).getStringValue());
        Assertions.assertEquals("20260921", row.partitionSourceTuple().get(0).getStringValue(),
                "partition decoding must reuse the dt cell without changing the logical tuple contract");
    }

    @Test
    void compositeSortKeyArityMismatchInResultThrows() {
        TableFunctionTable sourceTable = mockSourceTable(Map.of("format", "parquet"), List.of());
        // Sampler claims 2 sort-key columns but server returns 1-value rows — surfaced
        // as a clean StarRocksException (mapped to SkipReason.SAMPLE_FAILED) rather
        // than letting downstream tuple compare blow up.
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) ->
                        List.of(jsonResultBatch("{\"data\":[100]}")));
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class), "UTC"),
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
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        // 25_600 bytes / 256 bytes-per-row estimate = 100 rows, well below the 200_000 hard cap.
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class), "UTC"),
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
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedResources.add(computeResource);
                    return List.of();
                });
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, expectedComputeResource, "UTC"),
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

        ConnectContext returned = AbstractSqlSampleSubqueryExecutor.configureSampleContext(
                sampleContext, computeResource, /*queryTimeoutSeconds=*/ 0);

        Assertions.assertSame(sampleContext, returned);
        InOrder inOrder = Mockito.inOrder(sampleContext);
        inOrder.verify(sampleContext).setCurrentWarehouseId(42L);
        inOrder.verify(sampleContext).setCurrentComputeResource(computeResource);
        inOrder.verify(sampleContext).setNeedQueued(false);
        inOrder.verify(sampleContext).setStartTime();
        // queryTimeoutSeconds == 0 → no cap applied, session variable untouched.
        Mockito.verify(sampleContext, Mockito.never()).getSessionVariable();
    }

    @Test
    void configureSampleContextSetsQueryTimeoutWhenCapped() {
        // The data-tier pipeline caps the sample at the remaining pre-submit
        // budget; configureSampleContext must push that onto the sample
        // session's query_timeout (the BE reads it via SessionVariable.toThrift
        // on the executeDQL path — a SET_VAR SQL hint would be ignored there).
        ConnectContext sampleContext = Mockito.mock(ConnectContext.class);
        SessionVariable sessionVariable = Mockito.mock(SessionVariable.class);
        Mockito.when(sampleContext.getSessionVariable()).thenReturn(sessionVariable);
        ComputeResource computeResource = Mockito.mock(ComputeResource.class);
        Mockito.when(computeResource.getWarehouseId()).thenReturn(7L);

        AbstractSqlSampleSubqueryExecutor.configureSampleContext(
                sampleContext, computeResource, /*queryTimeoutSeconds=*/ 45);

        Mockito.verify(sessionVariable).setQueryTimeoutS(45);
    }

    @Test
    void directoryEntriesDoNotContributeToByteTotal() throws Exception {
        TBrokerFileStatus directoryEntry = new TBrokerFileStatus(
                "s3://b/dir", /*isDir=*/ true, /*size=*/ 999_999L, /*isSplitable=*/ false);
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://b/dir/*.parquet", "format", "parquet"),
                List.of(directoryEntry, brokerFileStatus("s3://b/dir/x.parquet", 512L)));

        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(
                bigintRequest(sourceTable, bigintColumn("sort_key")));
        Assertions.assertEquals(512L, execution.estimates().totalBytes());
    }

    @Test
    void decimalSortKeyDecodesToDecimalVariant() throws Exception {
        // Before DecimalVariant, Variant.of(decimalType, ...) threw and decodeCell recorded
        // SAMPLE_FAILED -> no pre-split. The data tier must now decode decimal cells.
        Column sortKeyColumn = new Column(
                "price", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2));
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "oss://bucket/data/*.parquet", "format", "parquet"),
                List.of(brokerFileStatus("oss://bucket/data/a.parquet", 4L * 1024L * 1024L)));

        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of(jsonResultBatch(
                        "{\"data\":[\"12.34\"]}",
                        "{\"data\":[\"56.78\"]}")));

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(
                bigintRequest(sourceTable, sortKeyColumn));

        List<SampleRow> rows = Lists.newArrayList(execution.rows());
        Assertions.assertEquals(2, rows.size());
        Assertions.assertInstanceOf(DecimalVariant.class, rows.get(0).sortKeyTuple().get(0));
        Assertions.assertEquals("12.34", rows.get(0).sortKeyTuple().get(0).getStringValue());
        Assertions.assertEquals("56.78", rows.get(1).sortKeyTuple().get(0).getStringValue());
    }

    @Test
    void wherePredicateIsCopiedIntoTheSampleSubquery() throws Exception {
        // INSERT INTO t SELECT * FROM FILES(...) WHERE dt >= '2026-01-01': the sampler builds its
        // own FROM clause, so the statement's predicate only reaches the BE if the scan context
        // carries it -- otherwise the sample would span rows the load never writes.
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://b/c/*.parquet", "format", "parquet"),
                List.of(brokerFileStatus("s3://b/c/a.parquet", 1024L)));
        StringBuilder capturedSql = new StringBuilder();
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class), "UTC",
                        Map.of("sort_key", "sort_key"), "`dt` >= '2026-01-01'"),
                List.of(bigintColumn("sort_key")), /*sampleByteLimit=*/ Long.MAX_VALUE, /*seed=*/ 0L));

        Assertions.assertTrue(capturedSql.toString().contains("WHERE (`dt` >= '2026-01-01') AND rand(0)"),
                "the statement's predicate must gate the sample: " + capturedSql);
    }

    @Test
    void renamedProjectionIsSampledFromTheFilesColumnThatBacksIt() throws Exception {
        // INSERT INTO t(sort_key, ...) SELECT file_col, ... FROM FILES(...): the load writes
        // file_col into sort_key, so the sample must read file_col. Projecting the TARGET name
        // would either fail (no such file column) or read an unrelated one.
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://b/c/*.parquet", "format", "parquet"),
                List.of(brokerFileStatus("s3://b/c/a.parquet", 1024L)));
        StringBuilder capturedSql = new StringBuilder();
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class), "UTC",
                        Map.of("sort_key", "file_col"), /*wherePredicateSql=*/ null),
                List.of(bigintColumn("sort_key")), /*sampleByteLimit=*/ Long.MAX_VALUE, /*seed=*/ 0L));

        Assertions.assertTrue(capturedSql.toString().startsWith("SELECT `file_col` FROM FILES("),
                "the projection must name the FILES column, not the target column: " + capturedSql);
    }

    @Test
    void literalFedPartitionColumnIsProjectedAsTheLiteralCastToTheColumnType() throws Exception {
        // INSERT INTO t BY NAME SELECT k, '20260917' AS dt FROM FILES("path" = ".../dt=20260917/*"):
        // dt is in the directory name, not the file, so projecting a FILES column for it would fail.
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://b/dt=20260917/*", "format", "parquet"),
                List.of(brokerFileStatus("s3://b/dt=20260917/a.parquet", 1024L)));
        StringBuilder capturedSql = new StringBuilder();
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class), "UTC",
                        Map.of("sort_key", "sort_key"), /*wherePredicateSql=*/ null, Map.of("dt", "'20260917'")),
                List.of(bigintColumn("sort_key")), List.of(new Column("dt", DateType.DATE)),
                /*sampleByteLimit=*/ Long.MAX_VALUE, /*seed=*/ 0L));

        Assertions.assertTrue(
                capturedSql.toString().startsWith("SELECT `sort_key`, CAST('20260917' AS date) FROM FILES("),
                "the literal stands in for the partition column: " + capturedSql);
    }

    @Test
    void literalFedSortKeyColumnIsProjectedAsTheLiteral() throws Exception {
        // ORDER BY (dt, sort_key) with dt fed by '20260917': the constant sits in the sort-key tuple
        // exactly as the load writes it.
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://b/dt=20260917/*", "format", "parquet"),
                List.of(brokerFileStatus("s3://b/dt=20260917/a.parquet", 1024L)));
        StringBuilder capturedSql = new StringBuilder();
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of();
                });
        Column dt = new Column("dt", DateType.DATE);

        executor.execute(new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class), "UTC",
                        Map.of("sort_key", "file_key"), /*wherePredicateSql=*/ null, Map.of("dt", "'20260917'")),
                List.of(dt, bigintColumn("sort_key")),
                /*partitionSourceColumns=*/ List.of(dt),
                /*sampleByteLimit=*/ Long.MAX_VALUE, /*seed=*/ 0L));

        Assertions.assertTrue(capturedSql.toString().startsWith(
                        "SELECT CAST('20260917' AS date), `file_key` FROM FILES("),
                "the literal stands in for dt in the sort key, and the partition column reuses that result: "
                        + capturedSql);
    }

    @Test
    void distinctLiteralProjectionsKeepThePartitionValue() throws Exception {
        TableFunctionTable sourceTable = mockSourceTable(
                Map.of("path", "s3://b/data/*", "format", "parquet"),
                List.of(brokerFileStatus("s3://b/data/a.parquet", 1024L)));
        StringBuilder capturedSql = new StringBuilder();
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of(jsonResultBatch("{\"data\":[\"A\",11,\"a\"]}"));
                });

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class), "UTC",
                        Map.of("file_key", "file_key"), /*wherePredicateSql=*/ null,
                        Map.of("sort_literal", "'A'", "partition_literal", "'a'")),
                List.of(new Column("sort_literal", VarcharType.VARCHAR), bigintColumn("file_key")),
                List.of(new Column("partition_literal", VarcharType.VARCHAR)),
                /*sampleByteLimit=*/ Long.MAX_VALUE, /*seed=*/ 0L));

        List<SampleRow> rows = Lists.newArrayList(execution.rows());
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("A", rows.get(0).sortKeyTuple().get(0).getStringValue());
        Assertions.assertEquals("a", rows.get(0).partitionSourceTuple().get(0).getStringValue());
        Assertions.assertTrue(capturedSql.toString().contains("CAST('A' AS"), capturedSql.toString());
        Assertions.assertTrue(capturedSql.toString().contains("CAST('a' AS"), capturedSql.toString());
    }

    @Test
    void projectedColumnWithNoFilesMappingThrows() {
        // Fail-safe for a metadata race between the admitting gate and sampling: never compute a
        // boundary from a column the mapping cannot account for.
        TableFunctionTable sourceTable = mockSourceTable(Map.of("format", "parquet"), List.of());
        InsertFromFilesSampleSubqueryExecutor executor = new InsertFromFilesSampleSubqueryExecutor(
                (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class), "UTC",
                        Map.of("other", "other"), /*wherePredicateSql=*/ null),
                List.of(bigintColumn("sort_key")), /*sampleByteLimit=*/ Long.MAX_VALUE, /*seed=*/ 0L);

        Assertions.assertThrows(StarRocksException.class, () -> executor.execute(request));
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
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class), "UTC"),
                List.of(sortKeyColumn),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);
    }

}
