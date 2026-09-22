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
import com.starrocks.common.StarRocksException;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.type.VarcharType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.brokerFileStatus;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.jsonResultBatch;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.nullableBigintColumn;

class BrokerLoadSampleSubqueryExecutorTest {

    @Test
    void happyPathSynthesizesFilesSqlAndDecodesRows() throws Exception {
        BrokerDesc brokerDesc = new BrokerDesc(Map.of("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com"));
        List<BrokerFileGroup> fileGroups = List.of(mockFileGroup("parquet"));
        List<List<TBrokerFileStatus>> fileStatusesPerGroup = List.of(List.of(
                brokerFileStatus("s3://bucket/a.parquet", 2L * 1024L * 1024L),
                brokerFileStatus("s3://bucket/b.parquet", 2L * 1024L * 1024L)));
        StringBuilder capturedSql = new StringBuilder();

        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of(jsonResultBatch(
                            "{\"data\":[100],\"meta\":[{\"name\":\"sort_key\",\"type\":\"BIGINT\"}]}",
                            "{\"data\":[200]}"));
                });

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(
                bigintRequest(brokerDesc, fileGroups, fileStatusesPerGroup));

        List<SampleRow> rows = Lists.newArrayList(execution.rows());
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals("100", rows.get(0).sortKeyTuple().get(0).getStringValue());
        Assertions.assertEquals("200", rows.get(1).sortKeyTuple().get(0).getStringValue());
        Assertions.assertTrue(rows.get(0).partitionSourceTuple().isEmpty(),
                "unpartitioned request must leave the partition-source tuple empty");
        Assertions.assertEquals(4L * 1024L * 1024L, execution.estimates().totalBytes());

        Assertions.assertTrue(capturedSql.toString().contains(
                "\"path\" = \"s3://bucket/a.parquet,s3://bucket/b.parquet\""),
                "comma-joined path list must appear verbatim: " + capturedSql);
        Assertions.assertTrue(capturedSql.toString().contains("\"format\" = \"parquet\""),
                "shared format must be wired through: " + capturedSql);
        Assertions.assertTrue(capturedSql.toString().contains("\"fs.s3a.endpoint\""),
                "broker properties must pass through verbatim: " + capturedSql);
    }

    @Test
    void executeForwardsRequestQueryTimeoutToRunner() throws Exception {
        // Make the soft pre-submit deadline hard: the data-tier pipeline caps the
        // sample via SampleRequest.queryTimeoutSeconds, and execute() must forward
        // that to the runner. Production applies it as query_timeout on the sample
        // context (see FilesSampleSubqueryExecutor.configureSampleContext); an
        // uncapped request forwards 0 (no cap).
        BrokerDesc brokerDesc = new BrokerDesc(Map.of("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com"));
        List<BrokerFileGroup> fileGroups = List.of(mockFileGroup("parquet"));
        List<List<TBrokerFileStatus>> fileStatusesPerGroup = List.of(List.of(
                brokerFileStatus("s3://bucket/a.parquet", 2L * 1024L * 1024L)));

        AtomicInteger capturedTimeout = new AtomicInteger(-1);
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, queryTimeoutSeconds) -> {
                    capturedTimeout.set(queryTimeoutSeconds);
                    return List.of();
                });

        executor.execute(bigintRequest(brokerDesc, fileGroups, fileStatusesPerGroup)
                .withQueryTimeoutSeconds(45));
        Assertions.assertEquals(45, capturedTimeout.get(),
                "execute() must forward the request's query-timeout cap to the runner");

        executor.execute(bigintRequest(brokerDesc, fileGroups, fileStatusesPerGroup));
        Assertions.assertEquals(0, capturedTimeout.get(),
                "an uncapped request forwards 0 (no cap)");
    }

    @Test
    void brokerBackedSourceIsRejected() {
        BrokerDesc brokerBacked = new BrokerDesc("the_broker", Map.of());
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        brokerBacked,
                        List.of(mockFileGroup("parquet")),
                        List.of(List.of(brokerFileStatus("s3://b/x.parquet", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("broker-backed"),
                "error should call out broker-backed rejection: " + thrown.getMessage());
    }

    @Test
    void missingBrokerDescIsRejected() {
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        /*brokerDesc=*/ null,
                        List.of(mockFileGroup("parquet")),
                        List.of(List.of(brokerFileStatus("s3://b/x.parquet", 1024L))))));
    }

    @Test
    void missingFormatIsRejected() {
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(mockFileGroup(/*format=*/ null)),
                        List.of(List.of(brokerFileStatus("s3://b/x.parquet", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("no declared format"),
                "error should call out missing format: " + thrown.getMessage());
    }

    @Test
    void conflictingFormatsAreRejected() {
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(mockFileGroup("parquet"), mockFileGroup("orc")),
                        List.of(
                                List.of(brokerFileStatus("s3://b/x.parquet", 1024L)),
                                List.of(brokerFileStatus("s3://b/y.orc", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("disagree on format"),
                "error should call out format mismatch: " + thrown.getMessage());
    }

    @Test
    void unsupportedFormatIsRejected() {
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(mockFileGroup("csv")),
                        List.of(List.of(brokerFileStatus("s3://b/x.csv", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("not yet supported"),
                "error should call out unsupported format: " + thrown.getMessage());
    }

    @Test
    void caseInsensitiveFormatAgreement() throws Exception {
        StringBuilder capturedSql = new StringBuilder();
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(bigintRequest(
                new BrokerDesc(Map.of()),
                List.of(mockFileGroup("PARQUET"), mockFileGroup("parquet")),
                List.of(
                        List.of(brokerFileStatus("s3://b/x.parquet", 1024L)),
                        List.of(brokerFileStatus("s3://b/y.parquet", 1024L)))));

        Assertions.assertTrue(capturedSql.toString().contains("\"format\" = \"parquet\""),
                "format must be normalized to lowercase: " + capturedSql);
    }

    @Test
    void directoriesAreSkipped() throws Exception {
        TBrokerFileStatus directoryEntry = new TBrokerFileStatus(
                "s3://b/dir", /*isDir=*/ true, /*size=*/ 999_999L, /*isSplitable=*/ false);
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(bigintRequest(
                new BrokerDesc(Map.of()),
                List.of(mockFileGroup("parquet")),
                List.of(List.of(directoryEntry, brokerFileStatus("s3://b/dir/x.parquet", 512L)))));

        Assertions.assertEquals(512L, execution.estimates().totalBytes());
    }

    @Test
    void emptyResolvedFileListIsRejected() {
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(mockFileGroup("parquet")),
                        List.of(List.<TBrokerFileStatus>of()))));
        Assertions.assertTrue(thrown.getMessage().contains("no files to sample"),
                "error should call out empty resolved file list: " + thrown.getMessage());
    }

    @Test
    void pathContainingCommaIsRejected() {
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(mockFileGroup("parquet")),
                        List.of(List.of(brokerFileStatus("s3://b/contains,comma.parquet", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("path-list separator"),
                "error should call out comma in path: " + thrown.getMessage());
    }

    @Test
    void whereClauseOnFileGroupIsRejected() {
        BrokerFileGroup fileGroup = mockFileGroup("parquet");
        Mockito.when(fileGroup.getWhereExpr()).thenReturn(Mockito.mock(Expr.class));
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(fileGroup),
                        List.of(List.of(brokerFileStatus("s3://b/x.parquet", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("WHERE filter"),
                "error should call out WHERE filter rejection: " + thrown.getMessage());
    }

    @Test
    void negativeLoadIsRejected() {
        BrokerFileGroup fileGroup = mockFileGroup("parquet");
        Mockito.when(fileGroup.isNegative()).thenReturn(true);
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(fileGroup),
                        List.of(List.of(brokerFileStatus("s3://b/x.parquet", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("negative-load"),
                "error should call out negative-load rejection: " + thrown.getMessage());
    }

    @Test
    void columnsFromPathDisjointFromKeyIsAcceptedAndForwardedToFiles() {
        // The ordinary "Parquet under a partition directory" load:
        //   COLUMNS (sort_key, dt) COLUMNS FROM PATH AS (dt)
        // dt is the partition column and comes from the directory name; the sort key is read
        // verbatim from the file. Nothing perturbs the sampled key, so this must sample rather
        // than skip -- and columns_from_path must reach FILES so dt is projectable.
        BrokerFileGroup fileGroup = mockFileGroup("parquet");
        Mockito.when(fileGroup.getColumnsFromPath()).thenReturn(List.of("dt"));
        Mockito.when(fileGroup.getColumnExprList())
                .thenReturn(List.of(identityColumn("sort_key"), identityColumn("dt")));
        StringBuilder capturedSql = new StringBuilder();
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        Assertions.assertDoesNotThrow(() -> executor.execute(partitionedRequest(
                new BrokerDesc(Map.of()),
                List.of(fileGroup),
                List.of(List.of(brokerFileStatus("s3://b/dt=20260921/x.parquet", 1024L))))));

        Assertions.assertTrue(capturedSql.toString().contains("\"columns_from_path\" = \"dt\""),
                "columns_from_path must be forwarded to FILES: " + capturedSql);
        Assertions.assertTrue(capturedSql.toString().contains("`sort_key`") 
                        && capturedSql.toString().contains("`dt`"),
                "the sub-query must project both the sort key and the path-derived partition column: "
                        + capturedSql);
    }

    @Test
    void columnsFromPathSupplyingAKeyColumnIsRejected() {
        // Here the path supplies the SORT KEY itself. Its value is in the directory name, not in
        // the file, so no footer or file scan can reproduce it -- boundaries must not be sampled.
        BrokerFileGroup fileGroup = mockFileGroup("parquet");
        Mockito.when(fileGroup.getColumnsFromPath()).thenReturn(List.of("sort_key"));
        Mockito.when(fileGroup.getColumnExprList()).thenReturn(List.of(identityColumn("sort_key")));
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(fileGroup),
                        List.of(List.of(brokerFileStatus("s3://b/sort_key=7/x.parquet", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("columns_from_path")
                        && thrown.getMessage().contains("sort_key"),
                "error should name the key column supplied from the path: " + thrown.getMessage());
    }

    @Test
    void identityColumnListIsAccepted() {
        // A COLUMNS list with no SET clause only NAMES the source fields. Both tiers are
        // Parquet/ORC-only and the BE resolves those by name, so the sampler's by-name SELECT
        // lands on the same physical column the load reads.
        BrokerFileGroup fileGroup = mockFileGroup("parquet");
        Mockito.when(fileGroup.getColumnExprList())
                .thenReturn(List.of(identityColumn("sort_key"), identityColumn("payload")));
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        Assertions.assertDoesNotThrow(() -> executor.execute(bigintRequest(
                new BrokerDesc(Map.of()),
                List.of(fileGroup),
                List.of(List.of(brokerFileStatus("s3://b/x.parquet", 1024L))))));
    }

    @Test
    void derivedColumnIsRejected() {
        // SET sort_key = <expr>: the sampler would read the file's raw sort_key while the load
        // inserts the mapped value.
        BrokerFileGroup fileGroup = mockFileGroup("parquet");
        Mockito.when(fileGroup.getColumnExprList()).thenReturn(List.of(
                new ImportColumnDesc("sort_key", Mockito.mock(Expr.class))));
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(fileGroup),
                        List.of(List.of(brokerFileStatus("s3://b/x.parquet", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("derived column"),
                "error should call out the derived column: " + thrown.getMessage());
    }

    @Test
    void columnListOmittingAKeyColumnIsRejected() {
        // The load never populates sort_key from the source (it stays at its default), but the
        // sampler would read whatever the file carries under that name.
        BrokerFileGroup fileGroup = mockFileGroup("parquet");
        Mockito.when(fileGroup.getColumnExprList()).thenReturn(List.of(identityColumn("payload")));
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(fileGroup),
                        List.of(List.of(brokerFileStatus("s3://b/x.parquet", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("does not name key column"),
                "error should call out the unnamed key column: " + thrown.getMessage());
    }

    @Test
    void columnsFromPathSupplyingARollupSortKeyIsRejected() {
        // The guard spans every sampled key, not just the base sort key. Here the base key
        // (sort_key) is read from the file and would pass on its own, but a visible rollup sorts by
        // rollup_key, which the path supplies -- so the rollup's boundaries could not be sampled and
        // the whole request must be rejected.
        BrokerFileGroup fileGroup = mockFileGroup("parquet");
        Mockito.when(fileGroup.getColumnsFromPath()).thenReturn(List.of("rollup_key"));
        Mockito.when(fileGroup.getColumnExprList())
                .thenReturn(List.of(identityColumn("sort_key"), identityColumn("rollup_key")));
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(rollupRequest(
                        new BrokerDesc(Map.of()),
                        List.of(fileGroup),
                        List.of(List.of(brokerFileStatus("s3://b/rollup_key=7/x.parquet", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("columns_from_path")
                        && thrown.getMessage().contains("rollup_key"),
                "error should name the ROLLUP key column supplied from the path: " + thrown.getMessage());
    }

    @Test
    void rollupSortKeyMissingFromColumnListIsRejected() {
        // Same reach, the other rejection arm: the COLUMNS list names the base key but not the
        // rollup's, so the load never populates rollup_key from the source.
        BrokerFileGroup fileGroup = mockFileGroup("parquet");
        Mockito.when(fileGroup.getColumnExprList()).thenReturn(List.of(identityColumn("sort_key")));
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(rollupRequest(
                        new BrokerDesc(Map.of()),
                        List.of(fileGroup),
                        List.of(List.of(brokerFileStatus("s3://b/x.parquet", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("does not name key column")
                        && thrown.getMessage().contains("rollup_key"),
                "error should name the unlisted ROLLUP key column: " + thrown.getMessage());
    }

    @Test
    void fileGroupsDisagreeingOnColumnsFromPathAreRejected() {
        // One FILES call carries one columns_from_path list.
        BrokerFileGroup withPath = mockFileGroup("parquet");
        Mockito.when(withPath.getColumnsFromPath()).thenReturn(List.of("dt"));
        BrokerFileGroup withoutPath = mockFileGroup("parquet");
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(withPath, withoutPath),
                        List.of(List.of(brokerFileStatus("s3://b/dt=1/x.parquet", 1024L)),
                                List.of(brokerFileStatus("s3://b/y.parquet", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("disagree on columns_from_path"),
                "error should call out the columns_from_path disagreement: " + thrown.getMessage());
    }

    @Test
    void wrongScanContextTypeThrows() {
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(
                        Mockito.mock(com.starrocks.catalog.TableFunctionTable.class),
                        Mockito.mock(ComputeResource.class), "UTC"),
                List.of(bigintColumn("sort_key")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        Assertions.assertThrows(StarRocksException.class, () -> executor.execute(request));
    }

    @Test
    void compositeSortKeyProjectsAllColumnsAndDecodesTuples() throws Exception {
        StringBuilder capturedSql = new StringBuilder();
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of(jsonResultBatch(
                            "{\"data\":[10, 20]}",
                            "{\"data\":[30, 40]}"));
                });
        SampleRequest request = new SampleRequest(
                new BrokerLoadScanContext(
                        new BrokerDesc(Map.of()),
                        List.of(mockFileGroup("parquet")),
                        List.of(List.of(brokerFileStatus("s3://b/x.parquet", 1024L))),
                        Mockito.mock(ComputeResource.class), "UTC"),
                List.of(bigintColumn("tenant"), bigintColumn("position")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(request);

        Assertions.assertTrue(capturedSql.toString().contains("SELECT `tenant`, `position` FROM FILES"),
                "both sort-key columns must appear in the projection: " + capturedSql);
        List<SampleRow> rows = Lists.newArrayList(execution.rows());
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(2, rows.get(0).sortKeyTuple().size());
        Assertions.assertEquals("10", rows.get(0).sortKeyTuple().get(0).getStringValue());
        Assertions.assertEquals("20", rows.get(0).sortKeyTuple().get(1).getStringValue());
    }

    @Test
    void compositeSortKeyWithNullableTrailingColumnDecodesNullVariant() throws Exception {
        // Mirrors the InsertFromFiles coverage: ORDER BY(group_id, nullable_col)
        // is valid, and null cells in the nullable column must decode to
        // NullVariant rather than failing SAMPLE_FAILED.
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of(jsonResultBatch(
                        "{\"data\":[1, null]}",
                        "{\"data\":[2, 42]}")));
        Column groupId = bigintColumn("group_id");
        Column nullableTrailing = nullableBigintColumn("trailing");
        SampleRequest request = new SampleRequest(
                new BrokerLoadScanContext(
                        new BrokerDesc(Map.of()),
                        List.of(mockFileGroup("parquet")),
                        List.of(List.of(brokerFileStatus("s3://b/x.parquet", 1024L))),
                        Mockito.mock(ComputeResource.class), "UTC"),
                List.of(groupId, nullableTrailing),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(request);

        List<SampleRow> rows = Lists.newArrayList(execution.rows());
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals("1", rows.get(0).sortKeyTuple().get(0).getStringValue());
        Assertions.assertInstanceOf(NullVariant.class, rows.get(0).sortKeyTuple().get(1),
                "nullable trailing column null cell must decode to NullVariant");
        Assertions.assertEquals("42", rows.get(1).sortKeyTuple().get(1).getStringValue());
    }

    @Test
    void runnerReceivesComputeResourceFromScanContext() throws Exception {
        ComputeResource expectedComputeResource = Mockito.mock(ComputeResource.class);
        List<ComputeResource> capturedResources = new ArrayList<>();
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedResources.add(computeResource);
                    return List.of();
                });

        SampleRequest request = new SampleRequest(
                new BrokerLoadScanContext(
                        new BrokerDesc(Map.of()),
                        List.of(mockFileGroup("parquet")),
                        List.of(List.of(brokerFileStatus("s3://b/x.parquet", 1024L))),
                        expectedComputeResource, "UTC"),
                List.of(bigintColumn("sort_key")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);
        executor.execute(request);

        Assertions.assertEquals(1, capturedResources.size());
        Assertions.assertSame(expectedComputeResource, capturedResources.get(0));
    }

    /** An {@code ImportColumnDesc} that only names a source field (expr == null -> isColumn()). */
    private static ImportColumnDesc identityColumn(String columnName) {
        return new ImportColumnDesc(columnName);
    }

    /** A request carrying one visible rollup that sorts by {@code rollup_key}. */
    private static SampleRequest rollupRequest(
            BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups,
            List<List<TBrokerFileStatus>> fileStatusesPerGroup) {
        return new SampleRequest(
                new BrokerLoadScanContext(brokerDesc, fileGroups, fileStatusesPerGroup,
                        Mockito.mock(ComputeResource.class), "UTC"),
                List.of(bigintColumn("sort_key")),
                List.of(new SecondaryIndexSpec(/*indexMetaId=*/ 1001L,
                        List.of(bigintColumn("rollup_key")))),
                /*partitionSourceColumns=*/ List.of(),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);
    }

    /** A request whose target is partitioned by {@code dt}, so the sub-query projects it. */
    private static SampleRequest partitionedRequest(
            BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups,
            List<List<TBrokerFileStatus>> fileStatusesPerGroup) {
        return new SampleRequest(
                new BrokerLoadScanContext(brokerDesc, fileGroups, fileStatusesPerGroup,
                        Mockito.mock(ComputeResource.class), "UTC"),
                List.of(bigintColumn("sort_key")),
                List.of(new Column("dt", VarcharType.VARCHAR)),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);
    }

    private static BrokerFileGroup mockFileGroup(String fileFormat) {
        BrokerFileGroup fileGroup = Mockito.mock(BrokerFileGroup.class);
        Mockito.when(fileGroup.getFileFormat()).thenReturn(fileFormat);
        return fileGroup;
    }

    private static SampleRequest bigintRequest(
            BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups,
            List<List<TBrokerFileStatus>> fileStatusesPerGroup) {
        return new SampleRequest(
                new BrokerLoadScanContext(brokerDesc, fileGroups, fileStatusesPerGroup,
                        Mockito.mock(ComputeResource.class), "UTC"),
                List.of(bigintColumn("sort_key")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);
    }
}
