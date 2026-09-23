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
import com.starrocks.persist.ColumnIdExpr;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StringType;
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
                        List.of(mockFileGroup("json")),
                        List.of(List.of(brokerFileStatus("s3://b/x.json", 1024L))))));
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

    // ---------------------------------------------------------------------------------------
    // CSV. Unlike Parquet/ORC, a CSV field has no name: the load maps field i onto the i-th entry
    // of the COLUMNS list, or onto the i-th loadable base-schema column when there is no COLUMNS
    // list. The sampler must restate that layout to FILES, whose `schema` property CSV matches by
    // position, or its by-name SELECT would have nothing to bind to.
    // ---------------------------------------------------------------------------------------

    @Test
    void csvColumnListIsForwardedAsAPositionalFilesSchema() throws Exception {
        BrokerFileGroup fileGroup = csvFileGroup();
        Mockito.when(fileGroup.getFileFieldNames()).thenReturn(List.of("payload", "sort_key"));
        Mockito.when(fileGroup.getColumnExprList())
                .thenReturn(List.of(identityColumn("payload"), identityColumn("sort_key")));
        StringBuilder capturedSql = new StringBuilder();
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(bigintRequest(
                new BrokerDesc(Map.of()),
                List.of(fileGroup),
                List.of(List.of(brokerFileStatus("s3://b/x.csv", 1024L)))));

        Assertions.assertTrue(capturedSql.toString().contains("\"format\" = \"csv\""),
                "CSV must reach FILES as its own format: " + capturedSql);
        // payload sits FIRST, so a by-name read alone would bind sort_key to the wrong field; the
        // schema has to carry the load's order, not the sort key's.
        Assertions.assertEquals("`payload` varchar(65533), `sort_key` bigint(20)",
                capturedFilesSchema(capturedSql.toString()));
    }

    @Test
    void csvSortKeyWithUnsetVarcharLengthIsWidenedNotTruncated() throws Exception {
        // ScalarType.toSql() emits a bare "varchar" when the length was never set, and the FILES
        // schema parser defaults THAT to length 1 -- which would silently truncate every sampled
        // sort-key value to one character and wreck the boundaries. Pin the widening guard.
        BrokerFileGroup fileGroup = csvFileGroup();
        Mockito.when(fileGroup.getFileFieldNames()).thenReturn(List.of("sort_key"));
        StringBuilder capturedSql = new StringBuilder();
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(new SampleRequest(
                csvScanContext(List.of(fileGroup), List.of(List.of(brokerFileStatus("s3://b/x.csv", 1024L))),
                        /*targetBaseSchema=*/ List.of()),
                // VarcharType.VARCHAR has length -1
                List.of(new Column("sort_key", VarcharType.VARCHAR)),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L));

        String schema = capturedFilesSchema(capturedSql.toString());
        Assertions.assertEquals("`sort_key` varchar(65533)", schema,
                "an unset-length VARCHAR sort key must widen, never render as bare `varchar`");
        List<Column> parsed = SqlParser.parseFilesSchema(schema);
        Assertions.assertEquals(StringType.DEFAULT_STRING_LENGTH,
                ((ScalarType) parsed.get(0).getType()).getLength(),
                "parsed back it must NOT collapse to varchar(1)");
    }

    @Test
    void csvFilesSchemaParsesBackToTheProjectedColumnTypes() throws Exception {
        // The schema string is only useful if FILES can parse it. Round-trip it through the same
        // parser TableFunctionTable uses, so a type this executor renders but the grammar cannot
        // read fails here rather than as a mid-load sub-query failure.
        BrokerFileGroup fileGroup = csvFileGroup();
        Mockito.when(fileGroup.getFileFieldNames()).thenReturn(List.of("sort_key", "name", "skipped"));
        Mockito.when(fileGroup.getColumnExprList()).thenReturn(List.of(
                identityColumn("sort_key"), identityColumn("name"), identityColumn("skipped")));
        StringBuilder capturedSql = new StringBuilder();
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(new SampleRequest(
                csvScanContext(List.of(fileGroup), List.of(List.of(brokerFileStatus("s3://b/x.csv", 1024L))),
                        /*targetBaseSchema=*/ List.of()),
                List.of(bigintColumn("sort_key"), new Column("name", new VarcharType(32))),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L));

        List<Column> parsed = SqlParser.parseFilesSchema(capturedFilesSchema(capturedSql.toString()));
        Assertions.assertEquals(List.of("sort_key", "name", "skipped"),
                parsed.stream().map(Column::getName).toList());
        Assertions.assertEquals(PrimitiveType.BIGINT, parsed.get(0).getType().getPrimitiveType());
        Assertions.assertEquals(PrimitiveType.VARCHAR, parsed.get(1).getType().getPrimitiveType());
        Assertions.assertEquals(32, ((ScalarType) parsed.get(1).getType()).getLength(),
                "a projected VARCHAR key must keep its declared length, not collapse to varchar(1)");
        Assertions.assertEquals(StringType.DEFAULT_STRING_LENGTH,
                ((ScalarType) parsed.get(2).getType()).getLength(),
                "an unprojected field only has to hold its position, so it is declared as a wide string");
    }

    @Test
    void csvWithoutAColumnListTakesItsLayoutFromTheTargetBaseSchema() throws Exception {
        // No COLUMNS list: Load.initColumns derives the field order from the base schema, skipping
        // generated and auto-increment columns because neither is read from the file.
        BrokerFileGroup fileGroup = csvFileGroup();
        StringBuilder capturedSql = new StringBuilder();
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(new SampleRequest(
                csvScanContext(List.of(fileGroup), List.of(List.of(brokerFileStatus("s3://b/x.csv", 1024L))),
                        List.of(bigintColumn("payload"), autoIncrementColumn("id"),
                                bigintColumn("sort_key"), generatedColumn("derived"))),
                List.of(bigintColumn("sort_key")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L));

        Assertions.assertEquals("`payload` varchar(65533), `sort_key` bigint(20)",
                capturedFilesSchema(capturedSql.toString()));
    }

    @Test
    void csvWithoutAColumnListAndWithoutABaseSchemaIsRejected() {
        BrokerFileGroup fileGroup = csvFileGroup();
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(fileGroup),
                        List.of(List.of(brokerFileStatus("s3://b/x.csv", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("positional field layout is unknown"),
                "error should call out the unknown CSV layout: " + thrown.getMessage());
    }

    @Test
    void csvColumnsFromPathStaysOutOfTheSchemaAndKeepsItsOwnProperty() throws Exception {
        // COLUMNS (sort_key) COLUMNS FROM PATH AS (dt): dt is not a CSV field on either side --
        // Broker Load appends path values after the file's fields and FILES appends path columns
        // after the declared schema -- so it must not consume a position in the schema.
        BrokerFileGroup fileGroup = csvFileGroup();
        Mockito.when(fileGroup.getFileFieldNames()).thenReturn(List.of("sort_key"));
        Mockito.when(fileGroup.getColumnsFromPath()).thenReturn(List.of("dt"));
        Mockito.when(fileGroup.getColumnExprList())
                .thenReturn(List.of(identityColumn("sort_key"), identityColumn("dt")));
        StringBuilder capturedSql = new StringBuilder();
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(new SampleRequest(
                csvScanContext(List.of(fileGroup),
                        List.of(List.of(brokerFileStatus("s3://b/dt=20260921/x.csv", 1024L))),
                        /*targetBaseSchema=*/ List.of()),
                List.of(bigintColumn("sort_key")),
                List.of(new Column("dt", VarcharType.VARCHAR)),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L));

        Assertions.assertEquals("`sort_key` bigint(20)", capturedFilesSchema(capturedSql.toString()));
        Assertions.assertTrue(capturedSql.toString().contains("\"columns_from_path\" = \"dt\""),
                "the path column must still reach FILES through columns_from_path: " + capturedSql);
    }

    @Test
    void csvDialectIsForwardedToFiles() throws Exception {
        // A different separator / enclose / escape / header skip splits a row into different
        // fields, which would move the positions the schema names.
        BrokerFileGroup fileGroup = csvFileGroup();
        Mockito.when(fileGroup.getFileFieldNames()).thenReturn(List.of("sort_key"));
        Mockito.when(fileGroup.getColumnSeparator()).thenReturn("|");
        Mockito.when(fileGroup.getRowDelimiter()).thenReturn("\r\n");
        Mockito.when(fileGroup.getEnclose()).thenReturn((byte) '"');
        Mockito.when(fileGroup.getEscape()).thenReturn((byte) '\\');
        Mockito.when(fileGroup.getSkipHeader()).thenReturn(1L);
        Mockito.when(fileGroup.isTrimspace()).thenReturn(true);
        StringBuilder capturedSql = new StringBuilder();
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(bigintRequest(
                new BrokerDesc(Map.of()),
                List.of(fileGroup),
                List.of(List.of(brokerFileStatus("s3://b/x.csv", 1024L)))));

        String sql = capturedSql.toString();
        Assertions.assertTrue(sql.contains("\"csv.column_separator\" = \"|\""), sql);
        Assertions.assertTrue(sql.contains("\"csv.row_delimiter\" = \"\r\n\""), sql);
        Assertions.assertTrue(sql.contains("\"csv.enclose\" = \"\\\"\""),
                "a quote enclose must be escaped inside the property literal: " + sql);
        Assertions.assertTrue(sql.contains("\"csv.escape\" = \"\\\\\""),
                "a backslash escape must be escaped inside the property literal: " + sql);
        Assertions.assertTrue(sql.contains("\"csv.skip_header\" = \"1\""), sql);
        Assertions.assertTrue(sql.contains("\"csv.trim_space\" = \"true\""), sql);
    }

    @Test
    void csvDialectLeftAtItsDefaultsIsNotForwarded() throws Exception {
        // FILES defaults to the same tab / newline / no-enclose / no-escape dialect Broker Load
        // does, so an untouched dialect emits nothing.
        BrokerFileGroup fileGroup = csvFileGroup();
        Mockito.when(fileGroup.getFileFieldNames()).thenReturn(List.of("sort_key"));
        StringBuilder capturedSql = new StringBuilder();
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                (sql, computeResource, ignoredQueryTimeoutSeconds) -> {
                    capturedSql.append(sql);
                    return List.of();
                });

        executor.execute(bigintRequest(
                new BrokerDesc(Map.of()),
                List.of(fileGroup),
                List.of(List.of(brokerFileStatus("s3://b/x.csv", 1024L)))));

        Assertions.assertFalse(capturedSql.toString().contains("csv."),
                "no csv.* property should be emitted for a default dialect: " + capturedSql);
    }

    @Test
    void csvFileGroupsWithDifferentLengthColumnListsAreRejected() {
        // The other arm of the layout comparison: lists of DIFFERENT length. One FILES call declares
        // one positional schema, so a group that names an extra field cannot share it.
        BrokerFileGroup first = csvFileGroup();
        Mockito.when(first.getFileFieldNames()).thenReturn(List.of("sort_key", "payload"));
        Mockito.when(first.getColumnExprList())
                .thenReturn(List.of(identityColumn("sort_key"), identityColumn("payload")));
        BrokerFileGroup second = csvFileGroup();
        Mockito.when(second.getFileFieldNames()).thenReturn(List.of("sort_key"));
        Mockito.when(second.getColumnExprList()).thenReturn(List.of(identityColumn("sort_key")));
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(first, second),
                        List.of(List.of(brokerFileStatus("s3://b/x.csv", 1024L)),
                                List.of(brokerFileStatus("s3://b/y.csv", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("disagree on their column layout"),
                "error should call out the layout disagreement: " + thrown.getMessage());
    }

    @Test
    void csvFileGroupsDisagreeingOnColumnLayoutAreRejected() {
        BrokerFileGroup first = csvFileGroup();
        Mockito.when(first.getFileFieldNames()).thenReturn(List.of("sort_key", "payload"));
        Mockito.when(first.getColumnExprList())
                .thenReturn(List.of(identityColumn("sort_key"), identityColumn("payload")));
        BrokerFileGroup second = csvFileGroup();
        Mockito.when(second.getFileFieldNames()).thenReturn(List.of("payload", "sort_key"));
        Mockito.when(second.getColumnExprList())
                .thenReturn(List.of(identityColumn("payload"), identityColumn("sort_key")));
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(first, second),
                        List.of(List.of(brokerFileStatus("s3://b/x.csv", 1024L)),
                                List.of(brokerFileStatus("s3://b/y.csv", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("disagree on their column layout"),
                "error should call out the layout disagreement: " + thrown.getMessage());
    }

    @Test
    void csvFileGroupsDisagreeingOnDialectAreRejected() {
        BrokerFileGroup first = csvFileGroup();
        Mockito.when(first.getFileFieldNames()).thenReturn(List.of("sort_key"));
        Mockito.when(first.getColumnSeparator()).thenReturn(",");
        BrokerFileGroup second = csvFileGroup();
        Mockito.when(second.getFileFieldNames()).thenReturn(List.of("sort_key"));
        Mockito.when(second.getColumnSeparator()).thenReturn("|");
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(first, second),
                        List.of(List.of(brokerFileStatus("s3://b/x.csv", 1024L)),
                                List.of(brokerFileStatus("s3://b/y.csv", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("disagree on parsing options"),
                "error should call out the dialect disagreement: " + thrown.getMessage());
    }

    @Test
    void csvNonAsciiEncloseIsRejected() {
        // FILES takes enclose as a string and reads its first byte back; a high byte would be
        // re-encoded as two UTF-8 bytes and silently split fields differently from the load.
        BrokerFileGroup fileGroup = csvFileGroup();
        Mockito.when(fileGroup.getFileFieldNames()).thenReturn(List.of("sort_key"));
        Mockito.when(fileGroup.getEnclose()).thenReturn((byte) 0xA7);
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(fileGroup),
                        List.of(List.of(brokerFileStatus("s3://b/x.csv", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("outside ASCII"),
                "error should call out the non-ASCII enclose byte: " + thrown.getMessage());
    }

    @Test
    void csvKeepsEveryKeyPerturbingRejection() throws Exception {
        // Admitting the format must not widen the mapping guard: a SET clause still diverges the
        // sampled key from the inserted value whatever the format is.
        BrokerFileGroup derived = csvFileGroup();
        Mockito.when(derived.getFileFieldNames()).thenReturn(List.of("sort_key"));
        Mockito.when(derived.getColumnExprList()).thenReturn(List.of(
                new ImportColumnDesc("sort_key", Mockito.mock(Expr.class))));
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource, ignoredQueryTimeoutSeconds) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(derived),
                        List.of(List.of(brokerFileStatus("s3://b/x.csv", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("derived column"),
                "error should still call out the derived column: " + thrown.getMessage());

        // ... and the path can still not supply a key column, CSV or not.
        BrokerFileGroup pathKey = csvFileGroup();
        Mockito.when(pathKey.getFileFieldNames()).thenReturn(List.of("payload"));
        Mockito.when(pathKey.getColumnsFromPath()).thenReturn(List.of("sort_key"));
        Mockito.when(pathKey.getColumnExprList())
                .thenReturn(List.of(identityColumn("payload"), identityColumn("sort_key")));
        StarRocksException pathThrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(pathKey),
                        List.of(List.of(brokerFileStatus("s3://b/sort_key=7/x.csv", 1024L))))));
        Assertions.assertTrue(pathThrown.getMessage().contains("columns_from_path"),
                "error should still call out the path-supplied key column: " + pathThrown.getMessage());
    }

    /** Extracts the value of the FILES {@code schema} property from a captured sub-query. */
    private static String capturedFilesSchema(String sampleSql) {
        java.util.regex.Matcher matcher =
                java.util.regex.Pattern.compile("\"schema\" = \"(.*?)\"(?:, |\\))").matcher(sampleSql);
        Assertions.assertTrue(matcher.find(), "sub-query carries no FILES schema property: " + sampleSql);
        return matcher.group(1);
    }

    private static BrokerFileGroup csvFileGroup() {
        return mockFileGroup("csv");
    }

    private static Column autoIncrementColumn(String name) {
        Column column = new Column(name, IntegerType.BIGINT);
        column.setIsAutoIncrement(true);
        return column;
    }

    private static Column generatedColumn(String name) {
        Column column = new Column(name, IntegerType.BIGINT);
        column.setGeneratedColumnExpr(ColumnIdExpr.create(new SlotRef(null, "sort_key")));
        return column;
    }

    private static BrokerLoadScanContext csvScanContext(
            List<BrokerFileGroup> fileGroups,
            List<List<TBrokerFileStatus>> fileStatusesPerGroup,
            List<Column> targetBaseSchema) {
        return new BrokerLoadScanContext(new BrokerDesc(Map.of()), fileGroups, fileStatusesPerGroup,
                Mockito.mock(ComputeResource.class), "UTC", targetBaseSchema);
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
