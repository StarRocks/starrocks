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
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
                /*sampleQueryRunner=*/ (sql, computeResource) -> {
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
    void brokerBackedSourceIsRejected() {
        BrokerDesc brokerBacked = new BrokerDesc("the_broker", Map.of());
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of());

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
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of());

        Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        /*brokerDesc=*/ null,
                        List.of(mockFileGroup("parquet")),
                        List.of(List.of(brokerFileStatus("s3://b/x.parquet", 1024L))))));
    }

    @Test
    void missingFormatIsRejected() {
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of());

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
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of());

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
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of());

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
                /*sampleQueryRunner=*/ (sql, computeResource) -> {
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
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of());

        SampleSubqueryExecutor.SampleExecution execution = executor.execute(bigintRequest(
                new BrokerDesc(Map.of()),
                List.of(mockFileGroup("parquet")),
                List.of(List.of(directoryEntry, brokerFileStatus("s3://b/dir/x.parquet", 512L)))));

        Assertions.assertEquals(512L, execution.estimates().totalBytes());
    }

    @Test
    void emptyResolvedFileListIsRejected() {
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of());

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
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of());

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
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of());

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
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(fileGroup),
                        List.of(List.of(brokerFileStatus("s3://b/x.parquet", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("negative-load"),
                "error should call out negative-load rejection: " + thrown.getMessage());
    }

    @Test
    void columnsFromPathIsRejected() {
        BrokerFileGroup fileGroup = mockFileGroup("parquet");
        Mockito.when(fileGroup.getColumnsFromPath()).thenReturn(List.of("partition_col"));
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(fileGroup),
                        List.of(List.of(brokerFileStatus("s3://b/x.parquet", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("columns_from_path"),
                "error should call out columns_from_path rejection: " + thrown.getMessage());
    }

    @Test
    void columnExprListIsRejected() {
        BrokerFileGroup fileGroup = mockFileGroup("parquet");
        Mockito.when(fileGroup.getColumnExprList()).thenReturn(List.of(Mockito.mock(ImportColumnDesc.class)));
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of());

        StarRocksException thrown = Assertions.assertThrows(StarRocksException.class,
                () -> executor.execute(bigintRequest(
                        new BrokerDesc(Map.of()),
                        List.of(fileGroup),
                        List.of(List.of(brokerFileStatus("s3://b/x.parquet", 1024L))))));
        Assertions.assertTrue(thrown.getMessage().contains("explicit column list"),
                "error should call out column-list/SET rejection: " + thrown.getMessage());
    }

    @Test
    void wrongScanContextTypeThrows() {
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(
                        Mockito.mock(com.starrocks.catalog.TableFunctionTable.class),
                        Mockito.mock(ComputeResource.class)),
                List.of(bigintColumn("sort_key")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of());

        Assertions.assertThrows(StarRocksException.class, () -> executor.execute(request));
    }

    @Test
    void compositeSortKeyProjectsAllColumnsAndDecodesTuples() throws Exception {
        StringBuilder capturedSql = new StringBuilder();
        BrokerLoadSampleSubqueryExecutor executor = new BrokerLoadSampleSubqueryExecutor(
                /*sampleQueryRunner=*/ (sql, computeResource) -> {
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
                        Mockito.mock(ComputeResource.class)),
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
                /*sampleQueryRunner=*/ (sql, computeResource) -> List.of(jsonResultBatch(
                        "{\"data\":[1, null]}",
                        "{\"data\":[2, 42]}")));
        Column groupId = bigintColumn("group_id");
        Column nullableTrailing = nullableBigintColumn("trailing");
        SampleRequest request = new SampleRequest(
                new BrokerLoadScanContext(
                        new BrokerDesc(Map.of()),
                        List.of(mockFileGroup("parquet")),
                        List.of(List.of(brokerFileStatus("s3://b/x.parquet", 1024L))),
                        Mockito.mock(ComputeResource.class)),
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
                /*sampleQueryRunner=*/ (sql, computeResource) -> {
                    capturedResources.add(computeResource);
                    return List.of();
                });

        SampleRequest request = new SampleRequest(
                new BrokerLoadScanContext(
                        new BrokerDesc(Map.of()),
                        List.of(mockFileGroup("parquet")),
                        List.of(List.of(brokerFileStatus("s3://b/x.parquet", 1024L))),
                        expectedComputeResource),
                List.of(bigintColumn("sort_key")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);
        executor.execute(request);

        Assertions.assertEquals(1, capturedResources.size());
        Assertions.assertSame(expectedComputeResource, capturedResources.get(0));
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
                        Mockito.mock(ComputeResource.class)),
                List.of(bigintColumn("sort_key")),
                /*sampleByteLimit=*/ Long.MAX_VALUE,
                /*seed=*/ 0L);
    }
}
