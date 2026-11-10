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

import com.starrocks.alter.reshard.TabletReshardUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.persist.ColumnIdExpr;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;

/**
 * Shared test fixtures for the presplit package. Centralizes the
 * cross-test-class helpers so the contract changes in one place when the
 * underlying types evolve (e.g., when {@link ScanContext} stops being an
 * empty marker).
 */
final class PresplitTestSupport {

    static final ScanContext DUMMY_CONTEXT = new ScanContext() { };

    private PresplitTestSupport() {
    }

    static Column bigintColumn(String name) {
        return new Column(name, IntegerType.BIGINT);
    }

    static Column nullableBigintColumn(String name) {
        return new Column(name, IntegerType.BIGINT, /*isAllowNull=*/ true);
    }

    static Column varcharColumn(String name) {
        return new Column(name, VarcharType.VARCHAR);
    }

    static Tuple bigintTuple(long value) {
        return new Tuple(List.of(Variant.of(IntegerType.BIGINT, Long.toString(value))));
    }

    static List<Variant> bigintRow(long value) {
        return List.of(Variant.of(IntegerType.BIGINT, Long.toString(value)));
    }

    static Tuple compositeTuple(String tenant, long position) {
        return new Tuple(List.of(
                Variant.of(VarcharType.VARCHAR, tenant),
                Variant.of(IntegerType.BIGINT, Long.toString(position))));
    }

    static List<Variant> compositeRow(String tenant, long position) {
        return List.of(
                Variant.of(VarcharType.VARCHAR, tenant),
                Variant.of(IntegerType.BIGINT, Long.toString(position)));
    }

    static final Column ACTIVITY_DATE = new Column("activity_date", DateType.DATETIME, true);
    /** What the sampler evaluates for {@link #activityMonth()} when activity_date is read from a source column. */
    static final String MONTH_SQL = "date_trunc('month', CAST(`activity_date` AS DATETIME))";

    /** A generated column defined as {@code definitionSql} over {@code schema}, stored as CREATE TABLE stores it. */
    static Column generatedColumn(String name, Type type, String definitionSql, List<Column> schema) {
        Column column = new Column(name, type, true);
        column.setGeneratedColumnExpr(ColumnIdExpr.create(schema,
                SqlParser.parseSqlToExpr(definitionSql, SqlModeHelper.MODE_DEFAULT)));
        return column;
    }

    /**
     * activity_date_month DATETIME AS date_trunc('month', activity_date). Built on call, not held as a constant here:
     * parsing needs the real GlobalStateMgr, and some tests mock it while this class may first load.
     */
    static Column activityMonth() {
        return generatedColumn("activity_date_month", DateType.DATETIME, "date_trunc('month', activity_date)",
                List.of(ACTIVITY_DATE));
    }

    /** Stubs the schema of {@code target}: {@code base}, the non-generated columns, plus {@code generated}. */
    static void stubGeneratedSchema(OlapTable target, List<Column> base, Column... generated) {
        List<Column> fullSchema = new ArrayList<>(base);
        fullSchema.addAll(List.of(generated));
        Map<ColumnId, Column> idToColumn = new HashMap<>();
        for (Column column : fullSchema) {
            idToColumn.put(column.getColumnId(), column);
        }
        Mockito.when(target.getBaseSchemaWithoutGeneratedColumn()).thenReturn(base);
        Mockito.when(target.getBaseSchema()).thenReturn(fullSchema);
        Mockito.when(target.getIdToColumn()).thenReturn(idToColumn);
    }

    /**
     * Builds a {@link ConnectContext} stub whose {@link SessionVariable} carries
     * a specific {@code enable_tablet_pre_split} value. Production hooks read
     * this value to honor the per-session opt-out; tests that go through the
     * hook's early session check should supply a properly-stubbed context
     * rather than {@code mock(ConnectContext.class)} (whose
     * {@code getSessionVariable()} returns {@code null} and would NPE the
     * check).
     */
    static ConnectContext mockConnectContextWithSessionPreSplit(boolean enablePreSplit) {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        SessionVariable sessionVariable = Mockito.mock(SessionVariable.class);
        Mockito.when(sessionVariable.isEnableTabletPreSplit()).thenReturn(enablePreSplit);
        Mockito.when(context.getSessionVariable()).thenReturn(sessionVariable);
        return context;
    }

    /**
     * Stub {@link TabletReshardUtils#computeNodeCount} to return a fixed count while delegating every
     * other static method to its real implementation. The returned MockedStatic MUST be closed by the
     * caller — declare it in a try-with-resources.
     */
    static MockedStatic<TabletReshardUtils> stubComputeNodeCount(int count) {
        MockedStatic<TabletReshardUtils> reshardUtils =
                Mockito.mockStatic(TabletReshardUtils.class, Mockito.CALLS_REAL_METHODS);
        reshardUtils.when(() -> TabletReshardUtils.computeNodeCount(any())).thenReturn(count);
        return reshardUtils;
    }

    static TBrokerFileStatus brokerFileStatus(String path, long size) {
        return new TBrokerFileStatus(path, /*isDir=*/ false, size, /*isSplitable=*/ true);
    }

    /**
     * Builds a {@link TResultBatch} carrying {@code rowJsons} as UTF-8 row
     * buffers — matches the HTTP_PROTOCAL sink shape the data tier executors
     * decode in production.
     */
    static TResultBatch jsonResultBatch(String... rowJsons) {
        List<ByteBuffer> rows = new ArrayList<>(rowJsons.length);
        for (String rowJson : rowJsons) {
            rows.add(ByteBuffer.wrap(rowJson.getBytes(StandardCharsets.UTF_8)));
        }
        TResultBatch resultBatch = new TResultBatch();
        resultBatch.setRows(rows);
        return resultBatch;
    }

    /**
     * A sample runner that answers with no rows and records the session semantics each sub-query was run with. Its
     * three-argument {@code run} fails the test: an executor must hand the runner the semantics it carries.
     */
    static final class SemanticsRecordingRunner implements AbstractSqlSampleSubqueryExecutor.SampleQueryRunner {
        final List<SampleSessionSemantics> received = new ArrayList<>();

        @Override
        public List<TResultBatch> run(String sampleSql, ComputeResource computeResource, int queryTimeoutSeconds) {
            throw new AssertionError("the sub-query was run without the load's session semantics");
        }

        @Override
        public List<TResultBatch> run(String sampleSql, ComputeResource computeResource, int queryTimeoutSeconds,
                                      String loadTimeZone, SampleSessionSemantics sessionSemantics) {
            received.add(sessionSemantics);
            return List.of();
        }
    }

    /**
     * Wraps {@code invocation} with a {@code MockedStatic} so a hook test can
     * assert the hook never reached
     * {@link TabletPreSplitCoordinator#submitAsynchronously}. "No throw" alone
     * is too weak — every hook swallows internal throws by design.
     */
    static void assertHookDoesNotDelegate(HookInvocation invocation) throws Exception {
        try (MockedStatic<TabletPreSplitCoordinator> coordinator =
                     Mockito.mockStatic(TabletPreSplitCoordinator.class)) {
            invocation.run();
            coordinator.verify(() -> TabletPreSplitCoordinator.submitAsynchronously(
                    any(), any(), anyLong(), any(), any(), any(), anyInt(), any()), never());
        }
    }

    /** Functional-interface signature for {@link #assertHookDoesNotDelegate} lambdas. */
    @FunctionalInterface
    interface HookInvocation {
        void run() throws Exception;
    }

    /**
     * Write a small Parquet fixture into {@code tempDirectory} for tests that
     * exercise the meta-tier reader / provider. Tiny page/block sizes coax the
     * writer into emitting multiple row groups even at row counts well below
     * a normal block boundary.
     */
    static Path writeParquetFixture(
            java.nio.file.Path tempDirectory,
            String schemaText,
            int rowCount,
            BiConsumer<Group, Integer> rowFiller) throws IOException {
        return writeParquetFixture(tempDirectory, MessageTypeParser.parseMessageType(schemaText), rowCount, rowFiller);
    }

    /**
     * Variant of {@link #writeParquetFixture(java.nio.file.Path, String, int, BiConsumer)} that
     * takes a prebuilt {@link MessageType}. Tests that need a precise logical-type flag the schema
     * text cannot express (e.g. TIMESTAMP {@code isAdjustedToUTC}) build the schema with the
     * {@code org.apache.parquet.schema.Types} API and call this overload.
     */
    static Path writeParquetFixture(
            java.nio.file.Path tempDirectory,
            MessageType schema,
            int rowCount,
            BiConsumer<Group, Integer> rowFiller) throws IOException {
        java.nio.file.Path file = Files.createTempFile(tempDirectory, "presplit-fixture-", ".parquet");
        Path outputPath = new Path(file.toUri());
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        Configuration configuration = new Configuration();
        configuration.setLong("parquet.block.size", 256);
        configuration.setLong("parquet.page.size", 64);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputPath)
                .withType(schema)
                .withConf(configuration)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Group group = groupFactory.newGroup();
                rowFiller.accept(group, rowIndex);
                writer.write(group);
            }
        }
        return outputPath;
    }

    /**
     * Write a two-column composite-key Parquet fixture (tenant VARCHAR + position BIGINT):
     * tenant changes every 4 rows, position ascends. Centralized here so the composite fixture
     * shape lives in one place (like {@link #writeParquetFixture}) for the provider tests that
     * exercise multi-column projection.
     */
    static Path writeCompositeParquetFixture(java.nio.file.Path tempDirectory, int rowCount) throws IOException {
        return writeParquetFixture(
                tempDirectory,
                "message schema { required binary tenant (UTF8); required int64 position; }",
                rowCount,
                (group, rowIndex) -> {
                    group.append("tenant", String.format("tenant-%02d", rowIndex / 4));
                    group.append("position", (long) rowIndex);
                });
    }

    /**
     * Resolve a local-filesystem {@link FileStatus} for a fixture {@link Path}. The footer readers
     * take a {@code FileStatus} (the load snapshots one), so reader tests pass their written fixture
     * through here.
     */
    static FileStatus statusOf(Path path) throws IOException {
        LocalFileSystem fileSystem = new LocalFileSystem();
        fileSystem.initialize(path.toUri(), new Configuration());
        return fileSystem.getFileStatus(path);
    }

    /** Fills column values for one row into an ORC {@link VectorizedRowBatch}. */
    @FunctionalInterface
    interface OrcRowFiller {
        void fill(VectorizedRowBatch batch, int batchRow, int globalRow);
    }

    /**
     * Write a small ORC fixture into {@code tempDirectory} for tests that exercise
     * the meta-tier ORC reader / provider. A tiny stripe size plus per-row memory
     * checks coax the writer into emitting multiple stripes at small row counts,
     * mirroring {@link #writeParquetFixture}'s multi-row-group trick.
     */
    static Path writeOrcFixture(
            java.nio.file.Path tempDirectory,
            String schemaText,
            int rowCount,
            OrcRowFiller rowFiller) throws IOException {
        java.nio.file.Path file = Files.createTempFile(tempDirectory, "presplit-fixture-", ".orc");
        // ORC's writer creates the file itself and refuses to overwrite; drop the
        // empty placeholder createTempFile just made.
        Files.delete(file);
        Path outputPath = new Path(file.toUri());
        TypeDescription schema = TypeDescription.fromString(schemaText);
        Configuration configuration = new Configuration();
        configuration.set("orc.stripe.size", "1");
        configuration.set("orc.rows.between.memory.checks", "1");
        try (Writer writer = OrcFile.createWriter(
                outputPath, OrcFile.writerOptions(configuration).setSchema(schema))) {
            VectorizedRowBatch batch = schema.createRowBatch();
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                int batchRow = batch.size++;
                rowFiller.fill(batch, batchRow, rowIndex);
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size != 0) {
                writer.addRowBatch(batch);
            }
        }
        return outputPath;
    }
}
