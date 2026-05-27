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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.List;
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
                    any(), any(), anyLong(), any(), any(), any(), anyInt()), never());
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
        java.nio.file.Path file = Files.createTempFile(tempDirectory, "presplit-fixture-", ".parquet");
        Path outputPath = new Path(file.toUri());
        MessageType schema = MessageTypeParser.parseMessageType(schemaText);
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
}
