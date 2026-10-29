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

package com.starrocks.planner;

import com.starrocks.catalog.PaimonTable;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.paimon.PaimonSinkExtra;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPaimonTableSink;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PaimonTableSinkTest {
    @Test
    public void testFrozenWriterContractAndRedactedExplain() {
        FileStoreTable nativeTable = nativeTable(-1);
        PaimonTable table = table(nativeTable);
        table.resolveNativeWriteIdentity();
        PaimonSinkExtra extra = PaimonSinkExtra.from(table, "STATIC", Map.of("p", "2026-10-29"),
                "operation_1", "Asia/Shanghai");
        clearInvocations(nativeTable);
        GlobalStateMgr state = mock(GlobalStateMgr.class, RETURNS_DEEP_STUBS);
        when(state.getConnectorMgr().getConnector("paimon_write").getMetadata().getCloudConfiguration())
                .thenReturn(new CloudConfiguration());
        try (MockedStatic<GlobalStateMgr> globals = mockStatic(GlobalStateMgr.class)) {
            globals.when(GlobalStateMgr::getCurrentState).thenReturn(state);
            TupleDescriptor tuple = new TupleDescriptor(new TupleId(7));
            PaimonTableSink sink = new PaimonTableSink(table, tuple, extra);
            TDataSink thrift = sink.toThrift();
            Assertions.assertEquals(TDataSinkType.PAIMON_TABLE_SINK, thrift.getType());
            TPaimonTableSink writer = thrift.getPaimon_table_sink();
            Assertions.assertEquals(table.getId(), writer.getTarget_table_id());
            Assertions.assertEquals(7, writer.getTuple_id());
            Assertions.assertEquals(nativeTable.location().toString(), writer.getLocation());
            Assertions.assertEquals("native-identity", writer.getTable_identity());
            Assertions.assertEquals(17L, writer.getSchema_id());
            Assertions.assertEquals(PaimonSinkExtra.canonicalSchemaJson(nativeTable), writer.getSchema_json());
            Map<String, String> persistedOptions = new HashMap<>(nativeTable.schema().options());
            persistedOptions.remove("path");
            Assertions.assertEquals(nativeTable.schema().copy(persistedOptions), TableSchema.fromJson(writer.getSchema_json()));
            Assertions.assertTrue(nativeTable.schema().options().containsKey("path"));
            Assertions.assertFalse(writer.getOptions().containsKey("path"));
            Assertions.assertEquals(List.of("p", "id", "value"), writer.getColumn_names());
            Assertions.assertEquals(List.of("p"), writer.getPartition_columns());
            Assertions.assertEquals(extra.getBucketColumns(), writer.getBucket_columns());
            Assertions.assertEquals(List.of(), writer.getPrimary_key_columns());
            Assertions.assertEquals(-1, writer.getBucket_count());
            Assertions.assertEquals("starrocks-operation_1", writer.getCommit_user());
            Assertions.assertEquals(BatchWriteBuilder.COMMIT_IDENTIFIER, writer.getCommit_identifier());
            Assertions.assertEquals("operation_1", writer.getOperation_id());
            Assertions.assertEquals("STATIC", writer.getOverwrite_mode());
            Assertions.assertEquals(Map.of("p", "2026-10-29"), writer.getStatic_partition());
            Assertions.assertEquals("Asia/Shanghai", writer.getTime_zone());
            Assertions.assertEquals(PaimonSinkExtra.WRITER_PROTOCOL_VERSION, writer.getWriter_protocol_version());
            Assertions.assertTrue(writer.isSetCloud_configuration());
            Assertions.assertFalse(writer.isSetBucket_routed());
            Assertions.assertSame(extra, sink.getPaimonSinkExtra());
            Assertions.assertSame(tuple, sink.getTupleDescriptor());
            Assertions.assertNull(sink.getExchNodeId());
            Assertions.assertNull(sink.getOutputPartition());
            Assertions.assertTrue(sink.canUsePipeLine());
            Assertions.assertFalse(sink.canUseRuntimeAdaptiveDop());

            writer.getOptions().put("secret.option", "changed");
            writer.getColumn_names().clear();
            writer.getStatic_partition().clear();
            TPaimonTableSink fresh = sink.getThriftTableSink();
            Assertions.assertEquals("do-not-explain", fresh.getOptions().get("secret.option"));
            Assertions.assertEquals(3, fresh.getColumn_namesSize());
            Assertions.assertEquals(Map.of("p", "2026-10-29"), fresh.getStatic_partition());
            String explain = sink.getExplainString("  ", TExplainLevel.NORMAL);
            Assertions.assertTrue(explain.contains("Paimon TABLE SINK"));
            Assertions.assertTrue(explain.contains("OVERWRITE MODE: STATIC"));
            Assertions.assertFalse(explain.contains("do-not-explain"));
            Assertions.assertFalse(explain.contains("secret.option"));
            Assertions.assertFalse(explain.contains("file:///"));
            verify(nativeTable, never()).uuid();
        }
    }

    @Test
    public void testFixedBucketsRequireInstalledRouting() {
        FileStoreTable nativeTable = nativeTable(4);
        PaimonTable table = table(nativeTable);
        table.resolveNativeWriteIdentity();
        PaimonSinkExtra extra = PaimonSinkExtra.from(table, "APPEND", Map.of(), "operation_2", "UTC");
        GlobalStateMgr state = mock(GlobalStateMgr.class, RETURNS_DEEP_STUBS);
        when(state.getConnectorMgr().getConnector("paimon_write").getMetadata().getCloudConfiguration())
                .thenReturn(new CloudConfiguration());
        try (MockedStatic<GlobalStateMgr> globals = mockStatic(GlobalStateMgr.class)) {
            globals.when(GlobalStateMgr::getCurrentState).thenReturn(state);
            PaimonTableSink sink = new PaimonTableSink(table, new TupleDescriptor(new TupleId(1)), extra);
            Assertions.assertThrows(IllegalStateException.class, sink::toThrift);
            Assertions.assertThrows(IllegalArgumentException.class, () -> sink.setBucketRouting(-1));
            sink.setBucketRouting(99);
            TPaimonTableSink writer = sink.toThrift().getPaimon_table_sink();
            Assertions.assertTrue(writer.isBucket_routed());
            Assertions.assertEquals(99, writer.getRouting_bucket_slot_id());
            Assertions.assertTrue(sink.getExplainString("", TExplainLevel.NORMAL).contains("BUCKET ROUTING: NATIVE"));
        }
    }

    @Test
    public void testIdentityMustBeResolvedAndIsInvalidatedOnRefresh() {
        FileStoreTable nativeTable = nativeTable(-1);
        PaimonTable table = table(nativeTable);
        clearInvocations(nativeTable);
        Assertions.assertTrue(table.supportInsert());
        Assertions.assertFalse(table.supportsUpdate());
        verify(nativeTable, never()).uuid();
        Assertions.assertThrows(StarRocksConnectorException.class, table::getNativeWriteIdentity);
        table.resolveNativeWriteIdentity();
        table.resolveNativeWriteIdentity();
        verify(nativeTable).uuid();
        Assertions.assertEquals("native-identity", table.getNativeWriteIdentity());
        table.setPaimonNativeTable(nativeTable);
        Assertions.assertThrows(StarRocksConnectorException.class, table::getNativeWriteIdentity);
    }

    @Test
    public void testChangedSchemaCannotUseFrozenContract() {
        FileStoreTable nativeTable = nativeTable(-1);
        PaimonTable table = table(nativeTable);
        table.resolveNativeWriteIdentity();
        PaimonSinkExtra extra = PaimonSinkExtra.from(table, "APPEND", Map.of(), "operation_3", "UTC");
        TableSchema changed = TableSchema.create(18, Schema.newBuilder().column("other", DataTypes.INT()).build());
        when(nativeTable.schema()).thenReturn(changed);
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new PaimonTableSink(table, new TupleDescriptor(new TupleId(1)), extra));
    }

    private static PaimonTable table(FileStoreTable nativeTable) {
        return new PaimonTable("paimon_write", "db", "target",
                ColumnTypeConverter.fromPaimonSchemas(nativeTable.rowType().getFields()), nativeTable);
    }

    private static FileStoreTable nativeTable(int buckets) {
        Map<String, String> options = new HashMap<>(Map.of("bucket", Integer.toString(buckets),
                "file.format", "parquet", "secret.option", "do-not-explain",
                "path", new Path("file:///paimon/write_target").toString()));
        TableSchema schema = TableSchema.create(17, Schema.newBuilder()
                .column("p", DataTypes.DATE()).column("id", DataTypes.INT()).column("value", DataTypes.STRING())
                .partitionKeys("p").options(options).build());
        FileStoreTable nativeTable = mock(FileStoreTable.class);
        when(nativeTable.schema()).thenReturn(schema);
        when(nativeTable.rowType()).thenReturn(schema.logicalRowType());
        when(nativeTable.partitionKeys()).thenReturn(schema.partitionKeys());
        when(nativeTable.primaryKeys()).thenReturn(schema.primaryKeys());
        when(nativeTable.options()).thenReturn(options);
        when(nativeTable.uuid()).thenReturn("native-identity");
        when(nativeTable.location()).thenReturn(new Path("file:///paimon/write_target"));
        return nativeTable;
    }
}
