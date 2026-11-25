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

package com.starrocks.connector.unified;

import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.CatalogConnectorMetadata;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.hive.HiveMetadata;
import com.starrocks.connector.informationschema.InformationSchemaMetadata;
import com.starrocks.connector.metadata.TableMetaMetadata;
import com.starrocks.connector.paimon.PaimonMetadata;
import com.starrocks.connector.paimon.PaimonRemoteFileDesc;
import com.starrocks.connector.paimon.PaimonScanRangeParams;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UnifiedPaimonSystemTableReadTest {
    @Test
    void readSystemTablesFromUnifiedCatalog(@TempDir Path warehouse) throws Exception {
        try (var sdk = CatalogFactory.createCatalog(CatalogContext.create(new org.apache.paimon.fs.Path(warehouse.toUri())))) {
            sdk.createDatabase("db", false);
            Identifier orders = Identifier.create("db", "orders");
            sdk.createTable(orders, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
            var builder = sdk.getTable(orders).newBatchWriteBuilder();
            try (var write = builder.newWrite(); var commit = builder.newCommit()) {
                write.write(GenericRow.of(42));
                commit.commit(write.prepareCommit());
            }
            ((FileStoreTable) sdk.getTable(orders)).createBranch("dev");
            var hive = mock(HiveMetadata.class);
            when(hive.getTable(any(), eq("db"), eq("orders"))).thenReturn(HiveTable.builder()
                    .setTableName("orders").setFullSchema(List.of()).setProperties(Map.of(
                            UnifiedMetadata.PAIMON_STORAGE_HANDLER_KEY, UnifiedMetadata.PAIMON_STORAGE_HANDLER_VALUE)).build());
            var paimon = new PaimonMetadata("unified", new HdfsEnvironment(), sdk, new ConnectorProperties(ConnectorType.PAIMON));
            var catalog = new CatalogConnectorMetadata(new UnifiedMetadata(Map.of(
                    Table.TableType.HIVE, hive, Table.TableType.PAIMON, paimon)), new InformationSchemaMetadata("unified"),
                    new TableMetaMetadata("unified", "unified"));
            ConnectContext context = UtFrameUtils.createDefaultCtx();
            assertSystemTableRow(catalog, context, "orders$schemas", "schema_id", 0L);
            assertSystemTableRow(catalog, context, "orders$snapshots", "snapshot_id", 1L);
            assertSystemTableRow(catalog, context, "orders$branch_dev$schemas", "schema_id", 0L);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    void routeGlobalIndexSuffixByBaseTableFormat() {
        var hive = mock(HiveMetadata.class);
        var paimon = mock(PaimonMetadata.class);
        var tableMetadata = new TableMetaMetadata("unified", "unified");
        var unified = new UnifiedMetadata(Map.of(Table.TableType.HIVE, hive, Table.TableType.PAIMON, paimon));
        when(hive.getTable(any(), eq("db"), eq("orders"))).thenReturn(hiveTable("orders", Map.of()));
        when(hive.getTable(any(), eq("db"), eq("paimon_orders"))).thenReturn(hiveTable("paimon_orders", Map.of(
                UnifiedMetadata.PAIMON_STORAGE_HANDLER_KEY, UnifiedMetadata.PAIMON_STORAGE_HANDLER_VALUE)));
        when(hive.getTable(any(), eq("db"), eq("iceberg_orders"))).thenReturn(hiveTable("iceberg_orders", Map.of(
                UnifiedMetadata.ICEBERG_TABLE_TYPE_NAME, UnifiedMetadata.ICEBERG_TABLE_TYPE_VALUE)));

        assertSame(unified, unified.resolveTableForRead("db", "orders$paimon_global_index", tableMetadata));
        assertSame(unified, unified.resolveTableForRead("db", "missing$paimon_global_index", tableMetadata));
        assertSame(unified, unified.resolveTableForRead("db", "iceberg_orders$paimon_global_index", tableMetadata));
        assertSame(tableMetadata, unified.resolveTableForRead("db", "paimon_orders$paimon_global_index", tableMetadata));
        assertSame(unified, unified.resolveTableForRead("db", "paimon_orders$paimon_global_index", null));

        HiveTable physical = hiveTable("orders$paimon_global_index", Map.of());
        when(hive.getTable(any(), eq("db"), eq("orders$paimon_global_index"))).thenReturn(physical);
        var catalog = new CatalogConnectorMetadata(unified, new InformationSchemaMetadata("unified"), tableMetadata);
        assertSame(physical, catalog.getTable(new ConnectContext(), "db", "orders$paimon_global_index"));
    }

    private static HiveTable hiveTable(String name, Map<String, String> properties) {
        return HiveTable.builder().setTableName(name).setFullSchema(List.of()).setProperties(properties).build();
    }

    private void assertSystemTableRow(CatalogConnectorMetadata catalog, ConnectContext context, String name,
                                      String column, long expected) throws Exception {
        assertFalse(catalog.tableExists(context, "db", name));
        var table = assertInstanceOf(PaimonTable.class, catalog.getTable(context, "db", name));
        assertTrue(catalog.listPartitionNames("db", name, ConnectorMetadataRequestContext.DEFAULT).isEmpty());
        var version = catalog.getTableVersionRange("db", table, Optional.empty(), Optional.empty());
        var params = new PaimonScanRangeParams(GetRemoteFilesParams.newBuilder()
                .setFieldNames(List.of(column)).setTableVersionRange(version));
        var files = catalog.getRemoteFiles(table, params);
        var splits = ((PaimonRemoteFileDesc) files.get(0).getFiles().get(0)).getPaimonSplitsInfo();
        var nativeTable = splits.getNativeTable();
        int index = nativeTable.rowType().getFieldIndex(column);
        var reader = nativeTable.newReadBuilder().withProjection(new int[] {index}).newRead();
        try (var rows = new RecordReaderIterator<InternalRow>(reader.createReader(splits.getPaimonSplits()))) {
            assertTrue(rows.hasNext());
            assertEquals(expected, rows.next().getLong(0));
            assertFalse(rows.hasNext());
        }
    }
}
