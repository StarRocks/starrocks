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

package com.starrocks.connector;

import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorScanRangeSource;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.PredicateSearchKey;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.delta.DeltaConnectorScanRangeSource;
import com.starrocks.connector.delta.DeltaLakeAddFileStatsSerDe;
import com.starrocks.connector.delta.DeltaLakeMetadata;
import com.starrocks.connector.delta.DeltaMetastoreOperations;
import com.starrocks.connector.delta.DeltaStatisticProvider;
import com.starrocks.connector.delta.ScanFileUtils;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergCatalog;
import com.starrocks.connector.iceberg.IcebergCatalogProperties;
import com.starrocks.connector.iceberg.IcebergConnectorScanRangeSource;
import com.starrocks.connector.iceberg.IcebergGetRemoteFilesParams;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.connector.iceberg.IcebergRemoteFileInfo;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.connector.iceberg.cost.IcebergMetricsReporter;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.planner.DeltaLakeScanNode;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.PartitionIdGenerator;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.thrift.TScanRangeLocations;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.ScanBuilderImpl;
import io.delta.kernel.internal.ScanImpl;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.Format;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StarRocksIcebergTableScan;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.TableScanUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Resource cleanup tests shared by Iceberg and Delta Lake connectors / scan nodes.
 */
public class ExternalResourceCleanupTest {

    private TupleDescriptor tupleDescriptor;

    @BeforeEach
    public void setUp() {
        tupleDescriptor = new TupleDescriptor(new TupleId(1));
        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(1), tupleDescriptor);
        idSlot.setType(Type.INT);
        idSlot.setColumn(new Column("id", Type.INT));

        SlotDescriptor dataSlot = new SlotDescriptor(new SlotId(2), tupleDescriptor);
        dataSlot.setType(Type.VARCHAR);
        dataSlot.setColumn(new Column("data", Type.VARCHAR));

        tupleDescriptor.addSlot(idSlot);
        tupleDescriptor.addSlot(dataSlot);
    }

    @Test
    public void testIcebergScanRangeSourceClosePropagates() {
        CountingRemoteFileInfoSource countingSource = new CountingRemoteFileInfoSource();

        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", Type.INT));
        schema.add(new Column("k1", Type.INT));
        schema.add(new Column("k2", Type.VARCHAR));


        // minimal IcebergTable: nativeTable can be null for this test because getNativeTable isn't invoked in close().
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", schema, null, java.util.Collections.emptyMap());

        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(
                icebergTable,
                countingSource,
                IcebergMORParams.EMPTY,
                tupleDescriptor,
                Optional.empty(),
                PartitionIdGenerator.of(),
                false,
                false);

        scanRangeSource.close();
        Assertions.assertEquals(1, countingSource.closeCount.get());
        Assertions.assertTrue(countingSource.closed);
    }

    @Test
    public void testIcebergScanRangeSourceCloseIdempotentAndSuppressesException() {
        CountingRemoteFileInfoSource countingSource = new CountingRemoteFileInfoSource(true /*throwOnClose*/);
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", List.of(), null, java.util.Collections.emptyMap());

        IcebergConnectorScanRangeSource scanRangeSource = new IcebergConnectorScanRangeSource(
                icebergTable,
                countingSource,
                IcebergMORParams.EMPTY,
                tupleDescriptor,
                Optional.empty(),
                PartitionIdGenerator.of(),
                false,
                false);

        // two closes: first throws (suppressed inside close), second also throws; should not escape
        scanRangeSource.close();
        scanRangeSource.close();
        Assertions.assertEquals(2, countingSource.closeCount.get());
        Assertions.assertTrue(countingSource.closed);
    }

    @Test
    public void testDeltaConnectorScanRangeSourceClosePropagates() {
        Format format = Mockito.mock(Format.class);
        Mockito.when(format.getProvider()).thenReturn("parquet");

        Metadata metadata = Mockito.mock(Metadata.class);
        Mockito.when(metadata.getFormat()).thenReturn(format);

        DeltaLakeTable table = Mockito.mock(DeltaLakeTable.class);
        Mockito.when(table.getPartitionColumnNames()).thenReturn(List.of());
        Mockito.when(table.getDeltaMetadata()).thenReturn(metadata);
        Mockito.when(table.getDeltaSnapshot()).thenReturn(Mockito.mock(SnapshotImpl.class));
        Mockito.when(table.getDeltaEngine()).thenReturn(Mockito.mock(Engine.class));
        Mockito.when(table.getTableLocation()).thenReturn("/tmp/table");
        Mockito.when(table.getId()).thenReturn(1L);

        CountingRemoteFileInfoSource countingSource = new CountingRemoteFileInfoSource();
        DeltaConnectorScanRangeSource scanRangeSource =
                new DeltaConnectorScanRangeSource(table, countingSource, com.starrocks.planner.PartitionIdGenerator.of());

        scanRangeSource.close();
        Assertions.assertEquals(1, countingSource.closeCount.get());
        Assertions.assertTrue(countingSource.closed);
    }

    @Test
    public void testDeltaConnectorScanRangeSourceCloseIdempotent() {
        Format format = Mockito.mock(Format.class);
        Mockito.when(format.getProvider()).thenReturn("parquet");

        Metadata metadata = Mockito.mock(Metadata.class);
        Mockito.when(metadata.getFormat()).thenReturn(format);

        DeltaLakeTable table = Mockito.mock(DeltaLakeTable.class);
        Mockito.when(table.getPartitionColumnNames()).thenReturn(List.of());
        Mockito.when(table.getDeltaMetadata()).thenReturn(metadata);
        Mockito.when(table.getDeltaSnapshot()).thenReturn(Mockito.mock(SnapshotImpl.class));
        Mockito.when(table.getDeltaEngine()).thenReturn(Mockito.mock(Engine.class));
        Mockito.when(table.getTableLocation()).thenReturn("/tmp/table");
        Mockito.when(table.getId()).thenReturn(1L);

        CountingRemoteFileInfoSource countingSource = new CountingRemoteFileInfoSource();
        DeltaConnectorScanRangeSource scanRangeSource =
                new DeltaConnectorScanRangeSource(table, countingSource, com.starrocks.planner.PartitionIdGenerator.of());

        scanRangeSource.close();
        scanRangeSource.close();
        Assertions.assertEquals(2, countingSource.closeCount.get());
        Assertions.assertTrue(countingSource.closed);
    }

    @Test
    public void testDeltaConnectorScanRangeSourceCloseSuppressesException() {
        Format format = Mockito.mock(Format.class);
        Mockito.when(format.getProvider()).thenReturn("parquet");
        Metadata metadata = Mockito.mock(Metadata.class);
        Mockito.when(metadata.getFormat()).thenReturn(format);

        DeltaLakeTable table = Mockito.mock(DeltaLakeTable.class);
        Mockito.when(table.getPartitionColumnNames()).thenReturn(List.of());
        Mockito.when(table.getDeltaMetadata()).thenReturn(metadata);
        Mockito.when(table.getDeltaSnapshot()).thenReturn(Mockito.mock(SnapshotImpl.class));
        Mockito.when(table.getDeltaEngine()).thenReturn(Mockito.mock(Engine.class));
        Mockito.when(table.getTableLocation()).thenReturn("/tmp/table");
        Mockito.when(table.getId()).thenReturn(1L);

        CountingRemoteFileInfoSource countingSource = new CountingRemoteFileInfoSource(true /*throwOnClose*/);
        DeltaConnectorScanRangeSource scanRangeSource =
                new DeltaConnectorScanRangeSource(table, countingSource, com.starrocks.planner.PartitionIdGenerator.of());

        scanRangeSource.close();
        scanRangeSource.close();
        Assertions.assertEquals(2, countingSource.closeCount.get());
        Assertions.assertTrue(countingSource.closed);
    }

    @Test
    public void testDeltaScanNodeClearClosesScanRangeSource() throws Exception {
        DeltaLakeTable table = Mockito.mock(DeltaLakeTable.class);
        Mockito.when(table.getCatalogName()).thenReturn(null); // skip cloud credential setup
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(1));
        tupleDescriptor.setTable(table);

        DeltaLakeScanNode scanNode = new DeltaLakeScanNode(new PlanNodeId(1), tupleDescriptor, "DeltaLakeScanNode");
        DeltaConnectorScanRangeSource scanRangeSource = Mockito.mock(DeltaConnectorScanRangeSource.class);

        Field field = DeltaLakeScanNode.class.getDeclaredField("scanRangeSource");
        field.setAccessible(true);
        field.set(scanNode, scanRangeSource);

        scanNode.clear();

        Mockito.verify(scanRangeSource, Mockito.times(1)).close();
        Assertions.assertNull(field.get(scanNode));
    }

    @Test
    public void testDeltaScanNodeClearIdempotent() throws Exception {
        DeltaLakeTable table = Mockito.mock(DeltaLakeTable.class);
        Mockito.when(table.getCatalogName()).thenReturn(null);
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(1));
        tupleDescriptor.setTable(table);

        DeltaLakeScanNode scanNode = new DeltaLakeScanNode(new PlanNodeId(1), tupleDescriptor, "DeltaLakeScanNode");
        DeltaConnectorScanRangeSource scanRangeSource = Mockito.mock(DeltaConnectorScanRangeSource.class);

        Field field = DeltaLakeScanNode.class.getDeclaredField("scanRangeSource");
        field.setAccessible(true);
        field.set(scanNode, scanRangeSource);

        scanNode.clear();
        scanNode.clear(); // second clear should be safe

        Mockito.verify(scanRangeSource, Mockito.times(1)).close();
        Assertions.assertNull(field.get(scanNode));
    }

    @Test
    public void testDeltaScanNodeClearWhenNoSource() {
        DeltaLakeTable table = Mockito.mock(DeltaLakeTable.class);
        Mockito.when(table.getCatalogName()).thenReturn(null);
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(1));
        tupleDescriptor.setTable(table);

        DeltaLakeScanNode scanNode = new DeltaLakeScanNode(new PlanNodeId(1), tupleDescriptor, "DeltaLakeScanNode");
        // scanRangeSource remains null
        scanNode.clear(); // should not throw
    }

    @Test
    public void testIcebergScanNodeClearClosesAndClearsMetadata() throws Exception {
        IcebergTable icebergTable = Mockito.mock(IcebergTable.class);
        Mockito.when(icebergTable.getCatalogName()).thenReturn(null); // skip cloud credential lookup
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(2));
        tupleDescriptor.setTable(icebergTable);

        IcebergScanNode scanNode = new IcebergScanNode(new PlanNodeId(2), tupleDescriptor, "IcebergScanNode",
                IcebergTableMORParams.EMPTY, IcebergMORParams.EMPTY, PartitionIdGenerator.of());
        IcebergConnectorScanRangeSource scanRangeSource = Mockito.mock(IcebergConnectorScanRangeSource.class);
        Field field = IcebergScanNode.class.getDeclaredField("scanRangeSource");
        field.setAccessible(true);
        field.set(scanNode, scanRangeSource);

        scanNode.clear();

        Mockito.verify(icebergTable, Mockito.times(1)).clearMetadata();
        Mockito.verify(scanRangeSource, Mockito.times(1)).close();
        Assertions.assertNull(field.get(scanNode));
    }

    @Test
    public void testIcebergTableClearMetadataNullsNativeTable() throws Exception {
        List<Column> schema = List.of(new Column("c1", Type.INT));
        IcebergTable icebergTable = new IcebergTable(2, "tbl", "cat", "res", "db", "tbl",
                "", schema, null, java.util.Collections.emptyMap());
        Field nativeField = IcebergTable.class.getDeclaredField("nativeTable");
        nativeField.setAccessible(true);
        nativeField.set(icebergTable, Mockito.mock(Table.class));

        icebergTable.clearMetadata();

        Assertions.assertNull(nativeField.get(icebergTable));
    }

    @Test
    public void testDeltaLakeTableClearMetadataNullsSnapshotAndEngine() throws Exception {
        MetastoreTable metastore = Mockito.mock(MetastoreTable.class);
        Mockito.when(metastore.getCreateTime()).thenReturn(0L);
        Mockito.when(metastore.getTableLocation()).thenReturn("/tmp");
        SnapshotImpl snapshot = Mockito.mock(SnapshotImpl.class);
        Engine engine = Mockito.mock(Engine.class);
        DeltaLakeTable table = new DeltaLakeTable(3, "catalog", "db", "tbl",
                List.of(), List.of(), snapshot, engine, metastore);

        table.clearMetadata();

        Assertions.assertNull(table.getDeltaSnapshot());
    }

    @Test
    public void testConnectorScanRangeSourceDefaultCloseNoOp() throws Exception {
        ConnectorScanRangeSource source = new ConnectorScanRangeSource() {
            @Override
            protected List<TScanRangeLocations> getSourceOutputs(int maxSize) {
                return List.of();
            }

            @Override
            protected boolean sourceHasMoreOutput() {
                return false;
            }
        };

        Assertions.assertDoesNotThrow(source::close);
        Assertions.assertFalse(source.hasMoreOutput());
        Assertions.assertTrue(source.getOutputs(1).isEmpty());
    }

    @Test
    public void testDeltaLakeMetadataIteratorAndCollect() throws Exception {
        // Prepare metadata with mocked delta ops and statistic provider.
        DeltaMetastoreOperations deltaOps = Mockito.mock(DeltaMetastoreOperations.class);
        DeltaLakeMetadata metadata = new DeltaLakeMetadata(
                new HdfsEnvironment(new java.util.HashMap<>()), "delta0", deltaOps, null,
                new ConnectorProperties(ConnectorType.DELTALAKE));
        Field statField = DeltaLakeMetadata.class.getDeclaredField("statisticProvider");
        statField.setAccessible(true);
        statField.set(metadata, Mockito.mock(DeltaStatisticProvider.class));

        // Prepare table and delta components.
        DeltaLakeTable table = Mockito.mock(DeltaLakeTable.class);
        Engine engine = Mockito.mock(Engine.class);
        SnapshotImpl snapshot = Mockito.mock(SnapshotImpl.class);
        Metadata meta = Mockito.mock(Metadata.class);
        StructType schema = Mockito.mock(StructType.class);
        StructField structField = Mockito.mock(StructField.class);
        Mockito.when(structField.getDataType()).thenReturn(Mockito.mock(io.delta.kernel.types.DataType.class));
        Mockito.when(schema.fieldNames()).thenReturn(List.of("c1"));
        Mockito.when(schema.get("c1")).thenReturn(structField);

        Mockito.when(table.getDeltaMetadata()).thenReturn(meta);
        Mockito.when(table.getDeltaEngine()).thenReturn(engine);
        Mockito.when(table.getDeltaSnapshot()).thenReturn(snapshot);
        Mockito.when(table.getColumns()).thenReturn(List.of(new Column("c1", Type.INT)));
        Mockito.when(table.getCatalogDBName()).thenReturn("db");
        Mockito.when(table.getCatalogTableName()).thenReturn("tbl");
        Mockito.when(meta.getSchema()).thenReturn(schema);
        Mockito.when(meta.getPartitionColNames()).thenReturn(Set.of());
        Mockito.when(snapshot.getVersion(engine)).thenReturn(1L);

        // Mock scan builder flow.
        ScanBuilderImpl scanBuilder = Mockito.mock(ScanBuilderImpl.class);
        ScanImpl scan = Mockito.mock(ScanImpl.class);
        Mockito.when(snapshot.getScanBuilder(engine)).thenReturn(scanBuilder);
        Mockito.when(scanBuilder.withFilter(Mockito.eq(engine), Mockito.any())).thenReturn(scanBuilder);
        Mockito.when(scanBuilder.build()).thenReturn(scan);

        // Mock row/batch iterators.
        Row row = Mockito.mock(Row.class);
        TestRowIterator rowIter = new TestRowIterator(row);
        FilteredColumnarBatch batch = Mockito.mock(FilteredColumnarBatch.class);
        Mockito.when(batch.getRows()).thenReturn(rowIter);
        TestBatchIterator batchIter = new TestBatchIterator(batch);
        Mockito.when(scan.getScanFiles(engine, true)).thenReturn(batchIter);

        FileScanTask fileTask = Mockito.mock(FileScanTask.class);
        DeltaLakeAddFileStatsSerDe serDe = Mockito.mock(DeltaLakeAddFileStatsSerDe.class);

        try (MockedStatic<InternalScanFileUtils> internalMock = Mockito.mockStatic(InternalScanFileUtils.class);
                MockedStatic<ScanFileUtils> scanFileMock = Mockito.mockStatic(ScanFileUtils.class)) {
            internalMock.when(() -> InternalScanFileUtils.getDeletionVectorDescriptorFromRow(row))
                    .thenReturn((DeletionVectorDescriptor) null);
            scanFileMock.when(() -> ScanFileUtils.convertFromRowToFileScanTask(
                    Mockito.anyBoolean(), Mockito.eq(row), Mockito.eq(meta), Mockito.anyLong(), Mockito.isNull()))
                    .thenReturn(Pair.create(fileTask, serDe));

            // invoke iterator
            Method iteratorMethod = DeltaLakeMetadata.class.getDeclaredMethod(
                    "buildFileScanTaskIterator", com.starrocks.catalog.Table.class,
                    com.starrocks.sql.optimizer.operator.scalar.ScalarOperator.class, boolean.class);
            iteratorMethod.setAccessible(true);
            @SuppressWarnings("unchecked")
            CloseableIterator<Pair<FileScanTask, DeltaLakeAddFileStatsSerDe>> iterator =
                    (CloseableIterator<Pair<FileScanTask, DeltaLakeAddFileStatsSerDe>>)
                            iteratorMethod.invoke(metadata, table, ConstantOperator.TRUE, false);

            Assertions.assertTrue(iterator.hasNext());
            Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> pair = iterator.next();
            Assertions.assertEquals(fileTask, pair.first);
            Assertions.assertFalse(iterator.hasNext());
            iterator.close();
            Assertions.assertTrue(rowIter.closed);
            Assertions.assertTrue(batchIter.closed);

            // trigger collectDeltaLakePlanFiles to populate splitTasks
            GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                    .setPredicate(ConstantOperator.TRUE)
                    .setTableVersionRange(TableVersionRange.withEnd(Optional.of(1L)))
                    .build();
            PredicateSearchKey key = PredicateSearchKey.of("db", "tbl", params);

            Method collectMethod = DeltaLakeMetadata.class.getDeclaredMethod(
                    "collectDeltaLakePlanFiles", PredicateSearchKey.class, com.starrocks.catalog.Table.class,
                    com.starrocks.sql.optimizer.operator.scalar.ScalarOperator.class,
                    com.starrocks.qe.ConnectContext.class, List.class);
            collectMethod.setAccessible(true);
            collectMethod.invoke(metadata, key, table, ConstantOperator.TRUE, null, List.of("c1"));

            Field splitField = DeltaLakeMetadata.class.getDeclaredField("splitTasks");
            splitField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<PredicateSearchKey, List<FileScanTask>> splits =
                    (Map<PredicateSearchKey, List<FileScanTask>>) splitField.get(metadata);
            Assertions.assertTrue(splits.containsKey(key));
        }
    }

    @Test
    public void testIcebergMetadataUsesCachedSplitTasks() throws Exception {
        ExecutorService exec = Executors.newSingleThreadExecutor();
        IcebergCatalog icebergCatalog = Mockito.mock(IcebergCatalog.class);
        IcebergCatalogProperties catalogProps = new IcebergCatalogProperties(
                java.util.Map.of(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, "hive"));
        IcebergMetadata metadata = new IcebergMetadata("ice", new HdfsEnvironment(new java.util.HashMap<>()),
                icebergCatalog, exec, exec, catalogProps);

        IcebergTable table = Mockito.mock(IcebergTable.class);
        Mockito.when(table.getCatalogDBName()).thenReturn("db");
        Mockito.when(table.getCatalogTableName()).thenReturn("tbl");

        IcebergGetRemoteFilesParams.Builder builder = IcebergGetRemoteFilesParams.newBuilder();
        builder.setTableVersionRange(TableVersionRange.withEnd(Optional.of(1L)));
        builder.setPredicate(ConstantOperator.TRUE);
        builder.setAllParams(IcebergTableMORParams.EMPTY);
        builder.setParams(IcebergMORParams.EMPTY);
        IcebergGetRemoteFilesParams params = (IcebergGetRemoteFilesParams) builder.build();
        PredicateSearchKey key = PredicateSearchKey.of("db", "tbl", params);

        FileScanTask task = Mockito.mock(FileScanTask.class);
        Field splitField = IcebergMetadata.class.getDeclaredField("splitTasks");
        splitField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<PredicateSearchKey, List<FileScanTask>> splitTasks =
                (Map<PredicateSearchKey, List<FileScanTask>>) splitField.get(metadata);
        splitTasks.put(key, List.of(task));

        com.starrocks.connector.RemoteFileInfoSource source = metadata.getRemoteFilesAsync(table, params);
        Assertions.assertTrue(source.hasMoreOutput());
        IcebergRemoteFileInfo info = (IcebergRemoteFileInfo) source.getOutput();
        Assertions.assertEquals(task, info.getFileScanTask());
    }

    @Test
    public void testIcebergMetadataEmptyVersionIterator() throws Exception {
        IcebergCatalog icebergCatalog = Mockito.mock(IcebergCatalog.class);
        IcebergCatalogProperties catalogProps = new IcebergCatalogProperties(
                java.util.Map.of(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, "hive"));
        ExecutorService exec = Executors.newSingleThreadExecutor();
        IcebergMetadata metadata = new IcebergMetadata("ice", new HdfsEnvironment(new java.util.HashMap<>()),
                icebergCatalog, exec, exec, catalogProps);

        IcebergTable table = Mockito.mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTbl = Mockito.mock(org.apache.iceberg.Table.class);
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(List.of());
        IcebergMetricsReporter reporter = new IcebergMetricsReporter();
        Mockito.when(nativeTbl.schema()).thenReturn(schema);
        Mockito.when(nativeTbl.properties()).thenReturn(java.util.Collections.emptyMap());
        Mockito.when(table.getNativeTable()).thenReturn(nativeTbl);
        Mockito.when(table.getIcebergMetricsReporter()).thenReturn(reporter);
        Mockito.when(table.getCatalogDBName()).thenReturn("db");
        Mockito.when(table.getCatalogTableName()).thenReturn("tbl");

        StarRocksIcebergTableScan tableScan = Mockito.mock(StarRocksIcebergTableScan.class);
        Mockito.when(icebergCatalog.getTableScan(Mockito.eq(nativeTbl), Mockito.any())).thenReturn(tableScan);
        Mockito.when(tableScan.useSnapshot(Mockito.anyLong())).thenReturn(tableScan);
        Mockito.when(tableScan.metricsReporter(Mockito.any())).thenReturn(tableScan);
        Mockito.when(tableScan.planWith(Mockito.any())).thenReturn(tableScan);
        Mockito.when(tableScan.includeColumnStats()).thenReturn(tableScan);
        Mockito.when(tableScan.filter(Mockito.any())).thenReturn(tableScan);
        Mockito.when(tableScan.targetSplitSize()).thenReturn(128L);
        Mockito.when(tableScan.planFiles()).thenReturn(CloseableIterable.withNoopClose(List.of()));
        Mockito.when(tableScan.getMetricsReporter()).thenReturn(new IcebergMetricsReporter());
        Mockito.when(tableScan.getIcebergTableName())
                .thenReturn(new com.starrocks.connector.iceberg.CachingIcebergCatalog.IcebergTableName("db", "tbl"));

        Method m = IcebergMetadata.class.getDeclaredMethod(
                "buildFileScanTaskIterator", IcebergTable.class, org.apache.iceberg.expressions.Expression.class,
                Long.class, com.starrocks.qe.ConnectContext.class, boolean.class);
        m.setAccessible(true);
        org.apache.iceberg.io.CloseableIterator<FileScanTask> iter =
                (org.apache.iceberg.io.CloseableIterator<FileScanTask>) m.invoke(
                        metadata, table, org.apache.iceberg.expressions.Expressions.alwaysTrue(),
                        -1L, null, false);
        Assertions.assertFalse(iter.hasNext());
        iter.close();
    }

    @Test
    public void testIcebergMetadataNullMetricsReporterThrows() throws Exception {
        IcebergCatalog icebergCatalog = Mockito.mock(IcebergCatalog.class);
        IcebergCatalogProperties catalogProps = new IcebergCatalogProperties(
                java.util.Map.of(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, "hive"));
        ExecutorService exec = Executors.newSingleThreadExecutor();
        IcebergMetadata metadata = new IcebergMetadata("ice", new HdfsEnvironment(new java.util.HashMap<>()),
                icebergCatalog, exec, exec, catalogProps);

        IcebergTable table = Mockito.mock(IcebergTable.class);
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(List.of());
        org.apache.iceberg.Table nativeTbl = Mockito.mock(org.apache.iceberg.Table.class);
        Mockito.when(nativeTbl.schema()).thenReturn(schema);
        Mockito.when(nativeTbl.properties()).thenReturn(java.util.Collections.emptyMap());
        Mockito.when(table.getNativeTable()).thenReturn(nativeTbl);
        Mockito.when(table.getCatalogDBName()).thenReturn("db");
        Mockito.when(table.getCatalogTableName()).thenReturn("tbl");
        Mockito.when(table.getIcebergMetricsReporter()).thenReturn(null);

        Method m = IcebergMetadata.class.getDeclaredMethod(
                "buildFileScanTaskIterator", IcebergTable.class, org.apache.iceberg.expressions.Expression.class,
                Long.class, com.starrocks.qe.ConnectContext.class, boolean.class);
        m.setAccessible(true);
        Assertions.assertThrows(java.lang.reflect.InvocationTargetException.class, () -> m.invoke(
                metadata, table, org.apache.iceberg.expressions.Expressions.alwaysTrue(),
                1L, null, false));
    }

    @Test
    public void testIcebergMetadataIncrementalIterator() throws Exception {
        ExecutorService exec = Executors.newSingleThreadExecutor();
        IcebergCatalog icebergCatalog = Mockito.mock(IcebergCatalog.class);
        IcebergCatalogProperties catalogProps = new IcebergCatalogProperties(
                java.util.Map.of(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, "hive"));
        IcebergMetadata metadata = new IcebergMetadata("ice", new HdfsEnvironment(new java.util.HashMap<>()),
                icebergCatalog, exec, exec, catalogProps);

        IcebergTable table = Mockito.mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTbl = Mockito.mock(org.apache.iceberg.Table.class);
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(List.of());
        IcebergMetricsReporter reporter = new IcebergMetricsReporter();
        Mockito.when(table.getNativeTable()).thenReturn(nativeTbl);
        Mockito.when(table.getIcebergMetricsReporter()).thenReturn(reporter);
        Mockito.when(table.getCatalogDBName()).thenReturn("db");
        Mockito.when(table.getCatalogTableName()).thenReturn("tbl");
        Mockito.when(nativeTbl.schema()).thenReturn(schema);
        Mockito.when(nativeTbl.properties()).thenReturn(java.util.Collections.emptyMap());

        FileScanTask task = Mockito.mock(FileScanTask.class);
        CloseableIterable<FileScanTask> iterable = CloseableIterable.withNoopClose(List.of(task));

        StarRocksIcebergTableScan tableScan = Mockito.mock(StarRocksIcebergTableScan.class);
        Mockito.when(icebergCatalog.getTableScan(Mockito.eq(nativeTbl), Mockito.any())).thenReturn(tableScan);
        Mockito.when(tableScan.useSnapshot(Mockito.anyLong())).thenReturn(tableScan);
        Mockito.when(tableScan.metricsReporter(Mockito.any())).thenReturn(tableScan);
        Mockito.when(tableScan.planWith(Mockito.any())).thenReturn(tableScan);
        Mockito.when(tableScan.includeColumnStats()).thenReturn(tableScan);
        Mockito.when(tableScan.filter(Mockito.any())).thenReturn(tableScan);
        Mockito.when(tableScan.targetSplitSize()).thenReturn(128L);
        Mockito.when(tableScan.planFiles()).thenReturn(iterable);
        Mockito.when(tableScan.getMetricsReporter()).thenReturn(new IcebergMetricsReporter());
        Mockito.when(tableScan.getIcebergTableName())
                .thenReturn(new com.starrocks.connector.iceberg.CachingIcebergCatalog.IcebergTableName("db", "tbl"));

        Method m = IcebergMetadata.class.getDeclaredMethod(
                "buildFileScanTaskIterator", IcebergTable.class, org.apache.iceberg.expressions.Expression.class,
                Long.class, com.starrocks.qe.ConnectContext.class, boolean.class);
        m.setAccessible(true);
        org.apache.iceberg.io.CloseableIterator<FileScanTask> iter =
                (org.apache.iceberg.io.CloseableIterator<FileScanTask>) m.invoke(
                        metadata, table, org.apache.iceberg.expressions.Expressions.alwaysTrue(),
                        2L, null, true);
        // iterator should be created and be consumable without exception
        iter.hasNext();
        iter.close();
    }

    @Test
    public void testIcebergMetadataGetDeleteFiles() {
        ExecutorService exec = Executors.newSingleThreadExecutor();
        IcebergCatalog icebergCatalog = Mockito.mock(IcebergCatalog.class);
        IcebergCatalogProperties catalogProps = new IcebergCatalogProperties(
                java.util.Map.of(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, "hive"));
        IcebergMetadata metadata = new IcebergMetadata("ice", new HdfsEnvironment(new java.util.HashMap<>()),
                icebergCatalog, exec, exec, catalogProps);

        IcebergTable table = Mockito.mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTbl = Mockito.mock(org.apache.iceberg.Table.class);
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(List.of());
        Mockito.when(nativeTbl.schema()).thenReturn(schema);
        Mockito.when(table.getNativeTable()).thenReturn(nativeTbl);
        Mockito.when(table.getCatalogDBName()).thenReturn("db");
        Mockito.when(table.getCatalogTableName()).thenReturn("tbl");

        StarRocksIcebergTableScan scan = Mockito.mock(StarRocksIcebergTableScan.class);
        Mockito.when(scan.useSnapshot(Mockito.anyLong())).thenReturn(scan);
        Mockito.when(scan.metricsReporter(Mockito.any())).thenReturn(scan);
        Mockito.when(scan.planWith(Mockito.any())).thenReturn(scan);
        Mockito.when(scan.filter(Mockito.any())).thenReturn(scan);
        Mockito.when(scan.getDeleteFiles(Mockito.any(FileContent.class))).thenReturn(Set.<DeleteFile>of());
        Mockito.when(icebergCatalog.getTableScan(Mockito.eq(nativeTbl), Mockito.any())).thenReturn(scan);

        Assertions.assertTrue(metadata.getDeleteFiles(table, 1L, ConstantOperator.TRUE, FileContent.EQUALITY_DELETES).isEmpty());
    }

    private static class CountingRemoteFileInfoSource implements RemoteFileInfoSource {
        private final AtomicInteger closeCount = new AtomicInteger();
        private volatile boolean closed = false;
        private final boolean throwOnClose;

        CountingRemoteFileInfoSource() {
            this(false);
        }

        CountingRemoteFileInfoSource(boolean throwOnClose) {
            this.throwOnClose = throwOnClose;
        }

        @Override
        public RemoteFileInfo getOutput() {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public boolean hasMoreOutput() {
            return false;
        }

        @Override
        public List<RemoteFileInfo> getAllOutputs() {
            return List.of();
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
            closed = true;
            if (throwOnClose) {
                throw new RuntimeException("close error");
            }
        }
    }

    // Minimal CloseableIterator implementations to validate close propagation.
    private static class TestBatchIterator implements CloseableIterator<FilteredColumnarBatch> {
        private final FilteredColumnarBatch batch;
        boolean closed = false;
        boolean used = false;

        TestBatchIterator(FilteredColumnarBatch batch) {
            this.batch = batch;
        }

        @Override
        public boolean hasNext() {
            return !used;
        }

        @Override
        public FilteredColumnarBatch next() {
            used = true;
            return batch;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static class TestRowIterator implements CloseableIterator<Row> {
        private final Row row;
        boolean closed = false;
        boolean used = false;

        TestRowIterator(Row row) {
            this.row = row;
        }

        @Override
        public boolean hasNext() {
            return !used;
        }

        @Override
        public Row next() {
            used = true;
            return row;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    @Test
    public void testDeltaScanNodeClearHandlesCloseException() throws Exception {
        DeltaLakeTable table = Mockito.mock(DeltaLakeTable.class);
        Mockito.when(table.getCatalogName()).thenReturn(null);
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(1));
        tupleDescriptor.setTable(table);

        DeltaLakeScanNode scanNode = new DeltaLakeScanNode(new PlanNodeId(1), tupleDescriptor, "DeltaLakeScanNode");
        DeltaConnectorScanRangeSource scanRangeSource = Mockito.mock(DeltaConnectorScanRangeSource.class);
        Mockito.doThrow(new RuntimeException("close error")).when(scanRangeSource).close();

        Field field = DeltaLakeScanNode.class.getDeclaredField("scanRangeSource");
        field.setAccessible(true);
        field.set(scanNode, scanRangeSource);

        Assertions.assertDoesNotThrow(scanNode::clear);
        Mockito.verify(scanRangeSource, Mockito.times(1)).close();
        Assertions.assertNull(field.get(scanNode));
    }

    @Test
    public void testIcebergScanNodeClearHandlesCloseException() throws Exception {
        IcebergTable icebergTable = Mockito.mock(IcebergTable.class);
        Mockito.when(icebergTable.getCatalogName()).thenReturn(null);
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(2));
        tupleDescriptor.setTable(icebergTable);

        IcebergScanNode scanNode = new IcebergScanNode(new PlanNodeId(2), tupleDescriptor, "IcebergScanNode",
                IcebergTableMORParams.EMPTY, IcebergMORParams.EMPTY, PartitionIdGenerator.of());
        IcebergConnectorScanRangeSource scanRangeSource = Mockito.mock(IcebergConnectorScanRangeSource.class);
        Mockito.doThrow(new RuntimeException("close error")).when(scanRangeSource).close();

        Field field = IcebergScanNode.class.getDeclaredField("scanRangeSource");
        field.setAccessible(true);
        field.set(scanNode, scanRangeSource);

        Assertions.assertDoesNotThrow(scanNode::clear);
        Mockito.verify(icebergTable, Mockito.times(1)).clearMetadata();
        Mockito.verify(scanRangeSource, Mockito.times(1)).close();
        Assertions.assertNull(field.get(scanNode));
    }

    @Test
    public void testIcebergMetadataIteratorThrowsIOException() throws Exception {
        ExecutorService exec = Executors.newSingleThreadExecutor();
        IcebergCatalog icebergCatalog = Mockito.mock(IcebergCatalog.class);
        IcebergCatalogProperties catalogProps = new IcebergCatalogProperties(
                java.util.Map.of(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, "hive"));
        IcebergMetadata metadata = new IcebergMetadata("ice", new HdfsEnvironment(new java.util.HashMap<>()),
                icebergCatalog, exec, exec, catalogProps);

        IcebergTable table = Mockito.mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTbl = Mockito.mock(org.apache.iceberg.Table.class);
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(List.of());
        Mockito.when(nativeTbl.schema()).thenReturn(schema);
        Mockito.when(nativeTbl.properties()).thenReturn(java.util.Collections.emptyMap());
        Mockito.when(table.getNativeTable()).thenReturn(nativeTbl);
        Mockito.when(table.getCatalogDBName()).thenReturn("db");
        Mockito.when(table.getCatalogTableName()).thenReturn("tbl");
        Mockito.when(table.getIcebergMetricsReporter()).thenReturn(new IcebergMetricsReporter());

        org.apache.iceberg.StarRocksIcebergTableScan tableScan =
                Mockito.mock(org.apache.iceberg.StarRocksIcebergTableScan.class);
        Mockito.when(icebergCatalog.getTableScan(Mockito.eq(nativeTbl), Mockito.any())).thenReturn(tableScan);
        Mockito.when(tableScan.useSnapshot(Mockito.anyLong())).thenReturn(tableScan);
        Mockito.when(tableScan.metricsReporter(Mockito.any())).thenReturn(tableScan);
        Mockito.when(tableScan.planWith(Mockito.any())).thenReturn(tableScan);
        Mockito.when(tableScan.includeColumnStats()).thenReturn(tableScan);
        Mockito.when(tableScan.filter(Mockito.any())).thenReturn(tableScan);
        Mockito.when(tableScan.targetSplitSize()).thenReturn(128L);
        Mockito.when(tableScan.planFiles()).thenReturn(null);
        Mockito.when(tableScan.getMetricsReporter()).thenReturn(new IcebergMetricsReporter());

        Method collectMethod = IcebergMetadata.class.getDeclaredMethod(
                "collectTableStatisticsAndCacheIcebergSplit", PredicateSearchKey.class, com.starrocks.catalog.Table.class,
                com.starrocks.common.profile.Tracers.class, com.starrocks.qe.ConnectContext.class);
        collectMethod.setAccessible(true);

        IcebergGetRemoteFilesParams.Builder builder = IcebergGetRemoteFilesParams.newBuilder();
        builder.setTableVersionRange(TableVersionRange.withEnd(Optional.of(1L)));
        builder.setPredicate(ConstantOperator.TRUE);
        builder.setAllParams(IcebergTableMORParams.EMPTY);
        builder.setParams(IcebergMORParams.EMPTY);
        IcebergGetRemoteFilesParams params = (IcebergGetRemoteFilesParams) builder.build();
        PredicateSearchKey key = PredicateSearchKey.of("db", "tbl", params);

        try (MockedStatic<TableScanUtil> tableScanUtilMock = Mockito.mockStatic(TableScanUtil.class)) {
            CloseableIterable<FileScanTask> iterable = new CloseableIterable<>() {
                @Override
                public void close() throws IOException {
                    // no-op
                }

                @Override
                public org.apache.iceberg.io.CloseableIterator<FileScanTask> iterator() {
                    return new org.apache.iceberg.io.CloseableIterator<>() {
                        @Override
                        public void close() throws IOException {
                            // simulate close error; swallowed by iterator implementation
                            throw new IOException("boom");
                        }

                        @Override
                        public boolean hasNext() {
                            return false;
                        }

                        @Override
                        public FileScanTask next() {
                            throw new NoSuchElementException();
                        }
                    };
                }
            };
            tableScanUtilMock.when(() -> TableScanUtil.splitFiles(Mockito.any(), Mockito.anyLong()))
                    .thenReturn(iterable);

            // current implementation swallows close errors; just assert no exception bubbles up
            Assertions.assertDoesNotThrow(() -> collectMethod.invoke(metadata, key, table, null, null));
        }
    }

    @Test
    public void testDeltaLakeMetadataIteratorThrowsIOException() throws Exception {
        DeltaMetastoreOperations deltaOps = Mockito.mock(DeltaMetastoreOperations.class);
        DeltaLakeMetadata metadata = new DeltaLakeMetadata(
                new HdfsEnvironment(new java.util.HashMap<>()), "delta0", deltaOps, null,
                new ConnectorProperties(ConnectorType.DELTALAKE));

        DeltaLakeTable table = Mockito.mock(DeltaLakeTable.class);
        Engine engine = Mockito.mock(Engine.class);
        SnapshotImpl snapshot = Mockito.mock(SnapshotImpl.class);
        Metadata meta = Mockito.mock(Metadata.class);
        StructType schema = Mockito.mock(StructType.class);
        StructField structField = Mockito.mock(StructField.class);
        Mockito.when(structField.getDataType()).thenReturn(Mockito.mock(io.delta.kernel.types.DataType.class));
        Mockito.when(schema.fieldNames()).thenReturn(List.of("c1"));
        Mockito.when(schema.get("c1")).thenReturn(structField);

        Mockito.when(table.getDeltaMetadata()).thenReturn(meta);
        Mockito.when(table.getDeltaEngine()).thenReturn(engine);
        Mockito.when(table.getDeltaSnapshot()).thenReturn(snapshot);
        Mockito.when(table.getColumns()).thenReturn(List.of(new Column("c1", Type.INT)));
        Mockito.when(table.getCatalogDBName()).thenReturn("db");
        Mockito.when(table.getCatalogTableName()).thenReturn("tbl");
        Mockito.when(meta.getSchema()).thenReturn(schema);
        Mockito.when(meta.getPartitionColNames()).thenReturn(Set.of());
        Mockito.when(snapshot.getVersion(engine)).thenReturn(1L);

        ScanBuilderImpl scanBuilder = Mockito.mock(ScanBuilderImpl.class);
        ScanImpl scan = Mockito.mock(ScanImpl.class);
        Mockito.when(snapshot.getScanBuilder(engine)).thenReturn(scanBuilder);
        Mockito.when(scanBuilder.withFilter(Mockito.eq(engine), Mockito.any())).thenReturn(scanBuilder);
        Mockito.when(scanBuilder.build()).thenReturn(scan);

        CloseableIterator<FilteredColumnarBatch> emptyIter = new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public FilteredColumnarBatch next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() throws IOException {
                throw new IOException("boom");
            }
        };
        Mockito.when(scan.getScanFiles(engine, true)).thenReturn(emptyIter);

        Method collectMethod = DeltaLakeMetadata.class.getDeclaredMethod(
                "collectDeltaLakePlanFiles", PredicateSearchKey.class, com.starrocks.catalog.Table.class,
                com.starrocks.sql.optimizer.operator.scalar.ScalarOperator.class,
                com.starrocks.qe.ConnectContext.class, List.class);
        collectMethod.setAccessible(true);
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setPredicate(ConstantOperator.TRUE)
                .setTableVersionRange(TableVersionRange.withEnd(Optional.of(1L)))
                .build();
        PredicateSearchKey key = PredicateSearchKey.of("db", "tbl", params);

        java.lang.reflect.InvocationTargetException ex = Assertions.assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> collectMethod.invoke(metadata, key, table, ConstantOperator.TRUE, null, List.of("c1")));
        Assertions.assertInstanceOf(StarRocksConnectorException.class, ex.getCause());
        Assertions.assertTrue(ex.getCause().getMessage().contains("Failed to get delta lake scan files"));
    }
}
