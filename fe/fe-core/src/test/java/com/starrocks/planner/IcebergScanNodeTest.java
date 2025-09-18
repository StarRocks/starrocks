package com.starrocks.planner;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.BucketProperty;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.ConnectorTblMetaInfoMgr;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergApiConverter;
import com.starrocks.connector.iceberg.IcebergConnectorScanRangeSource;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.connector.iceberg.IcebergMetadata.Append;
import com.starrocks.connector.iceberg.IcebergMetadata.BatchWrite;
import com.starrocks.connector.iceberg.IcebergMetadata.DynamicOverwrite;
import com.starrocks.connector.iceberg.IcebergMetadata.IcebergSinkExtra;
import com.starrocks.connector.iceberg.IcebergMetadata.RewriteData;
import com.starrocks.connector.iceberg.IcebergRemoteFileInfo;
import com.starrocks.connector.iceberg.IcebergRewriteData;
import com.starrocks.connector.iceberg.IcebergRewriteDataJob;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.TemporaryTableMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.IcebergRewriteStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TBucketFunction;
import com.starrocks.thrift.TIcebergTable;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.thrift.TTableDescriptor;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class IcebergScanNodeTest {
    class TestableIcebergConnectorScanRangeSource extends IcebergConnectorScanRangeSource {
        public TestableIcebergConnectorScanRangeSource(IcebergConnectorScanRangeSource original) {
            super(
                    Deencapsulation.getField(original, "table"),
                    Deencapsulation.getField(original, "remoteFileInfoSource"),
                    Deencapsulation.getField(original, "morParams"),
                    Deencapsulation.getField(original, "desc"),
                    Deencapsulation.getField(original, "bucketProperties"),
                    Deencapsulation.getField(original, "partitionIdGenerator"),
                    Deencapsulation.getField(original, "recordScanFiles")
            );
        }

        @Override
        public List<TScanRangeLocations> toScanRanges(FileScanTask fileScanTask) {
            return Collections.singletonList(new TScanRangeLocations());
        }

        @Override
        public List<TScanRangeLocations> getSourceOutputs(int maxSize) {
            return super.getSourceOutputs(maxSize);
        }
    }

    @Test
    public void testInit(@Mocked GlobalStateMgr globalStateMgr,
                         @Mocked CatalogConnector connector,
                         @Mocked IcebergTable table,
                         @Mocked IcebergTableMORParams tableMORParams,
                         @Mocked FileScanTask fileScanTask,
                         @Mocked DataFile mockDataFile,
                         @Mocked DeleteFile mockPosDelFile,
                         @Mocked DeleteFile mockEqDelFile,
                         @Mocked BucketProperty mockBucketProps) {

        String catalog = "XXX";
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(new HashMap<>());
        List<DeleteFile> delFiles = new ArrayList<>();
        delFiles.add(mockPosDelFile);
        delFiles.add(mockEqDelFile);
        new Expectations() {{
            GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalog);
            result = connector; minTimes = 0;
            connector.getMetadata().getCloudConfiguration();
            result = cc; minTimes = 0;
            table.getCatalogName(); result = catalog; minTimes = 0;
            table.getCatalogDBName(); result = "db1"; minTimes = 0;
            table.getCatalogTableName(); result = "tbl1"; minTimes = 0;
            // mockPosDelFile.content(); result = FileContent.POSITION_DELETES; minTimes = 0;
            // mockEqDelFile.content(); result = FileContent.EQUALITY_DELETES; minTimes = 0;
            // fileScanTask.deletes(); result = delFiles; minTimes = 0;
            fileScanTask.file(); result = mockDataFile; minTimes = 0;
            mockDataFile.fileSizeInBytes(); result = 10000000L; minTimes = 0;
            mockDataFile.specId(); result = 1; minTimes = 0;
            mockDataFile.partition(); result = null; minTimes = 0;
        }};

        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);

        IcebergScanNode scanNode = new IcebergScanNode(
                new PlanNodeId(0), desc, catalog,
                tableMORParams, IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE, null);
        scanNode.setTvrVersionRange(TvrTableSnapshot.of(Optional.of(12345L)));

        IcebergRemoteFileInfo remoteFileInfo = new IcebergRemoteFileInfo(fileScanTask);
        List<RemoteFileInfo> remoteFileInfos = List.of(remoteFileInfo);

        try {
            scanNode.rebuildScanRange(remoteFileInfos);
        } catch (Exception e) {
            Assertions.fail("Rebuild scan range should not throw exception: " + e.getMessage());
        }
        TestableIcebergConnectorScanRangeSource testSource =
                new TestableIcebergConnectorScanRangeSource(scanNode.getSourceRange());
        Deencapsulation.setField(scanNode, "scanRangeSource", testSource);
        List<TScanRangeLocations> res = scanNode.getScanRangeLocations(10);
        System.out.println("fileScanTask.deletes() size: " + fileScanTask.deletes().size());
        Assertions.assertEquals(1, res.size(), "1 scan task");
        Assertions.assertEquals(1, scanNode.getScannedDataFiles().size(), "1 DataFile");
        Assertions.assertEquals(0, scanNode.getPosAppliedDeleteFiles().size());
        Assertions.assertEquals(0, scanNode.getEqualAppliedDeleteFiles().size());
        List<RemoteFileInfo> empty = new ArrayList<>();
        try {
            scanNode.rebuildScanRange(empty);
            scanNode.setTvrVersionRange(TvrTableSnapshot.empty());
            scanNode.rebuildScanRange(remoteFileInfos);
            testSource.clearScannedFiles();
        } catch (Exception e) {
            Assertions.fail("Rebuild scan range should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testGetSourceOutputs_recorScanFilesPath(@Mocked RemoteFileInfoSource mockSource,
                                                        @Mocked FileScanTask mockScanTask,
                                                        @Mocked DataFile mockDataFile,
                                                        @Mocked DeleteFile mockPosDelFile,
                                                        @Mocked DeleteFile mockEqDelFile) {

        List<DeleteFile> deleteFiles = Arrays.asList(mockPosDelFile, mockEqDelFile);

        IcebergRemoteFileInfo mockIcebergRemoteFileInfo = new IcebergRemoteFileInfo(mockScanTask);

        new Expectations() {{
            mockSource.getOutput(); result = mockIcebergRemoteFileInfo; minTimes = 0;
            mockScanTask.file(); result = mockDataFile; minTimes = 0;
            mockScanTask.deletes(); result = deleteFiles; minTimes = 0;
            mockPosDelFile.content(); result = FileContent.POSITION_DELETES; minTimes = 0;
            mockEqDelFile.content(); result = FileContent.EQUALITY_DELETES; minTimes = 0;
        }};

        IcebergConnectorScanRangeSource scanSource = new IcebergConnectorScanRangeSource(
                null, mockSource, null, null, Optional.empty(),
                PartitionIdGenerator.of(), true  // recordScanFiles = true
        ) {
            private int callCount = 0;

            @Override
            public boolean sourceHasMoreOutput() {
                return callCount++ == 0;  
            }

            @Override
            public List<TScanRangeLocations> toScanRanges(FileScanTask fileScanTask) {
                return Collections.singletonList(new TScanRangeLocations());
            }
        };

        List<TScanRangeLocations> result = scanSource.getSourceOutputs(10);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, scanSource.getScannedDataFiles().size(), "should have 1 scanned data file");
        Assertions.assertEquals(1, scanSource.getPosAppliedDeleteFiles().size(), "should have 1 pos delete");
        Assertions.assertEquals(1, scanSource.getEqualAppliedDeleteFiles().size(), "should have 1 eq delete");
    }


    @Test
    public void testBuildAndIterate(
            @Mocked IcebergConnectorScanRangeSource mockSource,
            @Mocked FileScanTask mockTask,
            @Mocked DataFile mockDataFile,
            @Mocked PartitionSpec mockSpec) {

        new Expectations() {{
            mockSource.getSourceFileScanOutputs(anyInt, anyLong, anyBoolean);
            result = Collections.singletonList(mockTask);
            minTimes = 0;
            result = Collections.emptyList();
            minTimes = 0;

            mockTask.spec(); 
            result = mockSpec;
            minTimes = 0;
            mockSpec.partitionType(); 
            result = Types.StructType.of();
            minTimes = 0;
            mockTask.file(); 
            result = mockDataFile;
            minTimes = 0;
            mockDataFile.partition(); 
            result = null;
            minTimes = 0;
            mockDataFile.fileSizeInBytes(); 
            result = 1024L;
            minTimes = 0;
        }};

        IcebergRewriteData rewriteData = new IcebergRewriteData();
        rewriteData.setSource(mockSource);
        rewriteData.setBatchSize(10 * 1024);
        rewriteData.setMaxScanRangeLength(5);

        rewriteData.buildNewScanNodeRange(10512L, false);

        Assertions.assertTrue(rewriteData.hasMoreTaskGroup(), "task group should exist");
        List<RemoteFileInfo> group = rewriteData.nextTaskGroup();
        Assertions.assertEquals(1, group.size(), "task group should have 1 task");
        Assertions.assertFalse(rewriteData.hasMoreTaskGroup(), "no task group left");
    }

    @Test
    public void testFromStringMatching() {
        Assertions.assertEquals(IcebergTableOperation.EXPIRE_SNAPSHOTS,
                IcebergTableOperation.fromString("EXPIRE_SNAPSHOTS"));
        Assertions.assertEquals(IcebergTableOperation.FAST_FORWARD,
                IcebergTableOperation.fromString("fast_forward"));
        Assertions.assertEquals(IcebergTableOperation.CHERRYPICK_SNAPSHOT,
                IcebergTableOperation.fromString("Cherrypick_Snapshot"));
        Assertions.assertEquals(IcebergTableOperation.REMOVE_ORPHAN_FILES,
                IcebergTableOperation.fromString("remove_orphan_files"));
        Assertions.assertEquals(IcebergTableOperation.ROLLBACK_TO_SNAPSHOT,
                IcebergTableOperation.fromString("rollback_to_snapshot"));
        Assertions.assertEquals(IcebergTableOperation.REWRITE_DATA_FILES,
                IcebergTableOperation.fromString("rewrite_data_files"));
    }

    @Test
    public void testFromStringUnknown() {
        Assertions.assertEquals(IcebergTableOperation.UNKNOWN,
                IcebergTableOperation.fromString("non_existing_op"));
        Assertions.assertEquals(IcebergTableOperation.UNKNOWN,
                IcebergTableOperation.fromString(""));
        Assertions.assertEquals(IcebergTableOperation.UNKNOWN,
                IcebergTableOperation.fromString(null));
    }

    @Test
    public void testRewriteFileOptionMatching() {
        Assertions.assertEquals(IcebergTableOperation.RewriteFileOption.REWRITE_ALL,
                IcebergTableOperation.RewriteFileOption.fromString("rewrite_all"));
        Assertions.assertEquals(IcebergTableOperation.RewriteFileOption.MIN_FILE_SIZE_BYTES,
                IcebergTableOperation.RewriteFileOption.fromString("MIN_FILE_SIZE_BYTES"));
        Assertions.assertEquals(IcebergTableOperation.RewriteFileOption.BATCH_SIZE,
                IcebergTableOperation.RewriteFileOption.fromString("batch_size"));
    }

    @Test
    public void testRewriteFileOptionUnknown() {
        Assertions.assertEquals(IcebergTableOperation.RewriteFileOption.UNKNOWN,
                IcebergTableOperation.RewriteFileOption.fromString("no_such_option"));
        Assertions.assertEquals(IcebergTableOperation.RewriteFileOption.UNKNOWN,
                IcebergTableOperation.RewriteFileOption.fromString(""));
        Assertions.assertEquals(IcebergTableOperation.RewriteFileOption.UNKNOWN,
                IcebergTableOperation.RewriteFileOption.fromString(null));
    }

    @Test
    public void testToThriftWithPartitionExprs() throws Exception {

        Schema schema = new Schema(
                Types.NestedField.required(1, "col1", Types.StringType.get())
        );
        PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("col1", 10).build();


        BaseTable mockNativeTable = Mockito.mock(BaseTable.class);
        TableOperations mockOps = Mockito.mock(TableOperations.class);
        TableMetadata mockMetadata = Mockito.mock(TableMetadata.class);

        Mockito.when(mockMetadata.spec()).thenReturn(spec);
        Mockito.when(mockMetadata.uuid()).thenReturn("uuid");

        Mockito.when(mockOps.current()).thenReturn(mockMetadata);
        Mockito.when(mockNativeTable.schema()).thenReturn(schema);
        Mockito.when(mockNativeTable.spec()).thenReturn(spec);
        Mockito.when(mockNativeTable.location()).thenReturn("file:///tmp/test");
        Mockito.when(mockNativeTable.properties()).thenReturn(new HashMap<>());
        Mockito.when(mockNativeTable.operations()).thenReturn(mockOps);


        List<Column> schemaColumns = new ArrayList<>();
        schemaColumns.add(new Column("col1", ScalarType.createVarchar(20)));

        IcebergTable icebergTable = new IcebergTable.Builder()
                .setId(1234)
                .setSrTableName("test_tbl")
                .setCatalogName("test_catalog")
                .setCatalogDBName("test_db")
                .setCatalogTableName("test_tbl")
                .setFullSchema(schemaColumns)
                .setNativeTable(mockNativeTable)
                .setIcebergProperties(Collections.singletonMap("iceberg.catalog.type", "hive"))

                .build();

        icebergTable.setComment("some normal comment");

        Schema icebergApiSchema = IcebergApiConverter.toIcebergApiSchema(schemaColumns);
        SortOrder.Builder builder = SortOrder.builderFor(icebergApiSchema);
        builder.asc("col1", NullOrder.NULLS_FIRST);
        SortOrder sortOrder = builder.build();
        Mockito.when(mockNativeTable.sortOrder()).thenReturn(sortOrder);

        List<DescriptorTable.ReferencedPartitionInfo> partitions = new ArrayList<>();
        TTableDescriptor tdesc = icebergTable.toThrift(partitions);

        TIcebergTable tIcebergTable = tdesc.getIcebergTable();
        Assertions.assertNotNull(tIcebergTable);
        Assertions.assertEquals("file:///tmp/test", tIcebergTable.getLocation());
        Assertions.assertFalse(tIcebergTable.getPartition_info().isEmpty());
        Assertions.assertEquals("col1_trunc", tIcebergTable.getPartition_info().get(0).getPartition_column_name());
        Assertions.assertFalse(tIcebergTable.getSort_order().getSort_key_idxes().isEmpty());
    }

    private Schema _schema() {
        return new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "data", Types.StringType.get())
        );
    }

    private DataFile createFakeDataFile(org.apache.iceberg.Table table) {
        return DataFiles.builder(table.spec())
                .withPath("/tmp/fake-file.avro")
                .withFormat(FileFormat.AVRO)
                .withRecordCount(1)
                .withFileSizeInBytes(100)
                .withSplitOffsets(Collections.singletonList(0L))
                .build();
    }

    private DeleteFile createFakeDeleteFile(org.apache.iceberg.Table table) {
        return FileMetadata.deleteFileBuilder(table.spec())
                .ofEqualityDeletes(table.schema().findField("id").fieldId())
                .withPath("/tmp/fake-delete-file.avro")
                .withFormat(FileFormat.AVRO)
                .withRecordCount(1)
                .withFileSizeInBytes(50)
                .build();
    }

    @Test
    void testAppend() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", new HashMap<>());

        TableIdentifier tableIdentifier = TableIdentifier.of("test_db", "test_tbl");
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.unpartitioned();
        catalog.createNamespace(Namespace.of("test_db"));
        org.apache.iceberg.Table table = catalog.createTable(tableIdentifier, schema, spec);
        Transaction txn = table.newTransaction();

        BatchWrite writer = new Append(txn);
        AppendFiles mockRewriteFiles = Mockito.mock(AppendFiles.class);
        Mockito.doNothing().when(mockRewriteFiles).commit();
        Mockito.when(mockRewriteFiles.toBranch("dev")).thenReturn(mockRewriteFiles);
        Deencapsulation.setField(writer, "append", mockRewriteFiles);
        DataFile dataFile = createFakeDataFile(table);
        DeleteFile deleteFile = createFakeDeleteFile(table);
        writer.toBranch("dev");
        writer.addFile(dataFile);
        writer.deleteFile(dataFile);
        writer.deleteFile(deleteFile);
        writer.commit();
    }

    @Test
    void testRewriteData() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", new HashMap<>());

        TableIdentifier tableIdentifier = TableIdentifier.of("test_db", "test_tbl");
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.unpartitioned();
        catalog.createNamespace(Namespace.of("test_db"));
        org.apache.iceberg.Table table = catalog.createTable(tableIdentifier, schema, spec);
        Transaction txn = table.newTransaction();

        RewriteData writer = new RewriteData(txn);
        DataFile dataFile = createFakeDataFile(table);
        DeleteFile deleteFile = createFakeDeleteFile(table);
        RewriteFiles mockRewriteFiles = Mockito.mock(RewriteFiles.class);
        Mockito.doNothing().when(mockRewriteFiles).commit();
        Mockito.when(mockRewriteFiles.toBranch("dev")).thenReturn(mockRewriteFiles);
        Mockito.when(mockRewriteFiles.validateFromSnapshot(123L)).thenReturn(null);
        Deencapsulation.setField(writer, "rewriteFiles", mockRewriteFiles);
        writer.setSnapshotId(123L);
        writer.toBranch("dev");
        writer.addFile(dataFile);
        writer.deleteFile(deleteFile);
        writer.deleteFile(dataFile);
        writer.commit();
    }

    @Test
    void testDynamicOverwrite() {
        
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", new HashMap<>());

        TableIdentifier tableIdentifier = TableIdentifier.of("test_db", "test_tbl");
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.unpartitioned();
        catalog.createNamespace(Namespace.of("test_db"));
        org.apache.iceberg.Table table = catalog.createTable(tableIdentifier, schema, spec);
        DeleteFile deleteFile = createFakeDeleteFile(table);
        Transaction txn = table.newTransaction();
        ReplacePartitions mockRewriteFiles = Mockito.mock(ReplacePartitions.class);
        Mockito.when(mockRewriteFiles.toBranch("dev")).thenReturn(mockRewriteFiles);
        Mockito.doNothing().when(mockRewriteFiles).commit();
        BatchWrite writer = new DynamicOverwrite(txn);
        Deencapsulation.setField(writer, "replace", mockRewriteFiles);
        DataFile dataFile = createFakeDataFile(table);
        writer.toBranch("dev");
        writer.addFile(dataFile);
        writer.deleteFile(dataFile);
        writer.deleteFile(deleteFile);
        writer.commit();

    }

    @Test
    void testIcebergSinkExtra() {
        IcebergSinkExtra sinkExtra = new IcebergSinkExtra();

        // create table just to get DataFile/DeleteFile instances
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", new HashMap<>());

        TableIdentifier tableIdentifier = TableIdentifier.of("test_db", "test_tbl");
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.unpartitioned();
        catalog.createNamespace(Namespace.of("test_db"));
        org.apache.iceberg.Table table = catalog.createTable(tableIdentifier, schema, spec);
        DataFile df = createFakeDataFile(table);
        DeleteFile del = createFakeDeleteFile(table);

        sinkExtra.addScannedDataFiles(Collections.singleton(df));
        sinkExtra.addAppliedDeleteFiles(Collections.singleton(del));

        Assertions.assertTrue(sinkExtra.getScannedDataFiles().contains(df));
        Assertions.assertTrue(sinkExtra.getAppliedDeleteFiles().contains(del));
    }

    @Test
    public void testFillRewriteFilesShouldFillExtraCorrectly() throws Exception {
        // 1. Mock InsertStmt
        IcebergRewriteStmt rewriteStmt = Mockito.mock(IcebergRewriteStmt.class);
        Mockito.when(rewriteStmt.rewriteAll()).thenReturn(true);
    
        // 2. Mock IcebergScanNode
        IcebergScanNode scanNode = Mockito.mock(IcebergScanNode.class);
    
        DeleteFile pos1 = Mockito.mock(DeleteFile.class);
        DeleteFile pos2 = Mockito.mock(DeleteFile.class);
        DeleteFile eq1 = Mockito.mock(DeleteFile.class);
        DataFile data1 = Mockito.mock(DataFile.class);
        DataFile data2 = Mockito.mock(DataFile.class);
    
        Mockito.when(scanNode.getPlanNodeName()).thenReturn("IcebergScanNode");
        Mockito.when(scanNode.getPosAppliedDeleteFiles()).thenReturn(Set.of(pos1, pos2));
        Mockito.when(scanNode.getEqualAppliedDeleteFiles()).thenReturn(Set.of(eq1));
        Mockito.when(scanNode.getScannedDataFiles()).thenReturn(Set.of(data1, data2));
    
        // 3. Mock PlanFragment
        PlanFragment fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(fragment.collectScanNodes())
                .thenReturn(Map.of(new PlanNodeId(0), scanNode));
    
        // 4. Mock ExecPlan
        ExecPlan execPlan = Mockito.mock(ExecPlan.class);
        Mockito.when(execPlan.getFragments()).thenReturn(new ArrayList<>(List.of(fragment)));
    
        // 5. Prepare commitInfos
        List<TSinkCommitInfo> commitInfos = new ArrayList<>();
        TSinkCommitInfo info1 = new TSinkCommitInfo();
        TSinkCommitInfo info2 = new TSinkCommitInfo();
        commitInfos.add(info1);
        commitInfos.add(info2);
    
        // 6. Executor and extra
        StatementBase fakeStmt = Mockito.mock(StatementBase.class);
        ConnectContext ctx = new ConnectContext();
        StmtExecutor executor = new StmtExecutor(ctx, fakeStmt);
        IcebergMetadata.IcebergSinkExtra extra = new IcebergMetadata.IcebergSinkExtra();
    
        // 7. Call target method
        executor.fillRewriteFiles(rewriteStmt, execPlan, commitInfos, extra);
    
        // 8. Assert
        Assertions.assertTrue(info1.isIs_rewrite());
        Assertions.assertTrue(info2.isIs_rewrite());
    }

    @Test
    public void testGetSourceFileScanOutputsMixedScenarios() {
        // --- Mock DeleteFile ---
        DeleteFile posDeleteFile = Mockito.mock(DeleteFile.class);
        Mockito.when(posDeleteFile.content()).thenReturn(FileContent.POSITION_DELETES);
        Mockito.when(posDeleteFile.referencedDataFile()).thenReturn(null);
        Map<Integer, ByteBuffer> lower = Map.of(2147483546, ByteBuffer.wrap("a".getBytes()));
        Map<Integer, ByteBuffer> upper = Map.of(2147483546, ByteBuffer.wrap("b".getBytes()));
        Mockito.when(posDeleteFile.lowerBounds()).thenReturn(lower);
        Mockito.when(posDeleteFile.upperBounds()).thenReturn(upper);

        DeleteFile eqDeleteFile = Mockito.mock(DeleteFile.class);
        Mockito.when(eqDeleteFile.content()).thenReturn(FileContent.EQUALITY_DELETES);

        // --- Mock DataFile ---
        DataFile dataFile = Mockito.mock(DataFile.class);
        Mockito.when(dataFile.fileSizeInBytes()).thenReturn(500L);

        // --- Mock FileScanTask ---
        FileScanTask fileScanTask = Mockito.mock(FileScanTask.class);
        Mockito.when(fileScanTask.file()).thenReturn(dataFile);
        Mockito.when(fileScanTask.deletes()).thenReturn(List.of(posDeleteFile, eqDeleteFile));

        // --- Mock IcebergRemoteFileInfo ---
        IcebergRemoteFileInfo icebergRemoteFileInfo = Mockito.mock(IcebergRemoteFileInfo.class);
        Mockito.when(icebergRemoteFileInfo.getFileScanTask()).thenReturn(fileScanTask);

        // --- Mock RemoteFileInfo ---
        RemoteFileInfo remoteFileInfo = Mockito.mock(RemoteFileInfo.class);
        Mockito.when(remoteFileInfo.cast()).thenReturn(icebergRemoteFileInfo);

        // --- Mock RemoteFileInfoSource ---
        RemoteFileInfoSource remoteFileInfoSource = new RemoteFileInfoSource() {
            int count = 0;

            @Override
            public RemoteFileInfo getOutput() {
                count++;
                return remoteFileInfo;
            }

            @Override
            public boolean hasMoreOutput() {
                return count == 0;
            }
        };

        IcebergTable icebergTable = Mockito.mock(IcebergTable.class);
        IcebergMORParams morParams = Mockito.mock(IcebergMORParams.class);
        TupleDescriptor tupleDesc = Mockito.mock(TupleDescriptor.class);

        IcebergConnectorScanRangeSource source = new IcebergConnectorScanRangeSource(
                icebergTable,
                remoteFileInfoSource,
                morParams,
                tupleDesc,
                Optional.empty(),
                PartitionIdGenerator.of()
                );

        List<FileScanTask> result = source.getSourceFileScanOutputs(
                10, // maxSize
                100L, // threshold - dataFile is 500, so it's too big
                false // allFiles == false
        );

        Assertions.assertEquals(1, result.size());
        Assertions.assertSame(fileScanTask, result.get(0));
    }

    @Test
    public void testFinishSinkSuccess(
            @Mocked LocalMetastore localMetastore,
            @Mocked TemporaryTableMgr temporaryTableMgr,
            @Mocked ConnectorMgr connectorMgr,
            @Mocked ConnectorTblMetaInfoMgr tblMetaInfoMgr,
            @Mocked ConnectorMetadata connectorMetadata) {

        MetadataMgr metadataMgr = new MetadataMgr(localMetastore, temporaryTableMgr, connectorMgr, tblMetaInfoMgr);

        List<TSinkCommitInfo> sinkCommitInfos = new ArrayList<>();
        Object extra = new Object();

        new Expectations(metadataMgr) {{
            metadataMgr.getOptionalMetadata("testCatalog");
            result = Optional.of(connectorMetadata);
            minTimes = 0;

            connectorMetadata.finishSink("db", "tbl", sinkCommitInfos, "branch", extra);
        }};

        metadataMgr.finishSink("testCatalog", "db", "tbl", sinkCommitInfos, "branch", extra);
    }

    @Test
    public void testFinishSinkMetadataThrows(
            @Mocked LocalMetastore localMetastore,
            @Mocked TemporaryTableMgr temporaryTableMgr,
            @Mocked ConnectorMgr connectorMgr,
            @Mocked ConnectorTblMetaInfoMgr tblMetaInfoMgr,
            @Mocked ConnectorMetadata connectorMetadata) {

        MetadataMgr metadataMgr = new MetadataMgr(localMetastore, temporaryTableMgr, connectorMgr, tblMetaInfoMgr);

        List<TSinkCommitInfo> sinkCommitInfos = new ArrayList<>();
        Object extra = new Object();

        new Expectations(metadataMgr) {{
            metadataMgr.getOptionalMetadata("testCatalog");
            result = Optional.of(connectorMetadata);
            minTimes = 0;

            connectorMetadata.finishSink("db", "tbl", sinkCommitInfos, "branch", extra);
            result = new StarRocksConnectorException("fail!");
            minTimes = 0;
        }};

        Assertions.assertThrows(StarRocksConnectorException.class, () ->
                metadataMgr.finishSink("testCatalog", "db", "tbl", sinkCommitInfos, "branch", extra));
    }

    @Test
    public void testFinishSinkNoMetadata(
            @Mocked LocalMetastore localMetastore,
            @Mocked TemporaryTableMgr temporaryTableMgr,
            @Mocked ConnectorMgr connectorMgr,
            @Mocked ConnectorTblMetaInfoMgr tblMetaInfoMgr) {

        MetadataMgr metadataMgr = new MetadataMgr(localMetastore, temporaryTableMgr, connectorMgr, tblMetaInfoMgr);

        new Expectations(metadataMgr) {{
            metadataMgr.getOptionalMetadata("unknownCatalog");
            result = Optional.empty();
            minTimes = 0;
        }};

        metadataMgr.finishSink("unknownCatalog", "db", "tbl", new ArrayList<>(), "branch", new Object());
    }

    @Test
    void executeShouldRunNormallyWhenPreparedStateAndTasksExist() throws Exception {
        ConnectContext context = Mockito.mock(ConnectContext.class, Mockito.RETURNS_DEEP_STUBS);
        AlterTableStmt alter = Mockito.mock(AlterTableStmt.class);
        IcebergRewriteStmt rewriteStmt = Mockito.mock(IcebergRewriteStmt.class);
        ExecPlan execPlan = Mockito.mock(ExecPlan.class);
        IcebergScanNode scanNode = Mockito.mock(IcebergScanNode.class);
        IcebergRewriteData rewriteData = Mockito.mock(IcebergRewriteData.class);
        PlanFragment fragment = Mockito.mock(PlanFragment.class);
        ArrayList<PlanFragment> fragments = new ArrayList<>();
        Map<PlanNodeId, ScanNode> scanMap = new HashMap<>();
        scanMap.put(new PlanNodeId(1), scanNode);
        fragments.add(fragment);
        Mockito.when(execPlan.getFragments()).thenReturn(fragments);
        Mockito.when(fragment.collectScanNodes()).thenReturn(scanMap);
        Mockito.when(rewriteData.hasMoreTaskGroup())
                .thenReturn(true)
                .thenReturn(false);
        List<RemoteFileInfo> oneGroup = Collections.emptyList();
        Mockito.when(rewriteData.nextTaskGroup()).thenReturn(oneGroup);
        Mockito.when(scanNode.getPlanNodeName()).thenReturn("IcebergScanNode");
        RemoteFileInfo remoteFileInfo = Mockito.mock(RemoteFileInfo.class);
        // --- Mock DataFile ---
        DataFile dataFile = Mockito.mock(DataFile.class);
        Mockito.when(dataFile.fileSizeInBytes()).thenReturn(500L);

        // --- Mock FileScanTask ---
        FileScanTask fileScanTask = Mockito.mock(FileScanTask.class);
        Mockito.when(fileScanTask.file()).thenReturn(dataFile);
        Mockito.when(fileScanTask.deletes()).thenReturn(Collections.emptyList());

        // --- Mock IcebergRemoteFileInfo ---
        IcebergRemoteFileInfo icebergRemoteFileInfo = Mockito.mock(IcebergRemoteFileInfo.class);
        Mockito.when(icebergRemoteFileInfo.getFileScanTask()).thenReturn(fileScanTask);
        Mockito.when(remoteFileInfo.cast()).thenReturn(icebergRemoteFileInfo);
        RemoteFileInfoSource remoteFileInfoSource = new RemoteFileInfoSource() {
            int count = 0;

            @Override
            public RemoteFileInfo getOutput() {
                count++;
                return remoteFileInfo;
            }

            @Override
            public boolean hasMoreOutput() {
                return count == 0;
            }
        };

        IcebergTable icebergTable = Mockito.mock(IcebergTable.class);
        IcebergMORParams morParams = Mockito.mock(IcebergMORParams.class);
        TupleDescriptor tupleDesc = Mockito.mock(TupleDescriptor.class);

        IcebergConnectorScanRangeSource fakeSourceRange = new IcebergConnectorScanRangeSource(
                icebergTable,
                remoteFileInfoSource,
                morParams,
                tupleDesc,
                Optional.empty(),
                PartitionIdGenerator.of()
                );
        Mockito.when(scanNode.getSourceRange()).thenReturn(fakeSourceRange);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        new MockUp<StmtExecutor>() {
            @Mock
            public StmtExecutor newInternalExecutor(ConnectContext c, StatementBase s) {
                return executor;
            }
        };

        new MockUp<com.starrocks.sql.parser.SqlParser>() {
            @Mock
            public List<com.starrocks.sql.ast.StatementBase> parse(String sql, SessionVariable sessionVariable) {
                return Collections.singletonList(Mockito.mock(com.starrocks.sql.ast.InsertStmt.class));
            }
        };

        new MockUp<StatementPlanner>() {
            @Mock
            public ExecPlan plan(StatementBase stmt, ConnectContext session) {
                return execPlan;
            }
        };

        new MockUp<IcebergScanNode>() {
            @Mock
            public void rebuildScanRange(List<RemoteFileInfo> splits) {
                return;
            }
        };

        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) {
                return;
            }
        };
        new MockUp<IcebergRewriteData>() {
            @Mock
            public void buildNewScanNodeRange(long fileSizeThreshold, boolean allFiles) {
                return;
            }
        };

        new MockUp<IcebergRewriteStmt>() {
            @Mock
            public void $init(InsertStmt base, boolean rewriteAll) {
                //do nothing
            }
        };

        IcebergRewriteDataJob job = new IcebergRewriteDataJob(
                "insert into t select * from t", false, 0L, 10L, 1L, context, alter);

        job.prepare();
        Deencapsulation.setField(job, "execPlan", execPlan);
        Deencapsulation.setField(job, "scanNodes", Arrays.asList(scanNode));
        Deencapsulation.setField(job, "rewriteStmt", rewriteStmt);
        Deencapsulation.setField(job, "rewriteData", rewriteData);
        job.execute();

        Mockito.verify(rewriteData, Mockito.times(2)).hasMoreTaskGroup();
        Mockito.verify(rewriteData, Mockito.times(1)).nextTaskGroup();
        Mockito.verify(scanNode, Mockito.times(1)).rebuildScanRange(oneGroup);
        Mockito.verify(executor, Mockito.times(1)).handleDMLStmt(execPlan, rewriteStmt);
    }

    @Test
    void executeShouldSetErrorAndReturnWhenExecutorThrows() throws Exception {
        ConnectContext context = Mockito.mock(ConnectContext.class, Mockito.RETURNS_DEEP_STUBS);
        AlterTableStmt alter = Mockito.mock(AlterTableStmt.class);
        IcebergRewriteStmt rewriteStmt = Mockito.mock(IcebergRewriteStmt.class);
        ExecPlan execPlan = Mockito.mock(ExecPlan.class);
        IcebergScanNode scanNode = Mockito.mock(IcebergScanNode.class);
        IcebergRewriteData rewriteData = Mockito.mock(IcebergRewriteData.class);

        Mockito.when(rewriteData.hasMoreTaskGroup())
                .thenReturn(true)
                .thenReturn(false);
        Mockito.when(rewriteData.nextTaskGroup()).thenReturn(Collections.emptyList());

        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.doThrow(new RuntimeException("boom")).when(executor).handleDMLStmt(execPlan, rewriteStmt);

        new MockUp<StmtExecutor>() {
            @Mock
            public StmtExecutor newInternalExecutor(ConnectContext c, StatementBase s) {
                return executor;
            }
        };

        new MockUp<IcebergScanNode>() {
            @Mock
            public void rebuildScanRange(List<RemoteFileInfo> splits) {
                return;
            }
        };

        IcebergRewriteDataJob job = new IcebergRewriteDataJob(
                "insert into t select 1", false, 0L, 10L, 1L, context, alter);

        Deencapsulation.setField(job, "rewriteStmt", rewriteStmt);
        Deencapsulation.setField(job, "execPlan", execPlan);
        Deencapsulation.setField(job, "scanNodes", Arrays.asList(scanNode));
        Deencapsulation.setField(job, "rewriteData", rewriteData);

        job.execute();

        Mockito.verify(context.getState(), Mockito.times(1)).setError("boom");
        Mockito.verify(scanNode, Mockito.times(1)).rebuildScanRange(Mockito.anyList());
    }

    @Test
    public void testGetBucketNums(@Mocked IcebergTable table) {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);

        IcebergScanNode scanNode = new IcebergScanNode(
                new PlanNodeId(0), desc, "IcebergScanNode",
                IcebergTableMORParams.EMPTY, IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE, null);

        // Create three bucket properties
        List<BucketProperty> bucketProperties = new ArrayList<>();
        Column column1 = new Column("test_col1", ScalarType.INT);
        Column column2 = new Column("test_col2", ScalarType.INT);
        Column column3 = new Column("test_col3", ScalarType.INT);
        Column column4 = new Column("test_col4", ScalarType.INT);
        BucketProperty bucketProperty1 = new BucketProperty(TBucketFunction.MURMUR3_X86_32, 2, column1);
        BucketProperty bucketProperty2 = new BucketProperty(TBucketFunction.MURMUR3_X86_32, 3, column2);
        BucketProperty bucketProperty3 = new BucketProperty(TBucketFunction.MURMUR3_X86_32, 4, column3);
        BucketProperty bucketProperty4 = new BucketProperty(TBucketFunction.MURMUR3_X86_32, 5, column4);
        bucketProperties.add(bucketProperty1);
        bucketProperties.add(bucketProperty2);
        bucketProperties.add(bucketProperty3);
        bucketProperties.add(bucketProperty4);

        scanNode.setBucketProperties(bucketProperties);

        // Test
        int result = scanNode.getBucketNums();

        // Verify: (2 + 1) * (3 + 1) * (4 + 1) * (5 + 1) = 3 * 4 * 5 * 6 = 360
        Assertions.assertEquals(360, result);
        // wrong method
        Assertions.assertEquals(876, Stream.of(2, 3, 4, 5).reduce(1, (a, b) -> (a + 1) * (b + 1)));
    }
}
