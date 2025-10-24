package com.starrocks.planner;

import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.BucketProperty;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.ConnectorTblMetaInfoMgr;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergAlterTableExecutor;
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
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
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
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.IcebergRewriteStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.parser.NodePosition;
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
<<<<<<< HEAD
import javax.swing.text.html.Option;
import static com.starrocks.common.util.Util.executeCommand;
=======
import java.util.concurrent.ConcurrentLinkedQueue;
>>>>>>> 0a3eb5e25a ([Enhancement] add iceberg compaction batch parallel (#63959))
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;

public class IcebergScanNodeTest {
    public static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();
    class TestableIcebergConnectorScanRangeSource extends IcebergConnectorScanRangeSource {
        public TestableIcebergConnectorScanRangeSource(IcebergConnectorScanRangeSource original) {
            super(
                Deencapsulation.getField(original, "table"),
                Deencapsulation.getField(original, "remoteFileInfoSource"),
                Deencapsulation.getField(original, "morParams"),
                Deencapsulation.getField(original, "desc"),
                Deencapsulation.getField(original, "bucketProperties"),
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
                tableMORParams, IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE);
        scanNode.setSnapshotId(Optional.of(12345L));

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
            scanNode.setSnapshotId(Optional.empty());
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
                null, mockSource, null, null, Optional.empty(), true  // recordScanFiles = true
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
    public void testVisitAlterTableOperationClause_rewriteDataFiles2(@Mocked IcebergTable table) {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);

        Expr rewriteAll = new BinaryPredicate(BinaryType.EQ,
                new StringLiteral("REWRITE_ALL"), new BoolLiteral(true));
        Expr minFileSize = new BinaryPredicate(BinaryType.EQ,
                new StringLiteral("MIN_FILE_SIZE_BYTES"), new IntLiteral(100));
        Expr batchSize = new BinaryPredicate(BinaryType.EQ,
                new StringLiteral("BATCH_SIZE"), new IntLiteral(200));

        AlterTableOperationClause clause = new AlterTableOperationClause(new NodePosition(1, 1), "REWRITE_DATA_FILES",
                Arrays.asList(rewriteAll, minFileSize, batchSize), null);

        analyzer.visitAlterTableOperationClause(clause, new ConnectContext());

        Assertions.assertTrue(clause.isRewriteAll());
        Assertions.assertEquals(100, clause.getMinFileSizeBytes());
        Assertions.assertEquals(200, clause.getBatchSize());
    }

    @Test
    public void testVisitAlterTableOperationClause_invalidExpr(@Mocked IcebergTable table) {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
        Expr wrongExpr = new StringLiteral("wrong");
        AlterTableOperationClause clause = new AlterTableOperationClause(new NodePosition(1, 1), "REWRITE_DATA_FILES",
                Collections.singletonList(wrongExpr), null);

        Assertions.assertThrows(SemanticException.class,
                () -> analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
    }

    @Test
    public void testVisitAlterTableOperationClause_otherOp_addsArgs(@Mocked IcebergTable table) {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
        Expr expr = new StringLiteral("dummy");
    
        AlterTableOperationClause clause = new AlterTableOperationClause(
                new NodePosition(1, 1),
                "OTHER_OP",
                Collections.singletonList(expr),
                null
        );
    
        final ConstantOperator constOp = ConstantOperator.createInt(123);
    
        new MockUp<SqlToScalarOperatorTranslator>() {
            @Mock
            public ScalarOperator translate(Expr e, ExpressionMapping m, ColumnRefFactory f) {
                return constOp;
            }
        };
    
        analyzer.visitAlterTableOperationClause(clause, new ConnectContext());
    
        Assertions.assertFalse(clause.getArgs().isEmpty());
        Assertions.assertEquals(constOp, clause.getArgs().get(0));
    }

    @Test
    public void testVisitAlterTableOperationClause_otherOp_addsArgs2(@Mocked IcebergTable table) {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
        Expr expr = new StringLiteral("dummy");
    
        AlterTableOperationClause clause = new AlterTableOperationClause(
                new NodePosition(1, 1),
                "OTHER_OP",
                Collections.singletonList(expr),
                null
        );
    
        final ConstantOperator constOp = ConstantOperator.createInt(123);
    
        new MockUp<SqlToScalarOperatorTranslator>() {
            @Mock
            public ScalarOperator translate(Expr e, ExpressionMapping m, ColumnRefFactory f) {
                return constOp;
            }
        };
    
        analyzer.visitAlterTableOperationClause(clause, new ConnectContext());
    
        Assertions.assertFalse(clause.getArgs().isEmpty());
        Assertions.assertEquals(constOp, clause.getArgs().get(0));
    }

    @Test
    public void testVisitAlterTableOperationClause_rewriteDataFiles(@Mocked IcebergTable table) {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);

        Expr rewriteAll = new BinaryPredicate(BinaryType.EQ,
                new StringLiteral("REWRITE_ALL"), new BoolLiteral(true));
        Expr minFileSize = new BinaryPredicate(BinaryType.EQ,
                new StringLiteral("MIN_FILE_SIZE_BYTES"), new IntLiteral(100));
        Expr batchSize = new BinaryPredicate(BinaryType.EQ,
                new StringLiteral("BATCH_SIZE"), new IntLiteral(200));

        AlterTableOperationClause clause = new AlterTableOperationClause(
                new NodePosition(1, 1), "REWRITE_DATA_FILES",
                Arrays.asList(rewriteAll, minFileSize, batchSize), null);

        analyzer.visitAlterTableOperationClause(clause, new ConnectContext());

        Assertions.assertTrue(clause.isRewriteAll());
        Assertions.assertEquals(100, clause.getMinFileSizeBytes());
        Assertions.assertEquals(200, clause.getBatchSize());
    }

    @Test
    public void testVisitAlterTableOperationClause_nullTableOperationName_shouldThrow() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        AlterTableOperationClause clause = new AlterTableOperationClause(
                new NodePosition(1, 1), null, Collections.emptyList(), null);

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("Table operation name should be null"));
    }

    @Test
    public void testVisitAlterTableOperationClause_invalidExprType_shouldThrow() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        Expr expr = new BoolLiteral(true); // not a BinaryPredicate
        AlterTableOperationClause clause = new AlterTableOperationClause(
                new NodePosition(1, 1), "REWRITE_DATA_FILES",
                Collections.singletonList(expr), null);

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("Invalid"));
    }

    @Test
    public void testVisitAlterTableOperationClause_nonEqPredicate_shouldThrow() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        Expr expr = new BinaryPredicate(BinaryType.NE,
                new StringLiteral("REWRITE_ALL"), new BoolLiteral(true)); // Not EQ
        AlterTableOperationClause clause = new AlterTableOperationClause(
                new NodePosition(1, 1), "REWRITE_DATA_FILES",
                Collections.singletonList(expr), null);

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("Invalid"));
    }

    @Test
    public void testVisitAlterTableOperationClause_nonLiteralArgs_shouldThrow() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        Expr expr = new BinaryPredicate(BinaryType.EQ,
                new BinaryPredicate(BinaryType.EQ, new StringLiteral("x"), new StringLiteral("y")),
                new BoolLiteral(true)); // left child not a LiteralExpr
        AlterTableOperationClause clause = new AlterTableOperationClause(
                new NodePosition(1, 1), "REWRITE_DATA_FILES",
                Collections.singletonList(expr), null);

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("Invalid"));
    }

    @Test
    public void testVisitAlterTableOperationClause_nonStringKey_shouldThrow() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        Expr expr = new BinaryPredicate(BinaryType.EQ,
                new IntLiteral(1), new BoolLiteral(true)); // key is not StringLiteral
        AlterTableOperationClause clause = new AlterTableOperationClause(
                new NodePosition(1, 1), "REWRITE_DATA_FILES",
                Collections.singletonList(expr), null);

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("Invalid"));
    }

    @Test
    public void testVisitAlterTableOperationClause_rewriteAllInvalidType_shouldThrow() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        Expr expr = new BinaryPredicate(BinaryType.EQ,
                new StringLiteral("REWRITE_ALL"), new IntLiteral(1)); // value not BoolLiteral
        AlterTableOperationClause clause = new AlterTableOperationClause(
                new NodePosition(1, 1), "REWRITE_DATA_FILES",
                Collections.singletonList(expr), null);

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("boolean value"));
    }

    @Test
    public void testVisitAlterTableOperationClause_minFileSizeNegative_shouldThrow() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        Expr expr = new BinaryPredicate(BinaryType.EQ,
                new StringLiteral("MIN_FILE_SIZE_BYTES"), new IntLiteral(-1));
        AlterTableOperationClause clause = new AlterTableOperationClause(
                new NodePosition(1, 1), "REWRITE_DATA_FILES",
                Collections.singletonList(expr), null);

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("non-negative integer"));
    }

    @Test
    public void testVisitAlterTableOperationClause_batchSizeInvalidType_shouldThrow() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        Expr expr = new BinaryPredicate(BinaryType.EQ,
                new StringLiteral("BATCH_SIZE"), new BoolLiteral(false)); // invalid type
        AlterTableOperationClause clause = new AlterTableOperationClause(
                new NodePosition(1, 1), "REWRITE_DATA_FILES",
                Collections.singletonList(expr), null);

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("integer value"));
    }

    @Test
    public void testVisitAlterTableOperationClause_unknownKey_shouldThrow() {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        Expr expr = new BinaryPredicate(BinaryType.EQ,
                new StringLiteral("UNKNOWN_KEY"), new IntLiteral(1));
        AlterTableOperationClause clause = new AlterTableOperationClause(
                new NodePosition(1, 1), "REWRITE_DATA_FILES",
                Collections.singletonList(expr), null);

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("Unknown key"));
    }

    @Test
    public void testVisitAlterTableOperationClause_whereClauseButNotIceberg_shouldThrow(@Mocked com.starrocks.catalog.Table table) {
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
        Expr where = new BoolLiteral(true);
        AlterTableOperationClause clause = new AlterTableOperationClause(
                new NodePosition(1, 1), "REWRITE_DATA_FILES",
                Collections.emptyList(), where);

        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                analyzer.visitAlterTableOperationClause(clause, new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("not an iceberg table"));
    }

    @Test
    public void testToThrift_withPartitionExprs() throws Exception {

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
    public void testFillRewriteFiles_shouldFillExtraCorrectly() throws Exception {
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
    public void testGetSourceFileScanOutputs_mixedScenarios() {
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
                Optional.empty()
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
    public void testFinishSink_success(
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
    public void testFinishSink_metadataThrows(
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
    public void testFinishSink_noMetadata(
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
    void execute_shouldRunNormally_whenPreparedStateAndTasksExist() throws Exception {
        ConnectContext context = Mockito.mock(ConnectContext.class, Mockito.RETURNS_DEEP_STUBS);
        SessionVariable sessVar = Mockito.mock(SessionVariable.class);
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
        Mockito.when(rewriteData.hasMoreTaskGroup()).thenReturn(true).thenReturn(false);
        List<RemoteFileInfo> oneGroup = new ArrayList<>();
        RemoteFileInfo rfi = Mockito.mock(RemoteFileInfo.class);
        oneGroup.add(rfi);
        Mockito.when(rewriteData.nextTaskGroup()).thenReturn(oneGroup);
        Mockito.when(scanNode.getPlanNodeName()).thenReturn("IcebergScanNode");
        Mockito.when(context.getSessionVariable()).thenReturn(sessVar);
        Mockito.when(sessVar.clone()).thenReturn(sessVar);
        Mockito.doNothing().when(sessVar).setQueryTimeoutS(Mockito.anyInt());
    
        // --- Mock DataFile ---
        DataFile dataFile = Mockito.mock(DataFile.class);
        Mockito.when(dataFile.fileSizeInBytes()).thenReturn(500L);

        // --- Mock FileScanTask ---
        FileScanTask fileScanTask = Mockito.mock(FileScanTask.class);
        Mockito.when(fileScanTask.file()).thenReturn(dataFile);
        Mockito.when(fileScanTask.deletes()).thenReturn(Collections.emptyList());

        // --- Mock IcebergRemoteFileInfo ---
        RemoteFileInfo remoteFileInfo = Mockito.mock(RemoteFileInfo.class);
        IcebergRemoteFileInfo icebergRemoteFileInfo = Mockito.mock(IcebergRemoteFileInfo.class);
        Mockito.when(icebergRemoteFileInfo.getFileScanTask()).thenReturn(fileScanTask);
        Mockito.when(remoteFileInfo.cast()).thenReturn(icebergRemoteFileInfo);
        RemoteFileInfoSource remoteFileInfoSource = new RemoteFileInfoSource() {
            int count = 0;
            @Override public RemoteFileInfo getOutput() { count++; return remoteFileInfo; }
            @Override public boolean hasMoreOutput() { return count == 0; }
        };
    
        IcebergTable icebergTable = Mockito.mock(IcebergTable.class);
        IcebergMORParams morParams = Mockito.mock(IcebergMORParams.class);
        TupleDescriptor tupleDesc = Mockito.mock(TupleDescriptor.class);

        IcebergConnectorScanRangeSource fakeSourceRange = new IcebergConnectorScanRangeSource(
                icebergTable,
                remoteFileInfoSource,
                morParams,
                tupleDesc,
                Optional.empty()
        );
        Mockito.when(scanNode.getSourceRange()).thenReturn(fakeSourceRange);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        new MockUp<StmtExecutor>() {
            private static StmtExecutor HOLDER;
            { HOLDER = executor; }
            @Mock
            public static StmtExecutor newInternalExecutor(ConnectContext c, StatementBase s) {
                return HOLDER;
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
            public void rebuildScanRange(List<RemoteFileInfo> splits) { /* no-op */ }
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
        
        ConcurrentLinkedQueue<IcebergRewriteDataJob.FinishArgs> finishArg = new ConcurrentLinkedQueue<>();
        finishArg.add(job.new FinishArgs(
                "c", "db", "t",
                Arrays.asList(new TSinkCommitInfo(), new TSinkCommitInfo()),
                "main",
                new IcebergSinkExtra()
        ));
        Deencapsulation.setField(job, "collected", finishArg);
        // ---- run ----
        job.execute();

        Mockito.verify(rewriteData, Mockito.times(2)).hasMoreTaskGroup();
        Mockito.verify(rewriteData, Mockito.times(1)).nextTaskGroup();
        Mockito.verify(scanNode, Mockito.times(1)).rebuildScanRange(oneGroup);
    
        Mockito.verify(executor, Mockito.times(1))
               .handleDMLStmt(eq(execPlan), isA(IcebergRewriteStmt.class));
    }

    @Test
    void execute_shouldSetErrorAndReturn_whenExecutorThrows() throws Exception {
        ConnectContext context = Mockito.mock(ConnectContext.class, Mockito.RETURNS_DEEP_STUBS);
        SessionVariable sessVar = Mockito.mock(SessionVariable.class);
        AlterTableStmt alter = Mockito.mock(AlterTableStmt.class);
        IcebergRewriteStmt rewriteStmt = Mockito.mock(IcebergRewriteStmt.class);
        ExecPlan execPlan = Mockito.mock(ExecPlan.class);
        IcebergScanNode scanNode = Mockito.mock(IcebergScanNode.class);
        IcebergRewriteData rewriteData = Mockito.mock(IcebergRewriteData.class);
        List<RemoteFileInfo> oneGroup = new ArrayList<>();
        RemoteFileInfo rfi = Mockito.mock(RemoteFileInfo.class);
        oneGroup.add(rfi);
    
        Mockito.when(rewriteData.hasMoreTaskGroup()).thenReturn(true).thenReturn(false);
        Mockito.when(rewriteData.nextTaskGroup()).thenReturn(oneGroup);
        Mockito.when(scanNode.getPlanNodeName()).thenReturn("IcebergScanNode");
    
        Mockito.when(context.getQueryId()).thenReturn(java.util.UUID.randomUUID());
        Mockito.when(context.getSkipFinishSink()).thenReturn(true);
        Mockito.when(context.getSessionVariable()).thenReturn(sessVar);
        Mockito.when(sessVar.clone()).thenReturn(sessVar);
        Mockito.doNothing().when(sessVar).setQueryTimeoutS(Mockito.anyInt());

    
        PlanFragment fragment = Mockito.mock(PlanFragment.class);
        ArrayList<PlanFragment> fragments = new ArrayList<>();
        fragments.add(fragment);

        Map<PlanNodeId, ScanNode> scanMap = new HashMap<>();
        scanMap.put(new PlanNodeId(1), scanNode);
        Mockito.when(execPlan.getFragments()).thenReturn(fragments);
        Mockito.when(fragment.collectScanNodes()).thenReturn(scanMap);
    
        com.starrocks.sql.ast.InsertStmt parsedInsert = Mockito.mock(com.starrocks.sql.ast.InsertStmt.class);
    
        new mockit.MockUp<StatementPlanner>() {
            @mockit.Mock
            public ExecPlan plan(StatementBase stmt, ConnectContext session) { return execPlan; }
        };
        new mockit.MockUp<IcebergRewriteStmt>() {
            @mockit.Mock
            public void $init(InsertStmt base, boolean rewriteAll) { /* no-op */ }
        };
        new mockit.MockUp<IcebergScanNode>() {
            @mockit.Mock
            public void rebuildScanRange(List<RemoteFileInfo> splits) { /* no-op */ }
        };
    
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.doThrow(new RuntimeException("boom"))
               .when(executor)
               .handleDMLStmt(Mockito.eq(execPlan), Mockito.isA(IcebergRewriteStmt.class));
    
        new mockit.MockUp<StmtExecutor>() {
            private static StmtExecutor HOLDER;
            { HOLDER = executor; }     
            @mockit.Mock
            public static StmtExecutor newInternalExecutor(ConnectContext c, StatementBase s) { return HOLDER; }
        };

        IcebergRewriteDataJob job = new IcebergRewriteDataJob(
                "insert into t select 1", false, 0L, 10L, 1L, context, alter);

        Deencapsulation.setField(job, "rewriteStmt", rewriteStmt);
        Deencapsulation.setField(job, "parsedStmt", parsedInsert);
        Deencapsulation.setField(job, "execPlan", execPlan);
        Deencapsulation.setField(job, "scanNodes", Arrays.asList(scanNode));
        Deencapsulation.setField(job, "rewriteData", rewriteData);

        StarRocksConnectorException ex = Assertions.assertThrows(StarRocksConnectorException.class, () -> job.execute());

        Assertions.assertEquals("Failed to compact files", ex.getMessage());
        Mockito.verify(scanNode, Mockito.times(1)).rebuildScanRange(Mockito.anyList());
    }

    @Test
    void rewriteDataFiles_shouldBuildSQL_andRunJob(@Mocked org.apache.iceberg.Table table,
            @Mocked IcebergHiveCatalog icebergHiveCatalog) throws Exception {
        // --- arrange
        String catalog = "c1";
        String db = "db";
        String tbl = "table";

        AlterTableStmt stmt = Mockito.mock(AlterTableStmt.class);
        Mockito.when(stmt.getCatalogName()).thenReturn(catalog);
        Mockito.when(stmt.getDbName()).thenReturn(db);
        Mockito.when(stmt.getTableName()).thenReturn(tbl);

        AlterTableOperationClause clause = Mockito.mock(AlterTableOperationClause.class);
        Mockito.when(clause.isRewriteAll()).thenReturn(true);
        Mockito.when(clause.getMinFileSizeBytes()).thenReturn(128L);
        Mockito.when(clause.getBatchSize()).thenReturn(10L);

        Expr where = Mockito.mock(Expr.class);
        SlotRef slot = Mockito.mock(SlotRef.class);
        Mockito.doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            List<SlotRef> list = (List<SlotRef>) inv.getArgument(1);
            list.add(slot);
            return null;
        }).when(where).collect(Mockito.eq(SlotRef.class), Mockito.anyList());
        Mockito.when(where.toSql()).thenReturn("k1 = 1");
        Mockito.when(clause.getWhere()).thenReturn(where);

        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        new MockUp<IcebergRewriteDataJob>() {
            @Mock public void prepare() {}
            @Mock public void execute() {}
        };


        IcebergAlterTableExecutor target = new IcebergAlterTableExecutor(new AlterTableStmt(
                new TableName("db", "table"),
                List.of(clause)),
                table, icebergHiveCatalog,
                ctx,
                HDFS_ENVIRONMENT);

        // --- act
        target.rewriteDataFiles(clause, ctx);
    }

    @Test
    void rewriteDataFiles_shouldWrapAndThrow_whenJobExecuteFails(@Mocked org.apache.iceberg.Table table,
            @Mocked IcebergHiveCatalog icebergHiveCatalog) throws Exception {
        // --- arrange
        String catalog = "c1";
        String db = "db";
        String tbl = "table";

        AlterTableStmt stmt = Mockito.mock(AlterTableStmt.class);
        Mockito.when(stmt.getCatalogName()).thenReturn(catalog);
        Mockito.when(stmt.getDbName()).thenReturn(db);
        Mockito.when(stmt.getTableName()).thenReturn(tbl);

        AlterTableOperationClause clause = Mockito.mock(AlterTableOperationClause.class);
        Mockito.when(clause.isRewriteAll()).thenReturn(true);
        Mockito.when(clause.getMinFileSizeBytes()).thenReturn(128L);
        Mockito.when(clause.getBatchSize()).thenReturn(10L);

        Expr where = Mockito.mock(Expr.class);
        SlotRef slot = Mockito.mock(SlotRef.class);
        Mockito.doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            List<SlotRef> list = (List<SlotRef>) inv.getArgument(1);
            list.add(slot);
            return null;
        }).when(where).collect(Mockito.eq(SlotRef.class), Mockito.anyList());
        Mockito.when(where.toSql()).thenReturn("k1 = 1");
        Mockito.when(clause.getWhere()).thenReturn(where);

        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        new MockUp<IcebergRewriteDataJob>() {
            @Mock public void prepare() {}
            @Mock public void execute() { throw new RuntimeException("boom"); }
        };


        IcebergAlterTableExecutor target = new IcebergAlterTableExecutor(new AlterTableStmt(
                new TableName("db", "table"),
                List.of(clause)),
                table, icebergHiveCatalog,
                ctx,
                HDFS_ENVIRONMENT);

        // --- act + assert
        StarRocksConnectorException ex =
                Assertions.assertThrows(StarRocksConnectorException.class, () -> target.rewriteDataFiles(clause, ctx));
        Assertions.assertTrue(ex.getMessage().contains("db.table"));
        Assertions.assertTrue(ex.getMessage().contains("boom"));
    }

    @Test
    public void testGetBucketNums(@Mocked IcebergTable table) {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);

        IcebergScanNode scanNode = new IcebergScanNode(
                new PlanNodeId(0), desc, "IcebergScanNode",
                IcebergTableMORParams.EMPTY, IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE);

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
