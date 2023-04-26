package com.starrocks.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TIcebergDeleteFile;
import com.starrocks.thrift.TIcebergFileContent;
import com.starrocks.thrift.TScanRange;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataTableScan;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergScanNodeTest {
    private String dbName;
    private String tableName;
    private String resourceName;
    private List<Column> columns;
    private Map<String, String> properties;
    StarRocksAssert starRocksAssert = new StarRocksAssert();

    public IcebergScanNodeTest() throws IOException {
    }

    @Before
    public void before() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        String createCatalog = "CREATE EXTERNAL CATALOG iceberg_catalog PROPERTIES(\"type\"=\"iceberg\", " +
                "\"iceberg.catalog.hive.metastore.uris\"=\"thrift://127.0.0.1:9083\", \"iceberg.catalog.type\"=\"hive\")";
        starRocksAssert.withCatalog(createCatalog);
    }

    @After
    public void after() throws Exception {
        starRocksAssert.dropCatalog("iceberg_catalog");
    }

    static class LocalFileIO implements FileIO {

        @Override
        public InputFile newInputFile(String path) {
            return Files.localInput(path);
        }

        @Override
        public OutputFile newOutputFile(String path) {
            return Files.localOutput(path);
        }

        @Override
        public void deleteFile(String path) {
            if (!new File(path).delete()) {
                throw new RuntimeIOException("Failed to delete file: " + path);
            }
        }
    }

    class TestFileScanTask implements FileScanTask {
        private DataFile data;

        private DeleteFile[] deletes;

        public TestFileScanTask(DataFile data, DeleteFile[] deletes) {
            this.data = data;
            this.deletes = deletes;
        }
        @Override
        public DataFile file() {
            return data;
        }

        @Override
        public List<DeleteFile> deletes() {
            return ImmutableList.copyOf(deletes);
        }

        @Override
        public PartitionSpec spec() {
            return PartitionSpec.unpartitioned();
        }

        @Override
        public long start() {
            return 0;
        }

        @Override
        public long length() {
            return 0;
        }

        @Override
        public Expression residual() {
            return null;
        }

        @Override
        public Iterable<FileScanTask> split(long l) {
            return null;
        }
    }

    private void setUpMock(boolean isPosDelete, com.starrocks.catalog.IcebergTable table,
                           Table iTable, Snapshot snapshot, DataTableScan dataTableScan) {
        new MockUp<DataTableScan>() {
            @Mock
            public DataTableScan useSnapshot(long var1) {
                return new DataTableScan(null, iTable);
            }

            @Mock
            public CloseableIterable<CombinedScanTask> planTasks() {
                List<CombinedScanTask> tasks = new ArrayList<CombinedScanTask>();
                DataFile data = DataFiles.builder(PartitionSpec.unpartitioned())
                        .withInputFile(new LocalFileIO().newInputFile("input.orc"))
                        .withRecordCount(1)
                        .withFileSizeInBytes(1024)
                        .withFormat(FileFormat.ORC)
                        .build();

                FileMetadata.Builder builder;
                if (isPosDelete) {
                    builder = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                            .ofPositionDeletes();
                } else {
                    builder = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                            .ofEqualityDeletes(1);
                }
                DeleteFile delete = builder.withPath("delete.orc")
                            .withFileSizeInBytes(1024)
                            .withRecordCount(1)
                            .withFormat(FileFormat.ORC)
                            .build();


                FileScanTask scanTask = new TestFileScanTask(data, new DeleteFile[]{delete});
                tasks.add(new BaseCombinedScanTask(scanTask));

                return CloseableIterable.withNoopClose(tasks);
            }
        };

        new Expectations() {
            {
                table.getCatalogName();
                result = "iceberg_catalog";

                table.getNativeTable();
                result = iTable;

                iTable.currentSnapshot();
                result = snapshot;

                iTable.newScan().useSnapshot(0);
                result = dataTableScan;
            }
        };
    }

    @Before
    public void setUp() {
        dbName = "db";
        tableName = "table";
        resourceName = "iceberg";
        columns = Lists.newArrayList();
        Column column = new Column("col1", Type.BIGINT, true);
        columns.add(column);

        properties = Maps.newHashMap();
        properties.put("database", dbName);
        properties.put("table", tableName);
        properties.put("resource", resourceName);
    }

    @Test
    public void testGetScanRangeLocations(@Mocked com.starrocks.catalog.IcebergTable table,
                                          @Mocked Table iTable,
                                          @Mocked Snapshot snapshot,
                                          @Mocked DataTableScan dataTableScan) throws Exception {
        Analyzer analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), new ConnectContext());
        DescriptorTable descTable = analyzer.getDescTbl();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        tupleDesc.setTable(table);

        setUpMock(true, table, iTable, snapshot, dataTableScan);

        IcebergScanNode scanNode = new IcebergScanNode(new PlanNodeId(0), tupleDesc, "IcebergScanNode");
        scanNode.setupScanRangeLocations();

        List<TScanRangeLocations> result = scanNode.getScanRangeLocations(1);
        Assert.assertTrue(result.size() > 0);
        TScanRange scanRange = result.get(0).scan_range;
        Assert.assertTrue(scanRange.isSetHdfs_scan_range());
        THdfsScanRange hdfsScanRange = scanRange.hdfs_scan_range;
        Assert.assertEquals("input.orc", hdfsScanRange.full_path);
        Assert.assertEquals(1, hdfsScanRange.delete_files.size());
        TIcebergDeleteFile deleteFile = hdfsScanRange.delete_files.get(0);
        Assert.assertEquals("delete.orc", deleteFile.full_path);
        Assert.assertEquals(TIcebergFileContent.POSITION_DELETES, deleteFile.file_content);
    }

    @Test
    public void testGetScanRangeLocationsWithEquality(@Mocked com.starrocks.catalog.IcebergTable table,
                                                      @Mocked Table iTable,
                                                      @Mocked Snapshot snapshot,
                                                      @Mocked DataTableScan dataTableScan) throws Exception {
        Analyzer analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), new ConnectContext());
        DescriptorTable descTable = analyzer.getDescTbl();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        tupleDesc.setTable(table);

        setUpMock(false, table, iTable, snapshot, dataTableScan);

        IcebergScanNode scanNode = new IcebergScanNode(new PlanNodeId(0), tupleDesc, "IcebergScanNode");
        scanNode.setupScanRangeLocations();

        List<TScanRangeLocations> result = scanNode.getScanRangeLocations(1);
        TScanRange scanRange = result.get(0).scan_range;
        THdfsScanRange hdfsScanRange = scanRange.hdfs_scan_range;
        TIcebergDeleteFile deleteFile = hdfsScanRange.delete_files.get(0);
        Assert.assertEquals(TIcebergFileContent.EQUALITY_DELETES, deleteFile.file_content);
    }

    @Test
    public void testAppendEqualityColumns(@Mocked IcebergTable table,
                                          @Mocked Table iTable,
                                          @Mocked Snapshot snapshot,
                                          @Mocked PhysicalIcebergScanOperator node,
                                          @Mocked ColumnRefFactory columnRefFactory,
                                          @Mocked ExecPlan context,
                                          @Mocked DataTableScan dataTableScan) throws Exception {
        Analyzer analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), new ConnectContext());
        DescriptorTable descTable = analyzer.getDescTbl();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        tupleDesc.setTable(table);

        setUpMock(false, table, iTable, snapshot, dataTableScan);

        Map<ColumnRefOperator, Column> columnMap = new HashMap<>();
        Map<ColumnRefOperator, Expr> columnRefOperatorExprMap = new HashMap<>();
        DescriptorTable descriptorTable = new DescriptorTable();
        List<Column> columns = new ArrayList<>();
        Column column = new Column();
        column.setName("test");
        columns.add(column);
        List<Types.NestedField> iColumns = new ArrayList<>();
        Types.NestedField iColumn = Types.NestedField.of(1, false, "test", new Types.IntegerType());
        iColumns.add(iColumn);
        Schema schema = new Schema(iColumns);
        new Expectations() {
            {
                node.getColRefToColumnMetaMap();
                result = columnMap;

                node.getTable();
                result = table;

                table.getFullSchema();
                result = columns;

                iTable.schema();
                result = schema;

                context.getColRefToExpr();
                result = columnRefOperatorExprMap;

                context.getDescTbl();
                result = descriptorTable;
            }
        };

        new MockUp<TupleDescriptor>() {
            @Mock
            public TableRef getRef() {
                return new TableRef();
            }
        };

        IcebergScanNode scanNode = new IcebergScanNode(new PlanNodeId(0), tupleDesc, "IcebergScanNode");
        scanNode.setupScanRangeLocations();
        scanNode.appendEqualityColumns(node, columnRefFactory, context);
        Assert.assertEquals(context.getColRefToExpr().size(), 1);
    }
}
