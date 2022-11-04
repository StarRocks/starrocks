package com.starrocks.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.common.UserException;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.analysis.Analyzer;
import com.starrocks.thrift.*;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.iceberg.*;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IcebergScanNodeTest {
    private String dbName;
    private String tableName;
    private String resourceName;
    private List<Column> columns;
    private Map<String, String> properties;

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
    public void testGetScanRangeLocations(@Mocked GlobalStateMgr globalStateMgr,
                                          @Mocked com.starrocks.catalog.IcebergTable table,
                                          @Mocked Table iTable,
                                          @Mocked Snapshot snapshot,
                                          @Mocked TableScan scan) throws UserException {
        Analyzer analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), new ConnectContext());
        DescriptorTable descTable = analyzer.getDescTbl();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        tupleDesc.setTable(table);

        new MockUp<IcebergUtil>() {
            @Mock
            public TableScan getTableScan(Table table,
                                          Snapshot snapshot,
                                          List<Expression> icebergPredicates) {
                return new DataTableScan(null, iTable);
            }
        };


        new MockUp<DataTableScan>() {
            @Mock
            public CloseableIterable<CombinedScanTask> planTasks() {
                List<CombinedScanTask> tasks = new ArrayList<CombinedScanTask>();
                DataFile data = DataFiles.builder(PartitionSpec.unpartitioned())
                        .withInputFile(new LocalFileIO().newInputFile("input.orc"))
                        .withRecordCount(1)
                        .withFileSizeInBytes(1024)
                        .withFormat(FileFormat.ORC)
                        .build();

                DeleteFile delete = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                        .ofPositionDeletes()
                        .withPath("delete.orc")
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
                table.getIcebergTable();
                result = iTable;

                iTable.currentSnapshot();
                result = snapshot;
            }
        };



        IcebergScanNode scanNode = new IcebergScanNode(new PlanNodeId(0), tupleDesc, "IcebergScanNode");
        scanNode.getScanRangeLocations();

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
}
