package com.starrocks.planner;

import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.BucketProperty;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.connector.iceberg.IcebergConnectorScanRangeSource;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergRemoteFileInfo;
import com.starrocks.connector.iceberg.IcebergRewriteData;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TScanRangeLocations;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class IcebergScanNodeTest {

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
                         @Mocked BucketProperty mockBucketProps) {

        String catalog = "XXX";
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(new HashMap<>());
        // new MockUp<IcebergConnectorScanRangeSource>() {
        //     @Mock
        //     long addPartition(FileScanTask fileScanTask) {
        //         return 123L;
        //     }
        // };
        new Expectations() {{
            GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalog);
            result = connector; minTimes = 0;
            connector.getMetadata().getCloudConfiguration();
            result = cc; minTimes = 0;
            table.getCatalogName(); result = catalog; minTimes = 0;
            table.getCatalogDBName(); result = "db1"; minTimes = 0;
            table.getCatalogTableName(); result = "tbl1"; minTimes = 0;

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

        Assertions.assertEquals(1, res.size(), "1 scan task");
        Assertions.assertEquals(1, scanNode.getScannedDataFiles().size(), "1 DataFile");
        Assertions.assertEquals(0, scanNode.getPosAppliedDeleteFiles().size());
        Assertions.assertEquals(0, scanNode.getEqualAppliedDeleteFiles().size());
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
            result = Collections.emptyList();

            mockTask.spec(); 
            result = mockSpec;
            mockSpec.partitionType(); 
            result = Types.StructType.of();
            mockTask.file(); 
            result = mockDataFile;
            mockDataFile.partition(); 
            result = null;
            mockDataFile.fileSizeInBytes(); 
            result = 1024L;
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
}