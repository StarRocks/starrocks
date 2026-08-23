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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.paimon.PaimonRemoteFileDesc;
import com.starrocks.connector.paimon.PaimonSplitsInfo;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.thrift.THdfsFileFormat;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.Split;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.paimon.io.DataFileMeta.DUMMY_LEVEL;
import static org.apache.paimon.io.DataFileMeta.EMPTY_MAX_KEY;
import static org.apache.paimon.io.DataFileMeta.EMPTY_MIN_KEY;
import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;

public class PaimonScanNodeTest {
    @Test
    public void testInit(@Mocked GlobalStateMgr globalStateMgr,
                         @Mocked CatalogConnector connector,
                         @Mocked PaimonTable table) {
        String catalog = "XXX";
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(new HashMap<>());
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalog);
                result = connector;
                connector.getMetadata().getCloudConfiguration();
                result = cc;
                table.getCatalogName();
                result = catalog;
            }
        };
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");
    }

    @Test
    public void testTotalFileLength(@Mocked PaimonTable table) {
        BinaryRow row1 = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row1, 10);
        writer.writeInt(0, 2000);
        writer.writeInt(1, 4444);
        writer.complete();

        List<DataFileMeta> meta1 = new ArrayList<>();
        meta1.add(DataFileMeta.create("file1", 100L, 200L, EMPTY_MIN_KEY, EMPTY_MAX_KEY, 
                EMPTY_STATS, EMPTY_STATS, 100L, 200L, 1L, DUMMY_LEVEL, 0L, null, null, null, null, null));
        meta1.add(DataFileMeta.create("file2", 100L, 300L, EMPTY_MIN_KEY, EMPTY_MAX_KEY, 
                EMPTY_STATS, EMPTY_STATS, 100L, 300L, 1L, DUMMY_LEVEL, 0L, null, null, null, null, null));

        DataSplit split = DataSplit.builder().withSnapshot(1L).withPartition(row1).withBucket(1)
                .withBucketPath("not used").withDataFiles(meta1).isStreaming(false).build();

        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");
        long totalFileLength = scanNode.getTotalFileLength(split);

        Assertions.assertEquals(200, totalFileLength);
    }

    @Test
    public void testEstimatedLength(@Mocked PaimonTable table) {
        BinaryRow row1 = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row1, 10);
        writer.writeInt(0, 2000);
        writer.writeInt(1, 4444);
        writer.complete();

        List<DataFileMeta> meta1 = new ArrayList<>();
        meta1.add(DataFileMeta.create("file1", 100L, 200L, EMPTY_MIN_KEY, EMPTY_MAX_KEY, 
                EMPTY_STATS, EMPTY_STATS, 100L, 200L, 1L, DUMMY_LEVEL, 0L, null, null, null, null, null));
        meta1.add(DataFileMeta.create("file2", 100L, 300L, EMPTY_MIN_KEY, EMPTY_MAX_KEY, 
                EMPTY_STATS, EMPTY_STATS, 100L, 300L, 1L, DUMMY_LEVEL, 0L, null, null, null, null, null));

        DataSplit split = DataSplit.builder().withSnapshot(1L).withPartition(row1).withBucket(1)
                .withBucketPath("not used").withDataFiles(meta1).isStreaming(false).build();

        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        SlotDescriptor slot1 = new SlotDescriptor(new SlotId(1), "id", IntegerType.INT, false);
        slot1.setColumn(new Column("id", IntegerType.INT));
        SlotDescriptor slot2 = new SlotDescriptor(new SlotId(2), "name", StringType.STRING, false);
        slot2.setColumn(new Column("name", StringType.STRING));
        desc.addSlot(slot1);
        desc.addSlot(slot2);
        PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");
        long totalFileLength = scanNode.getEstimatedLength(split.rowCount(), desc);
        Assertions.assertEquals(10000, totalFileLength);
    }

    @Test
    public void testSplitRawFileScanRange(@Mocked PaimonTable table, @Mocked RawFile rawFile) {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();

        BinaryRow row1 = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row1, 10);
        writer.writeInt(0, 2000);
        writer.writeInt(1, 4444);
        writer.complete();

        List<DataFileMeta> meta1 = new ArrayList<>();

        meta1.add(DataFileMeta.create("file1", 100L, 200L, EMPTY_MIN_KEY, EMPTY_MAX_KEY, 
                EMPTY_STATS, EMPTY_STATS, 100L, 200L, 1L, DUMMY_LEVEL, 0L, null, null, null, null, null));
        meta1.add(DataFileMeta.create("file2", 100L, 300L, EMPTY_MIN_KEY, EMPTY_MAX_KEY, 
                EMPTY_STATS, EMPTY_STATS, 100L, 300L, 1L, DUMMY_LEVEL, 0L, null, null, null, null, null));

        DataSplit split = DataSplit.builder().withSnapshot(1L).withPartition(row1).withBucket(1)
                .withBucketPath("not used").withDataFiles(meta1).isStreaming(false).build();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        new Expectations() {
            {
                rawFile.format();
                result = "orc";
            }
        };
        desc.setTable(table);
        PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");
        DeletionFile deletionFile = new DeletionFile("dummy", 1, 22, 0L);
        scanNode.splitRawFileScanRangeLocations(rawFile, deletionFile);
        scanNode.splitScanRangeLocations(rawFile, 0, 256 * 1024 * 1024, 64 * 1024 * 1024, null);
        scanNode.addJNISplitScanRangeLocations(split, null, 256 * 1024 * 1024);
        Assertions.assertEquals(6, scanNode.getScanRangeLocations(10).size());
    }

    @Test
    public void testAddSplitScanRangeLocations(@Mocked PaimonTable table, @Mocked RawFile rawFile) {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();

        BinaryRow row1 = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row1, 10);
        writer.writeInt(0, 2000);
        writer.writeInt(1, 4444);
        writer.complete();

        List<DataFileMeta> meta1 = new ArrayList<>();

        meta1.add(DataFileMeta.create("file1", 100L, 200L, EMPTY_MIN_KEY, EMPTY_MAX_KEY, 
                EMPTY_STATS, EMPTY_STATS, 100L, 200L, 1L, DUMMY_LEVEL, 0L, null, null, null, null, null));
        meta1.add(DataFileMeta.create("file2", 100L, 300L, EMPTY_MIN_KEY, EMPTY_MAX_KEY, 
                EMPTY_STATS, EMPTY_STATS, 100L, 300L, 1L, DUMMY_LEVEL, 0L, null, null, null, null, null));

        DataSplit split = DataSplit.builder().withSnapshot(1L).withPartition(row1).withBucket(1)
                .withBucketPath("not used").withDataFiles(meta1).isStreaming(false).build();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");
        scanNode.addJNISplitScanRangeLocations(split, null, 256 * 1024 * 1024);
        Assertions.assertEquals(1, scanNode.getScanRangeLocations(10).size());
        TScanRangeLocations tScanRangeLocations = scanNode.getScanRangeLocations(10).get(0);
        THdfsScanRange hdfsScanRange = tScanRangeLocations.getScan_range().getHdfs_scan_range();
        Assertions.assertEquals(THdfsFileFormat.UNKNOWN, hdfsScanRange.getFile_format());
        Assertions.assertTrue(hdfsScanRange.isUse_paimon_jni_reader());
        Assertions.assertFalse(hdfsScanRange.isUse_paimon_native_reader());
        Assertions.assertFalse(hdfsScanRange.isSetPaimon_table_path());
        Assertions.assertFalse(hdfsScanRange.isSetPaimon_split_info_binary());
        Assertions.assertEquals(com.starrocks.qe.SessionVariable.PaimonReaderMode.AUTO,
                ctx.getSessionVariable().getPaimonReaderMode());
    }

    @Test
    public void testAddNativeSplitScanRangeLocations(@Mocked PaimonTable table) throws IOException {
        String tablePath = "s3://warehouse/db/table";
        new Expectations() {
            {
                table.getTableLocation();
                result = tablePath;
            }
        };

        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setPaimonReaderMode("NATIVE");
        ctx.setThreadLocalInfo();
        try {
            DataSplit split = createDataSplit();

            TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
            desc.setTable(table);
            PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");
            scanNode.addSplitScanRangeLocations(split, null, 200L);

            Assertions.assertEquals(1, scanNode.getScanRangeLocations(10).size());
            THdfsScanRange hdfsScanRange = scanNode.getScanRangeLocations(10).get(0)
                    .getScan_range().getHdfs_scan_range();
            Assertions.assertFalse(hdfsScanRange.isUse_paimon_jni_reader());
            Assertions.assertTrue(hdfsScanRange.isUse_paimon_native_reader());
            Assertions.assertEquals(tablePath, hdfsScanRange.getFull_path());
            Assertions.assertEquals(tablePath, hdfsScanRange.getPaimon_table_path());
            Assertions.assertTrue(hdfsScanRange.isSetPaimon_split_info_binary());
            Assertions.assertNotNull(hdfsScanRange.getPaimon_split_info_binary());
            Assertions.assertTrue(hdfsScanRange.getPaimon_split_info_binary().length > 0);
            DataSplit deserializedSplit = DataSplit.deserialize(new DataInputViewStreamWrapper(
                    new ByteArrayInputStream(hdfsScanRange.getPaimon_split_info_binary())));
            Assertions.assertEquals(split.snapshotId(), deserializedSplit.snapshotId());
            Assertions.assertEquals(split.bucket(), deserializedSplit.bucket());
            Assertions.assertEquals(split.bucketPath(), deserializedSplit.bucketPath());

            // The deprecated boolean only affects AUTO mode and cannot override an explicit mode.
            ctx.getSessionVariable().setPaimonForceJNIReader(true);
            PaimonScanNode stillNativeScanNode = new PaimonScanNode(new PlanNodeId(1), desc, "XXX");
            stillNativeScanNode.addSplitScanRangeLocations(split, null, 200L);
            THdfsScanRange stillNativeRange = stillNativeScanNode.getScanRangeLocations(10).get(0)
                    .getScan_range().getHdfs_scan_range();
            Assertions.assertFalse(stillNativeRange.isUse_paimon_jni_reader());
            Assertions.assertTrue(stillNativeRange.isUse_paimon_native_reader());

            ctx.getSessionVariable().setPaimonReaderMode("JNI");
            PaimonScanNode jniScanNode = new PaimonScanNode(new PlanNodeId(1), desc, "XXX");
            jniScanNode.addSplitScanRangeLocations(split, null, 200L);
            THdfsScanRange jniScanRange = jniScanNode.getScanRangeLocations(10).get(0)
                    .getScan_range().getHdfs_scan_range();
            Assertions.assertTrue(jniScanRange.isUse_paimon_jni_reader());
            Assertions.assertFalse(jniScanRange.isUse_paimon_native_reader());
            Assertions.assertFalse(jniScanRange.isSetPaimon_table_path());
            Assertions.assertFalse(jniScanRange.isSetPaimon_split_info_binary());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testSetupScanRangeLocationsAutoRoutesRawAndUnknownFormats(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked MetadataMgr metadataMgr, @Mocked PaimonTable table) {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        try {
            DeletionFile deletionFile = new DeletionFile("delete-file", 7, 11, 0L);
            DataSplit parquetSplit = createRawConvertibleDataSplit("parquet", List.of(deletionFile));
            DataSplit unknownSplit = createRawConvertibleDataSplit("unknown", List.of());
            List<RemoteFileInfo> remoteFiles = createRemoteFiles(parquetSplit, unknownSplit);
            new Expectations() {
                {
                    GlobalStateMgr.getCurrentState();
                    result = globalStateMgr;
                    globalStateMgr.getMetadataMgr();
                    result = metadataMgr;
                    metadataMgr.getRemoteFiles((Table) any, (GetRemoteFilesParams) any);
                    result = remoteFiles;
                }
            };

            TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
            desc.setTable(table);
            PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");
            scanNode.setupScanRangeLocations(desc, null, -1);

            Assertions.assertEquals(2, scanNode.getScanRangeLocations(10).size());
            THdfsScanRange parquetRange = scanNode.getScanRangeLocations(10).get(0)
                    .getScan_range().getHdfs_scan_range();
            Assertions.assertEquals(THdfsFileFormat.PARQUET, parquetRange.getFile_format());
            Assertions.assertTrue(parquetRange.isSetPaimon_deletion_file());
            Assertions.assertEquals(deletionFile.path(), parquetRange.getPaimon_deletion_file().getPath());
            Assertions.assertFalse(parquetRange.isUse_paimon_jni_reader());

            THdfsScanRange unknownRange = scanNode.getScanRangeLocations(10).get(1)
                    .getScan_range().getHdfs_scan_range();
            Assertions.assertEquals(THdfsFileFormat.UNKNOWN, unknownRange.getFile_format());
            Assertions.assertTrue(unknownRange.isUse_paimon_jni_reader());
            Assertions.assertFalse(unknownRange.isUse_paimon_native_reader());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testSetupScanRangeLocationsNativeAndNonDataSplit(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked MetadataMgr metadataMgr, @Mocked PaimonTable table) {
        String tablePath = "s3://warehouse/db/table";
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setPaimonReaderMode("NATIVE");
        ctx.setThreadLocalInfo();
        try {
            DataSplit dataSplit = createDataSplit();
            List<RemoteFileInfo> remoteFiles = createRemoteFiles(dataSplit, new TestSplit());
            new Expectations() {
                {
                    GlobalStateMgr.getCurrentState();
                    result = globalStateMgr;
                    globalStateMgr.getMetadataMgr();
                    result = metadataMgr;
                    metadataMgr.getRemoteFiles((Table) any, (GetRemoteFilesParams) any);
                    result = remoteFiles;
                    table.getTableLocation();
                    result = tablePath;
                }
            };

            TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
            desc.setTable(table);
            PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");
            scanNode.setupScanRangeLocations(desc, null, -1);

            Assertions.assertEquals(2, scanNode.getScanRangeLocations(10).size());
            THdfsScanRange nativeRange = scanNode.getScanRangeLocations(10).get(0)
                    .getScan_range().getHdfs_scan_range();
            Assertions.assertTrue(nativeRange.isUse_paimon_native_reader());
            Assertions.assertTrue(nativeRange.isSetPaimon_split_info_binary());
            Assertions.assertEquals(tablePath, nativeRange.getPaimon_table_path());

            THdfsScanRange nonDataSplitRange = scanNode.getScanRangeLocations(10).get(1)
                    .getScan_range().getHdfs_scan_range();
            Assertions.assertTrue(nonDataSplitRange.isUse_paimon_jni_reader());
            Assertions.assertFalse(nonDataSplitRange.isUse_paimon_native_reader());
            Assertions.assertFalse(nonDataSplitRange.isSetPaimon_split_info_binary());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testNativeSplitSerializationFailure(@Mocked PaimonTable table) {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setPaimonReaderMode("NATIVE");
        ctx.setThreadLocalInfo();
        try {
            TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
            desc.setTable(table);
            PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");

            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> scanNode.addSplitScanRangeLocations(
                            new FailingDataSplit(createDataSplit()), null, 200L));
            Assertions.assertTrue(exception.getMessage().contains("Failed to serialize Paimon data split"));
            Assertions.assertTrue(exception.getCause() instanceof IOException);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testSplitRawFileScanRangeLocationsWithSessionVariable(@Mocked PaimonTable table, @Mocked RawFile rawFile) {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        ctx.getSessionVariable().setConnectorMaxSplitSize(50L);

        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        new Expectations() {
            {
                rawFile.format();
                result = "orc";
                rawFile.offset();
                result = 100L;
                rawFile.length();
                result = 120L;
                rawFile.path();
                result = "hdfs://dummy";
            }
        };
        desc.setTable(table);
        PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");
        scanNode.splitRawFileScanRangeLocations(rawFile, null);
        Assertions.assertEquals(2, scanNode.getScanRangeLocations(10).size());
    }

    @Test
    public void testJniReaderRejectsVariantSlot() {
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(0));
        SlotDescriptor slot = new SlotDescriptor(new SlotId(0), tuple);
        slot.setType(com.starrocks.type.VariantType.VARIANT);
        slot.setColumn(new Column("v", com.starrocks.type.VariantType.VARIANT));
        tuple.addSlot(slot);
        StarRocksConnectorException e = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> PaimonScanNode.checkJniReaderVariantSupport(tuple));
        Assertions.assertTrue(e.getMessage().contains("VARIANT"));
    }

    @Test
    public void testJniReaderAllowsNonVariantSlots() {
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(0));
        SlotDescriptor slot = new SlotDescriptor(new SlotId(0), tuple);
        slot.setType(IntegerType.INT); // same constant PaimonColumnConverterTest asserts against
        slot.setColumn(new Column("i", IntegerType.INT));
        tuple.addSlot(slot);
        Assertions.assertDoesNotThrow(() -> PaimonScanNode.checkJniReaderVariantSupport(tuple));
    }

    private static DataSplit createDataSplit() {
        BinaryRow partition = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(partition, 10);
        writer.writeInt(0, 2000);
        writer.writeInt(1, 4444);
        writer.complete();

        DataFileMeta dataFile = DataFileMeta.create("file1", 100L, 200L, EMPTY_MIN_KEY, EMPTY_MAX_KEY,
                EMPTY_STATS, EMPTY_STATS, 100L, 200L, 1L, DUMMY_LEVEL, List.of(),
                null, null, null, null, null, null, null);
        return DataSplit.builder().withSnapshot(1L).withPartition(partition).withBucket(1)
                .withBucketPath("bucket-1").withDataFiles(List.of(dataFile)).isStreaming(false).build();
    }

    private static DataSplit createRawConvertibleDataSplit(String format, List<DeletionFile> deletionFiles) {
        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(partition, 4);
        writer.writeInt(0, 1);
        writer.complete();
        DataFileMeta dataFile = DataFileMeta.create("file." + format, 100L, 10L, EMPTY_MIN_KEY, EMPTY_MAX_KEY,
                EMPTY_STATS, EMPTY_STATS, 1L, 10L, 1L, DUMMY_LEVEL, List.of(), null, null,
                FileSource.APPEND, List.of(), null, null, List.of());
        DataSplit.Builder builder = DataSplit.builder().withSnapshot(1L).withPartition(partition).withBucket(1)
                .withBucketPath("s3://warehouse/db/table/bucket-1").withDataFiles(List.of(dataFile))
                .rawConvertible(true).isStreaming(false);
        if (!deletionFiles.isEmpty()) {
            builder.withDataDeletionFiles(deletionFiles);
        }
        return builder.build();
    }

    private static List<RemoteFileInfo> createRemoteFiles(Split... splits) {
        PaimonSplitsInfo splitsInfo = new PaimonSplitsInfo(List.of(), List.of(splits));
        return List.of(RemoteFileInfo.builder()
                .setFiles(List.of(PaimonRemoteFileDesc.createPaimonRemoteFileDesc(splitsInfo)))
                .build());
    }

    private static class TestSplit implements Split {
        @Override
        public long rowCount() {
            return 1;
        }
    }

    private static class FailingDataSplit extends DataSplit {
        private int serializeCount;

        private FailingDataSplit(DataSplit split) {
            assign(split);
        }

        @Override
        public void serialize(DataOutputView out) throws IOException {
            if (++serializeCount > 1) {
                throw new IOException("expected binary split serialization failure");
            }
            super.serialize(out);
        }
    }
}
