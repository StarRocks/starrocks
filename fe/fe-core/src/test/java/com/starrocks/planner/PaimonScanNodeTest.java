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
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.paimon.PaimonReaderSelector.Reader;
import com.starrocks.connector.paimon.PaimonRemoteFileDesc;
import com.starrocks.connector.paimon.PaimonSplitsInfo;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SessionVariable.PaimonReaderMode;
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
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

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
        scanNode.addSDKSplitScanRangeLocations(Reader.NATIVE, split, null, 256 * 1024 * 1024);
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
        scanNode.addSDKSplitScanRangeLocations(Reader.JNI, split, null, 256 * 1024 * 1024);
        Assertions.assertEquals(1, scanNode.getScanRangeLocations(10).size());
        TScanRangeLocations tScanRangeLocations = scanNode.getScanRangeLocations(10).get(0);
        THdfsScanRange hdfsScanRange = tScanRangeLocations.getScan_range().getHdfs_scan_range();
        Assertions.assertEquals(THdfsFileFormat.UNKNOWN, hdfsScanRange.getFile_format());
        Assertions.assertTrue(hdfsScanRange.isUse_paimon_jni_reader());
        Assertions.assertFalse(hdfsScanRange.isUse_paimon_native_reader());
        Assertions.assertFalse(hdfsScanRange.isSetPaimon_split_info_binary());
        Assertions.assertEquals(com.starrocks.qe.SessionVariable.PaimonReaderMode.AUTO,
                ctx.getSessionVariable().getPaimonReaderMode());
    }

    @Test
    public void testAddNativeSplitScanRangeLocations(@Mocked PaimonTable table) throws IOException {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        try {
            DataSplit split = createDataSplit();

            TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
            desc.setTable(table);
            PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");
            String predicateInfo = PaimonScanNode.encodeObjectToString(List.of());
            scanNode.addSDKSplitScanRangeLocations(Reader.NATIVE, split, predicateInfo, 200L);

            Assertions.assertEquals(1, scanNode.getScanRangeLocations(10).size());
            THdfsScanRange hdfsScanRange = scanNode.getScanRangeLocations(10).get(0)
                    .getScan_range().getHdfs_scan_range();
            Assertions.assertFalse(hdfsScanRange.isUse_paimon_jni_reader());
            Assertions.assertTrue(hdfsScanRange.isUse_paimon_native_reader());
            Assertions.assertFalse(hdfsScanRange.isSetFull_path());
            Assertions.assertFalse(hdfsScanRange.isSetPaimon_split_info());
            Assertions.assertFalse(hdfsScanRange.isSetJni_predicate_info());
            Assertions.assertTrue(hdfsScanRange.isSetPaimon_split_info_binary());
            Assertions.assertNotNull(hdfsScanRange.getPaimon_split_info_binary());
            Assertions.assertTrue(hdfsScanRange.getPaimon_split_info_binary().length > 0);
            DataSplit deserializedSplit = DataSplit.deserialize(new DataInputViewStreamWrapper(
                    new ByteArrayInputStream(hdfsScanRange.getPaimon_split_info_binary())));
            Assertions.assertEquals(split.snapshotId(), deserializedSplit.snapshotId());
            Assertions.assertEquals(split.bucket(), deserializedSplit.bucket());
            Assertions.assertEquals(split.bucketPath(), deserializedSplit.bucketPath());

            PaimonScanNode jniScanNode = new PaimonScanNode(new PlanNodeId(1), desc, "XXX");
            jniScanNode.addSDKSplitScanRangeLocations(Reader.JNI, split, predicateInfo, 200L);
            THdfsScanRange jniScanRange = jniScanNode.getScanRangeLocations(10).get(0)
                    .getScan_range().getHdfs_scan_range();
            Assertions.assertTrue(jniScanRange.isUse_paimon_jni_reader());
            Assertions.assertFalse(jniScanRange.isUse_paimon_native_reader());
            Assertions.assertFalse(jniScanRange.isSetPaimon_split_info_binary());
            Assertions.assertEquals(PaimonScanNode.encodeObjectToString(split), jniScanRange.getPaimon_split_info());
            Assertions.assertEquals(predicateInfo, jniScanRange.getJni_predicate_info());
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> scanNode.addSDKSplitScanRangeLocations(Reader.STARROCKS, split, predicateInfo, 200L));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testSetupScanRangeLocationsAutoRoutesRawSdkAndSystemSplits(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked MetadataMgr metadataMgr, @Mocked PaimonTable table) {
        ConnectContext ctx = new ConnectContext();
        // With GlobalStateMgr mocked, the ConnectContext-created SessionVariable is a cascaded mock
        // that returns null for the reader mode, so install a real one (defaults to AUTO).
        ctx.setSessionVariable(new SessionVariable());
        ctx.setThreadLocalInfo();
        try {
            DeletionFile deletionFile = new DeletionFile("delete-file", 7, 11, 0L);
            DataSplit parquetSplit = createRawConvertibleDataSplit("parquet", List.of(deletionFile));
            DataSplit avroSplit = createRawConvertibleDataSplit("avro", List.of());
            FileStoreTable nativeTable = createNativeTable();
            List<RemoteFileInfo> remoteFiles = createRemoteFiles(parquetSplit, avroSplit, new TestSplit());
            new Expectations() {
                {
                    GlobalStateMgr.getCurrentState();
                    result = globalStateMgr;
                    globalStateMgr.getMetadataMgr();
                    result = metadataMgr;
                    table.getNativeTable();
                    result = nativeTable;
                    metadataMgr.getRemoteFiles((Table) any, (GetRemoteFilesParams) any);
                    result = remoteFiles;
                }
            };

            TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
            desc.setTable(table);
            SlotDescriptor slot = new SlotDescriptor(new SlotId(1), "id", IntegerType.INT, false);
            slot.setColumn(new Column("id", IntegerType.INT));
            desc.addSlot(slot);
            PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");
            scanNode.setupScanRangeLocations(desc, null, -1);

            Assertions.assertEquals(3, scanNode.getScanRangeLocations(10).size());
            THdfsScanRange parquetRange = scanNode.getScanRangeLocations(10).get(0)
                    .getScan_range().getHdfs_scan_range();
            Assertions.assertEquals(THdfsFileFormat.PARQUET, parquetRange.getFile_format());
            Assertions.assertTrue(parquetRange.isSetPaimon_deletion_file());
            Assertions.assertEquals(deletionFile.path(), parquetRange.getPaimon_deletion_file().getPath());
            Assertions.assertFalse(parquetRange.isUse_paimon_jni_reader());
            Assertions.assertEquals(100L, parquetRange.getLength());

            THdfsScanRange avroRange = scanNode.getScanRangeLocations(10).get(1)
                    .getScan_range().getHdfs_scan_range();
            Assertions.assertEquals(THdfsFileFormat.UNKNOWN, avroRange.getFile_format());
            Assertions.assertFalse(avroRange.isUse_paimon_jni_reader());
            Assertions.assertTrue(avroRange.isUse_paimon_native_reader());
            Assertions.assertTrue(avroRange.isSetPaimon_split_info_binary());
            Assertions.assertEquals(100L, avroRange.getLength());

            THdfsScanRange systemRange = scanNode.getScanRangeLocations(10).get(2)
                    .getScan_range().getHdfs_scan_range();
            Assertions.assertTrue(systemRange.isUse_paimon_jni_reader());
            Assertions.assertFalse(systemRange.isUse_paimon_native_reader());
            Assertions.assertFalse(systemRange.isSetPaimon_split_info_binary());
            Assertions.assertEquals(IntegerType.INT.getTypeSize(), systemRange.getLength());
            Assertions.assertEquals(1, scanNode.getScanNodePredicates().getSelectedPartitionIds().size());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testSetupScanRangeLocationsNativeRejectsNonDataSplit(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked MetadataMgr metadataMgr, @Mocked PaimonTable table) {
        ConnectContext ctx = new ConnectContext();
        // With GlobalStateMgr mocked, the ConnectContext-created SessionVariable is a cascaded mock
        // whose setters are no-ops, so install a real one before configuring the reader mode.
        ctx.setSessionVariable(new SessionVariable());
        ctx.getSessionVariable().setPaimonReaderMode("NATIVE");
        ctx.setThreadLocalInfo();
        try {
            FileStoreTable nativeTable = createNativeTable();
            List<RemoteFileInfo> remoteFiles = createRemoteFiles(new TestSplit());
            new Expectations() {
                {
                    GlobalStateMgr.getCurrentState();
                    result = globalStateMgr;
                    globalStateMgr.getMetadataMgr();
                    result = metadataMgr;
                    table.getNativeTable();
                    result = nativeTable;
                    metadataMgr.getRemoteFiles((Table) any, (GetRemoteFilesParams) any);
                    result = remoteFiles;
                }
            };

            TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
            desc.setTable(table);
            PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");
            var error = Assertions.assertThrows(StarRocksConnectorException.class,
                    () -> scanNode.setupScanRangeLocations(desc, null, -1));
            Assertions.assertTrue(error.getMessage().contains("SYSTEM_SPLIT"));
            Assertions.assertTrue(scanNode.getScanRangeLocations(10).isEmpty());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testSetupIndexedSplitHonorsForcedJniAndTracksPartition(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked MetadataMgr metadataMgr, @Mocked PaimonTable table)
            throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(new SessionVariable());
        ctx.getSessionVariable().setPaimonReaderMode("NATIVE");
        Deencapsulation.setField(ctx.getSessionVariable(), "paimonForceJNIReader", true);
        ctx.setThreadLocalInfo();
        try {
            DataSplit dataSplit = createDataSplit();
            IndexedSplit indexedSplit = new IndexedSplit(dataSplit, List.of(new Range(0L, 0L)), null);
            FileStoreTable nativeTable = createNativeTable();
            List<RemoteFileInfo> remoteFiles = createRemoteFiles(indexedSplit);
            new Expectations() {
                {
                    GlobalStateMgr.getCurrentState();
                    result = globalStateMgr;
                    globalStateMgr.getMetadataMgr();
                    result = metadataMgr;
                    table.getNativeTable();
                    result = nativeTable;
                    metadataMgr.getRemoteFiles((Table) any, (GetRemoteFilesParams) any);
                    result = remoteFiles;
                }
            };

            TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
            desc.setTable(table);
            PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");
            scanNode.setupScanRangeLocations(desc, null, -1);

            Assertions.assertEquals(1, scanNode.getScanRangeLocations(10).size());
            THdfsScanRange indexedRange = scanNode.getScanRangeLocations(10).get(0)
                    .getScan_range().getHdfs_scan_range();
            Assertions.assertTrue(indexedRange.isUse_paimon_jni_reader());
            Assertions.assertFalse(indexedRange.isUse_paimon_native_reader());
            Assertions.assertTrue(indexedRange.isSetPaimon_split_info());
            Assertions.assertTrue(indexedRange.isSetJni_predicate_info());
            Assertions.assertFalse(indexedRange.isSetPaimon_split_info_binary());
            IndexedSplit decoded = InstantiationUtil.deserializeObject(
                    Base64.getUrlDecoder().decode(indexedRange.getPaimon_split_info()), getClass().getClassLoader());
            Assertions.assertEquals(dataSplit.snapshotId(), decoded.dataSplit().snapshotId());
            Assertions.assertEquals(dataSplit.bucket(), decoded.dataSplit().bucket());
            Assertions.assertEquals(1, decoded.rowRanges().size());
            Assertions.assertEquals(0L, decoded.rowRanges().get(0).from);
            Assertions.assertEquals(0L, decoded.rowRanges().get(0).to);
            Assertions.assertEquals(100L, indexedRange.getFile_length());
            Assertions.assertEquals(1, scanNode.getScanNodePredicates().getSelectedPartitionIds().size());

            // AUTO still uses native for compatible index ranges.
            Deencapsulation.setField(ctx.getSessionVariable(), "paimonForceJNIReader", false);
            ctx.getSessionVariable().setPaimonReaderMode("AUTO");
            PaimonScanNode autoScanNode = new PaimonScanNode(new PlanNodeId(1), desc, "XXX");
            autoScanNode.setupScanRangeLocations(desc, null, -1);
            THdfsScanRange autoRange = autoScanNode.getScanRangeLocations(10).get(0).getScan_range().getHdfs_scan_range();
            Assertions.assertTrue(autoRange.isUse_paimon_native_reader());
            Assertions.assertFalse(autoRange.isUse_paimon_jni_reader());
            IndexedSplit nativeDecoded = IndexedSplit.deserialize(new DataInputViewStreamWrapper(
                    new ByteArrayInputStream(autoRange.getPaimon_split_info_binary())));
            Assertions.assertEquals(decoded.rowRanges(), nativeDecoded.rowRanges());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testNativeSplitSerializationFailure(@Mocked PaimonTable table) {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        try {
            TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
            desc.setTable(table);
            PaimonScanNode scanNode = new PaimonScanNode(new PlanNodeId(0), desc, "XXX");

            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> scanNode.addSDKSplitScanRangeLocations(
                            Reader.NATIVE, new FailingDataSplit(createDataSplit()), null, 200L));
            Assertions.assertTrue(exception.getMessage().contains("Failed to serialize Paimon split"));
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

    private static FileStoreTable createNativeTable() {
        FileStoreTable table = org.mockito.Mockito.mock(FileStoreTable.class);
        org.mockito.Mockito.when(table.schema()).thenReturn(new TableSchema(1L,
                List.of(DataTypes.FIELD(0, "id", DataTypes.INT())), 0, List.of(), List.of(), Map.of(), ""));
        return table;
    }

    private static DataSplit createDataSplit() {
        BinaryRow partition = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(partition, 10);
        writer.writeInt(0, 2000);
        writer.writeInt(1, 4444);
        writer.complete();

        DataFileMeta dataFile = DataFileMeta.create("file1.parquet", 100L, 200L, EMPTY_MIN_KEY, EMPTY_MAX_KEY,
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

        @Override
        public OptionalLong mergedRowCount() {
            return OptionalLong.empty();
        }
    }

    private static class FailingDataSplit extends DataSplit {
        private FailingDataSplit(DataSplit split) {
            assign(split);
        }

        @Override
        public void serialize(DataOutputView out) throws IOException {
            throw new IOException("expected binary split serialization failure");
        }
    }
}
