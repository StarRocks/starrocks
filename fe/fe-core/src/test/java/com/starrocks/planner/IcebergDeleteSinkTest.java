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
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TIcebergTableSink;

import java.util.HashMap;
import java.util.Map;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

public class IcebergDeleteSinkTest {

    private MockedStatic<IcebergUtil> mockedIcebergUtil;

    @BeforeEach
    public void setUp() {
        // Mock the static method
        mockedIcebergUtil = mockStatic(IcebergUtil.class);
        CloudConfiguration mockCloudConfig = mock(CloudConfiguration.class);

        // Mock toThrift method to avoid NPE during test
        doAnswer(invocation -> {
            com.starrocks.thrift.TCloudConfiguration tConfig = invocation.getArgument(0);
            tConfig.setCloud_type(com.starrocks.thrift.TCloudType.AWS);
            return null;
        }).when(mockCloudConfig).toThrift(any(com.starrocks.thrift.TCloudConfiguration.class));

        when(IcebergUtil.getVendedCloudConfiguration(anyString(), any(IcebergTable.class)))
                .thenReturn(mockCloudConfig);
        when(IcebergUtil.getVendedCloudConfiguration(isNull(), any(IcebergTable.class)))
                .thenReturn(mockCloudConfig);
    }

    @AfterEach
    public void tearDown() {
        // Close the static mock
        if (mockedIcebergUtil != null) {
            mockedIcebergUtil.close();
        }
    }

    @Test
    public void testValidDeleteTuple() {
        // Create a valid tuple descriptor with _file and _pos columns
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0), "DeleteTuple");

        // Add _file column (STRING type)
        Column fileColumn = new Column(IcebergTable.FILE_PATH, VarcharType.VARCHAR);
        SlotDescriptor fileSlot = new SlotDescriptor(new SlotId(0), desc);
        fileSlot.setColumn(fileColumn);
        desc.addSlot(fileSlot);

        // Add _pos column (BIGINT type)
        Column posColumn = new Column(IcebergTable.ROW_POSITION, IntegerType.BIGINT);
        SlotDescriptor posSlot = new SlotDescriptor(new SlotId(1), desc);
        posSlot.setColumn(posColumn);
        desc.addSlot(posSlot);

        // Mock IcebergTable
        IcebergTable icebergTable = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        when(icebergTable.getNativeTable()).thenReturn(nativeTable);
        when(nativeTable.location()).thenReturn("/tmp/iceberg");

        // Should not throw exception
        IcebergDeleteSink sink = new IcebergDeleteSink(icebergTable, desc, new SessionVariable());
        assertNotNull(sink);
    }

    @Test
    public void testMissingFileColumn() {
        // Create a tuple descriptor without _file column
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0), "DeleteTuple");

        // Add only _pos column
        Column posColumn = new Column(IcebergTable.ROW_POSITION, IntegerType.BIGINT);
        SlotDescriptor posSlot = new SlotDescriptor(new SlotId(0), desc);
        posSlot.setColumn(posColumn);
        desc.addSlot(posSlot);

        // Mock IcebergTable
        IcebergTable icebergTable = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        when(icebergTable.getNativeTable()).thenReturn(nativeTable);
        when(nativeTable.location()).thenReturn("/tmp/iceberg");

        // Should throw exception
        IcebergDeleteSink deleteSink = new IcebergDeleteSink(icebergTable, desc, new SessionVariable());
        StarRocksConnectorException exception = assertThrows(StarRocksConnectorException.class, deleteSink::init);
        assertTrue(exception.getMessage().contains("_file"));
    }

    @Test
    public void testThriftSerialization() {
        // Create a valid tuple descriptor
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0), "DeleteTuple");

        Column fileColumn = new Column(IcebergTable.FILE_PATH, VarcharType.VARCHAR);
        SlotDescriptor fileSlot = new SlotDescriptor(new SlotId(0), desc);
        fileSlot.setColumn(fileColumn);
        desc.addSlot(fileSlot);

        Column posColumn = new Column(IcebergTable.ROW_POSITION, IntegerType.BIGINT);
        SlotDescriptor posSlot = new SlotDescriptor(new SlotId(1), desc);
        posSlot.setColumn(posColumn);
        desc.addSlot(posSlot);

        // Mock IcebergTable
        IcebergTable icebergTable = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        when(icebergTable.getNativeTable()).thenReturn(nativeTable);
        when(nativeTable.location()).thenReturn("hdfs://localhost:9000/iceberg");
        when(icebergTable.getUUID()).thenReturn("iceberg_catalog.db.table");

        IcebergDeleteSink sink = new IcebergDeleteSink(icebergTable, desc, new SessionVariable());
        sink.init();

        // Check thrift serialization
        TDataSink tDataSink = sink.toThrift();
        assertNotNull(tDataSink);
        assertTrue(tDataSink.isSetIceberg_table_sink());

        TIcebergTableSink icebergSink = tDataSink.getIceberg_table_sink();
        assertEquals("hdfs://localhost:9000/iceberg", icebergSink.getLocation());
        assertEquals("parquet", icebergSink.getFile_format());
        assertFalse(icebergSink.isIs_static_partition_sink());
    }

    @Test
    public void testGetExplainString() {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0), "DeleteTuple");

        Column fileColumn = new Column(IcebergTable.FILE_PATH, VarcharType.VARCHAR);
        SlotDescriptor fileSlot = new SlotDescriptor(new SlotId(0), desc);
        fileSlot.setColumn(fileColumn);
        desc.addSlot(fileSlot);

        Column posColumn = new Column(IcebergTable.ROW_POSITION, IntegerType.BIGINT);
        SlotDescriptor posSlot = new SlotDescriptor(new SlotId(1), desc);
        posSlot.setColumn(posColumn);
        desc.addSlot(posSlot);

        // Mock IcebergTable
        IcebergTable icebergTable = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        when(icebergTable.getNativeTable()).thenReturn(nativeTable);
        when(nativeTable.location()).thenReturn("/tmp/iceberg");
        when(icebergTable.getUUID()).thenReturn("iceberg_catalog.db.table");

        IcebergDeleteSink sink = new IcebergDeleteSink(icebergTable, desc, new SessionVariable());

        String explainString = sink.getExplainString("", TExplainLevel.NORMAL);
        assertTrue(explainString.contains("ICEBERG DELETE SINK"));
        assertTrue(explainString.contains("iceberg_catalog.db.table"));
        assertTrue(explainString.contains("/tmp/iceberg"));
    }

    @Test
    public void testSinkExtraInfo() {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0), "DeleteTuple");

        Column fileColumn = new Column(IcebergTable.FILE_PATH, VarcharType.VARCHAR);
        SlotDescriptor fileSlot = new SlotDescriptor(new SlotId(0), desc);
        fileSlot.setColumn(fileColumn);
        desc.addSlot(fileSlot);

        Column posColumn = new Column(IcebergTable.ROW_POSITION, IntegerType.BIGINT);
        SlotDescriptor posSlot = new SlotDescriptor(new SlotId(1), desc);
        posSlot.setColumn(posColumn);
        desc.addSlot(posSlot);

        // Mock IcebergTable
        IcebergTable icebergTable = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        when(icebergTable.getNativeTable()).thenReturn(nativeTable);
        when(nativeTable.location()).thenReturn("/tmp/iceberg");

        IcebergDeleteSink sink = new IcebergDeleteSink(icebergTable, desc, new SessionVariable());

        // Initially, sink extra info should be null
        assertNull(sink.getSinkExtraInfo());

        // Create and set sink extra info
        com.starrocks.connector.iceberg.IcebergMetadata.IcebergSinkExtra extraInfo =
                new com.starrocks.connector.iceberg.IcebergMetadata.IcebergSinkExtra();
        sink.setSinkExtraInfo(extraInfo);

        // Verify the extra info was set correctly
        assertNotNull(sink.getSinkExtraInfo());
        assertEquals(extraInfo, sink.getSinkExtraInfo());
    }

    @Test
    public void testIsUnpartitionedTable() {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0), "DeleteTuple");

        Column fileColumn = new Column(IcebergTable.FILE_PATH, VarcharType.VARCHAR);
        SlotDescriptor fileSlot = new SlotDescriptor(new SlotId(0), desc);
        fileSlot.setColumn(fileColumn);
        desc.addSlot(fileSlot);

        Column posColumn = new Column(IcebergTable.ROW_POSITION, IntegerType.BIGINT);
        SlotDescriptor posSlot = new SlotDescriptor(new SlotId(1), desc);
        posSlot.setColumn(posColumn);
        desc.addSlot(posSlot);

        IcebergTable icebergTable = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        when(icebergTable.getNativeTable()).thenReturn(nativeTable);
        when(nativeTable.location()).thenReturn("/tmp/iceberg");

        when(icebergTable.isPartitioned()).thenReturn(false);
        IcebergDeleteSink unpartitionedSink = new IcebergDeleteSink(icebergTable, desc, new SessionVariable());
        assertTrue(unpartitionedSink.isUnpartitionedTable());

        when(icebergTable.isPartitioned()).thenReturn(true);
        IcebergDeleteSink partitionedSink = new IcebergDeleteSink(icebergTable, desc, new SessionVariable());
        assertFalse(partitionedSink.isUnpartitionedTable());
    }

    @Test
    public void testCompressionTypePriority() {
        // Create a valid tuple descriptor
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0), "DeleteTuple");

        Column fileColumn = new Column(IcebergTable.FILE_PATH, VarcharType.VARCHAR);
        SlotDescriptor fileSlot = new SlotDescriptor(new SlotId(0), desc);
        fileSlot.setColumn(fileColumn);
        desc.addSlot(fileSlot);

        Column posColumn = new Column(IcebergTable.ROW_POSITION, IntegerType.BIGINT);
        SlotDescriptor posSlot = new SlotDescriptor(new SlotId(1), desc);
        posSlot.setColumn(posColumn);
        desc.addSlot(posSlot);

        // Test 1: Only write.delete.parquet.compression is set
        {
            IcebergTable icebergTable = mock(IcebergTable.class);
            org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
            when(icebergTable.getNativeTable()).thenReturn(nativeTable);
            when(nativeTable.location()).thenReturn("/tmp/iceberg");
            when(icebergTable.getUUID()).thenReturn("iceberg_catalog.db.table");

            Map<String, String> properties = new HashMap<>();
            properties.put("write.delete.parquet.compression-codec", "zstd");
            when(nativeTable.properties()).thenReturn(properties);

            IcebergDeleteSink sink = new IcebergDeleteSink(icebergTable, desc, new SessionVariable());
            sink.init();

            TDataSink tDataSink = sink.toThrift();
            TIcebergTableSink icebergSink = tDataSink.getIceberg_table_sink();
            assertEquals(TCompressionType.ZSTD, icebergSink.getDelete_compression_type());
        }

        // Test 2: Only write.parquet.compression is set (fallback)
        {
            IcebergTable icebergTable = mock(IcebergTable.class);
            org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
            when(icebergTable.getNativeTable()).thenReturn(nativeTable);
            when(nativeTable.location()).thenReturn("/tmp/iceberg");
            when(icebergTable.getUUID()).thenReturn("iceberg_catalog.db.table");

            Map<String, String> properties = new HashMap<>();
            properties.put("write.parquet.compression-codec", "snappy");
            when(nativeTable.properties()).thenReturn(properties);

            IcebergDeleteSink sink = new IcebergDeleteSink(icebergTable, desc, new SessionVariable());
            sink.init();

            TDataSink tDataSink = sink.toThrift();
            TIcebergTableSink icebergSink = tDataSink.getIceberg_table_sink();
            assertEquals(TCompressionType.SNAPPY, icebergSink.getDelete_compression_type());
        }

        // Test 3: Both set, write.delete.parquet.compression has higher priority
        {
            IcebergTable icebergTable = mock(IcebergTable.class);
            org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
            when(icebergTable.getNativeTable()).thenReturn(nativeTable);
            when(nativeTable.location()).thenReturn("/tmp/iceberg");
            when(icebergTable.getUUID()).thenReturn("iceberg_catalog.db.table");

            Map<String, String> properties = new HashMap<>();
            properties.put("write.delete.parquet.compression-codec", "gzip");
            properties.put("write.parquet.compression-codec", "snappy");
            when(nativeTable.properties()).thenReturn(properties);

            IcebergDeleteSink sink = new IcebergDeleteSink(icebergTable, desc, new SessionVariable());
            sink.init();

            TDataSink tDataSink = sink.toThrift();
            TIcebergTableSink icebergSink = tDataSink.getIceberg_table_sink();
            // Should use write.delete.parquet.compression (gzip), not write.parquet.compression (snappy)
            assertEquals(TCompressionType.GZIP, icebergSink.getDelete_compression_type());
        }

        // Test 4: Neither set, use session variable default (uncompressed)
        {
            IcebergTable icebergTable = mock(IcebergTable.class);
            org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
            when(icebergTable.getNativeTable()).thenReturn(nativeTable);
            when(nativeTable.location()).thenReturn("/tmp/iceberg");
            when(icebergTable.getUUID()).thenReturn("iceberg_catalog.db.table");

            Map<String, String> properties = new HashMap<>();
            when(nativeTable.properties()).thenReturn(properties);

            IcebergDeleteSink sink = new IcebergDeleteSink(icebergTable, desc, new SessionVariable());
            sink.init();

            TDataSink tDataSink = sink.toThrift();
            TIcebergTableSink icebergSink = tDataSink.getIceberg_table_sink();
            assertEquals(TCompressionType.NO_COMPRESSION, icebergSink.getDelete_compression_type());
        }

        // Test 5: Session variable with custom compression
        {
            IcebergTable icebergTable = mock(IcebergTable.class);
            org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
            when(icebergTable.getNativeTable()).thenReturn(nativeTable);
            when(nativeTable.location()).thenReturn("/tmp/iceberg");
            when(icebergTable.getUUID()).thenReturn("iceberg_catalog.db.table");

            Map<String, String> properties = new HashMap<>();
            when(nativeTable.properties()).thenReturn(properties);

            SessionVariable sessionVariable = new SessionVariable();
            sessionVariable.setConnectorSinkCompressionCodec("lz4");

            IcebergDeleteSink sink = new IcebergDeleteSink(icebergTable, desc, sessionVariable);
            sink.init();

            TDataSink tDataSink = sink.toThrift();
            TIcebergTableSink icebergSink = tDataSink.getIceberg_table_sink();
            assertEquals(TCompressionType.LZ4, icebergSink.getDelete_compression_type());
        }
    }

    /**
     * DELETE writes position-delete files, which carry the row positions and file paths of the rows
     * being removed. On an encrypted table those must be encrypted like any other file: an unencrypted
     * delete file beside encrypted data still discloses which rows were deleted. Until the signal was
     * derived in one place only INSERT set it, and DELETE wrote plaintext with nothing surfacing it.
     */
    @Test
    public void testEncryptedTableSendsTheEncryptionSignalToTheBackend() {
        TupleDescriptor desc = deleteTuple();

        IcebergTable icebergTable = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        when(icebergTable.getNativeTable()).thenReturn(nativeTable);
        when(nativeTable.location()).thenReturn("/tmp/iceberg");
        when(icebergTable.getUUID()).thenReturn("iceberg_catalog.db.table");

        Map<String, String> properties = new HashMap<>();
        properties.put("encryption.key-id", "my-kms-key");
        properties.put("encryption.data-key-length", "32");
        when(nativeTable.properties()).thenReturn(properties);
        // Stubbed explicitly. An unstubbed encryption() returns null, and `null instanceof
        // PlaintextEncryptionManager` is false, so the refusal gate would be skipped and this test
        // would prove nothing about it -- it would pass even on a catalog that cannot encrypt.
        when(nativeTable.encryption()).thenReturn(new StubEncryptionManager());

        IcebergDeleteSink sink = new IcebergDeleteSink(icebergTable, desc, new SessionVariable());
        sink.init();

        TIcebergTableSink icebergSink = sink.toThrift().getIceberg_table_sink();
        assertTrue(icebergSink.isSetParquet_encryption_info(),
                "a DELETE on an encrypted table must tell the BE to encrypt its position-delete files");
        assertEquals(32, icebergSink.getParquet_encryption_info().getDek_length());
        assertEquals("AES_GCM_V1", icebergSink.getParquet_encryption_info().getEncryption_algorithm());
        // The BE generates its own per-file DEK and returns it at commit; the FE never ships key material.
        assertFalse(icebergSink.getParquet_encryption_info().isSetFile_dek());
    }

    @Test
    public void testUnencryptedTableSendsNoEncryptionSignal() {
        TupleDescriptor desc = deleteTuple();

        IcebergTable icebergTable = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        when(icebergTable.getNativeTable()).thenReturn(nativeTable);
        when(nativeTable.location()).thenReturn("/tmp/iceberg");
        when(icebergTable.getUUID()).thenReturn("iceberg_catalog.db.table");
        when(nativeTable.properties()).thenReturn(new HashMap<>());
        when(nativeTable.encryption()).thenReturn(PlaintextEncryptionManager.instance());

        IcebergDeleteSink sink = new IcebergDeleteSink(icebergTable, desc, new SessionVariable());
        sink.init();

        assertFalse(sink.toThrift().getIceberg_table_sink().isSetParquet_encryption_info());
    }

    /**
     * A table declaring encryption while the catalog hands back a plaintext manager must fail at
     * planning. Asserted here, not only on IcebergEncryption, so the gate is known to be reachable
     * from this call site -- until this existed, no test anywhere covered a sink refusing.
     */
    @Test
    public void testDeclaredEncryptionWithAPlaintextManagerIsRefusedAtPlanning() {
        TupleDescriptor desc = deleteTuple();

        IcebergTable icebergTable = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        when(icebergTable.getNativeTable()).thenReturn(nativeTable);
        when(nativeTable.location()).thenReturn("/tmp/iceberg");
        when(icebergTable.getUUID()).thenReturn("iceberg_catalog.db.table");
        Map<String, String> properties = new HashMap<>();
        properties.put("encryption.key-id", "my-kms-key");
        when(nativeTable.properties()).thenReturn(properties);
        when(nativeTable.encryption()).thenReturn(PlaintextEncryptionManager.instance());

        StarRocksConnectorException e = assertThrows(StarRocksConnectorException.class,
                () -> new IcebergDeleteSink(icebergTable, desc, new SessionVariable()));
        assertTrue(e.getMessage().contains("refusing to write"), e.getMessage());
    }

    /** Non-plaintext, non-standard manager: stands in for a catalog that does apply encryption. */
    private static class StubEncryptionManager implements org.apache.iceberg.encryption.EncryptionManager {
        @Override
        public org.apache.iceberg.io.InputFile decrypt(org.apache.iceberg.encryption.EncryptedInputFile encrypted) {
            throw new UnsupportedOperationException("not reached");
        }

        @Override
        public org.apache.iceberg.encryption.EncryptedOutputFile encrypt(org.apache.iceberg.io.OutputFile rawOutput) {
            throw new UnsupportedOperationException("not reached");
        }
    }

    /**
     * A mocked table leaves both encryption() and properties() unstubbed, i.e. null. That is also a real
     * shape -- properties() is null before the metadata is loaded -- and resolving the DEK length before
     * establishing that the table declares encryption threw NPE straight out of this constructor, which
     * would fail every DELETE at planning.
     */
    @Test
    public void testTableWithNoPropertiesPlansWithoutEncryption() {
        TupleDescriptor desc = deleteTuple();

        IcebergTable icebergTable = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        when(icebergTable.getNativeTable()).thenReturn(nativeTable);
        when(nativeTable.location()).thenReturn("/tmp/iceberg");
        when(icebergTable.getUUID()).thenReturn("iceberg_catalog.db.table");

        IcebergDeleteSink sink = new IcebergDeleteSink(icebergTable, desc, new SessionVariable());
        sink.init();

        assertFalse(sink.toThrift().getIceberg_table_sink().isSetParquet_encryption_info());
    }

    private static TupleDescriptor deleteTuple() {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0), "DeleteTuple");

        Column fileColumn = new Column(IcebergTable.FILE_PATH, VarcharType.VARCHAR);
        SlotDescriptor fileSlot = new SlotDescriptor(new SlotId(0), desc);
        fileSlot.setColumn(fileColumn);
        desc.addSlot(fileSlot);

        Column posColumn = new Column(IcebergTable.ROW_POSITION, IntegerType.BIGINT);
        SlotDescriptor posSlot = new SlotDescriptor(new SlotId(1), desc);
        posSlot.setColumn(posColumn);
        desc.addSlot(posSlot);

        return desc;
    }
}