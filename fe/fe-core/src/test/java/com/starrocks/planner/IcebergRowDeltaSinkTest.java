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
import com.starrocks.thrift.TIcebergTableSink;
import com.starrocks.thrift.TIcebergWriteMode;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * UPDATE and MERGE plan through {@link IcebergRowDeltaSink}, and it is the only sink that writes both
 * data files and position-delete files in one statement -- so a missed encryption signal here costs
 * the most. It had no test at all: the signal is derived in one shared place precisely so the three
 * sinks cannot drift, and the sink with no coverage was the one with the widest blast radius.
 */
public class IcebergRowDeltaSinkTest {

    private MockedStatic<IcebergUtil> mockedIcebergUtil;

    @BeforeEach
    public void setUp() {
        mockedIcebergUtil = mockStatic(IcebergUtil.class);
        CloudConfiguration mockCloudConfig = mock(CloudConfiguration.class);
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
        if (mockedIcebergUtil != null) {
            mockedIcebergUtil.close();
        }
    }

    @Test
    public void testEncryptedTableSendsTheEncryptionSignalToTheBackend() {
        IcebergTable icebergTable = rowDeltaTable(encryptedProperties("32"), new StubEncryptionManager());

        IcebergRowDeltaSink sink = new IcebergRowDeltaSink(icebergTable, rowDeltaTuple(), new SessionVariable(),
                TIcebergWriteMode.ROW_DELTA_UPDATE);
        sink.init();

        TIcebergTableSink icebergSink = sink.toThrift().getIceberg_table_sink();
        assertTrue(icebergSink.isSetParquet_encryption_info(),
                "UPDATE/MERGE on an encrypted table must tell the BE to encrypt both the data files "
                        + "and the position-delete files it writes");
        assertEquals(32, icebergSink.getParquet_encryption_info().getDek_length());
        assertEquals("AES_GCM_V1", icebergSink.getParquet_encryption_info().getEncryption_algorithm());
        // The BE generates its own per-file DEK and returns it at commit; FE never ships key material.
        assertFalse(icebergSink.getParquet_encryption_info().isSetFile_dek());
        assertFalse(icebergSink.getParquet_encryption_info().isSetAad_prefix());
    }

    @Test
    public void testDekLengthDefaultsToIcebergsValue() {
        IcebergTable icebergTable = rowDeltaTable(encryptedProperties(null), new StubEncryptionManager());

        IcebergRowDeltaSink sink = new IcebergRowDeltaSink(icebergTable, rowDeltaTuple(), new SessionVariable(),
                TIcebergWriteMode.ROW_DELTA_MIXED);
        sink.init();

        TIcebergTableSink icebergSink = sink.toThrift().getIceberg_table_sink();
        assertTrue(icebergSink.isSetParquet_encryption_info());
        assertEquals(TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT,
                icebergSink.getParquet_encryption_info().getDek_length());
    }

    @Test
    public void testUnencryptedTableSendsNoEncryptionSignal() {
        IcebergTable icebergTable = rowDeltaTable(new HashMap<>(), PlaintextEncryptionManager.instance());

        IcebergRowDeltaSink sink = new IcebergRowDeltaSink(icebergTable, rowDeltaTuple(), new SessionVariable(),
                TIcebergWriteMode.ROW_DELTA_UPDATE);
        sink.init();

        assertFalse(sink.toThrift().getIceberg_table_sink().isSetParquet_encryption_info());
    }

    /**
     * A table that declares encryption while the catalog hands back a plaintext manager must fail at
     * planning, before any fragment reaches a BE. Asserted at the sink rather than only on
     * {@code IcebergEncryption} so the gate is known to be reachable from this call site.
     */
    @Test
    public void testDeclaredEncryptionWithAPlaintextManagerIsRefusedAtPlanning() {
        IcebergTable icebergTable =
                rowDeltaTable(encryptedProperties("32"), PlaintextEncryptionManager.instance());

        StarRocksConnectorException e = assertThrows(StarRocksConnectorException.class,
                () -> new IcebergRowDeltaSink(icebergTable, rowDeltaTuple(), new SessionVariable(),
                        TIcebergWriteMode.ROW_DELTA_UPDATE));
        assertTrue(e.getMessage().contains("refusing to write"), e.getMessage());
    }

    /** A key-length property with no key-id must not be enough to turn encryption on. */
    @Test
    public void testKeyLengthWithoutAKeyIdSendsNoSignal() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.ENCRYPTION_DEK_LENGTH, "32");
        IcebergTable icebergTable = rowDeltaTable(props, new StubEncryptionManager());

        IcebergRowDeltaSink sink = new IcebergRowDeltaSink(icebergTable, rowDeltaTuple(), new SessionVariable(),
                TIcebergWriteMode.ROW_DELTA_UPDATE);
        sink.init();

        assertFalse(sink.toThrift().getIceberg_table_sink().isSetParquet_encryption_info(),
                "a table that does not declare encryption.key-id must never be signalled as encrypted");
    }

    private static Map<String, String> encryptedProperties(String dekLength) {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.ENCRYPTION_TABLE_KEY, "my-kms-key");
        if (dekLength != null) {
            props.put(TableProperties.ENCRYPTION_DEK_LENGTH, dekLength);
        }
        return props;
    }

    private static IcebergTable rowDeltaTable(Map<String, String> properties,
                                              org.apache.iceberg.encryption.EncryptionManager encryptionManager) {
        IcebergTable icebergTable = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        when(icebergTable.getNativeTable()).thenReturn(nativeTable);
        when(nativeTable.location()).thenReturn("/tmp/iceberg");
        when(icebergTable.getUUID()).thenReturn("iceberg_catalog.db.table");
        when(nativeTable.properties()).thenReturn(properties);
        // Stubbed explicitly: an unstubbed encryption() returns null, and `null instanceof
        // PlaintextEncryptionManager` is false, so the refusal gate would be silently skipped and a
        // test asserting a signal would establish nothing about it.
        when(nativeTable.encryption()).thenReturn(encryptionManager);
        return icebergTable;
    }

    /** Row delta needs _file and _pos to identify the rows being replaced, plus a data column. */
    private static TupleDescriptor rowDeltaTuple() {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0), "RowDeltaTuple");

        Column fileColumn = new Column(IcebergTable.FILE_PATH, VarcharType.VARCHAR);
        SlotDescriptor fileSlot = new SlotDescriptor(new SlotId(0), desc);
        fileSlot.setColumn(fileColumn);
        desc.addSlot(fileSlot);

        Column posColumn = new Column(IcebergTable.ROW_POSITION, IntegerType.BIGINT);
        SlotDescriptor posSlot = new SlotDescriptor(new SlotId(1), desc);
        posSlot.setColumn(posColumn);
        desc.addSlot(posSlot);

        Column dataColumn = new Column("c1", IntegerType.INT);
        SlotDescriptor dataSlot = new SlotDescriptor(new SlotId(2), desc);
        dataSlot.setColumn(dataColumn);
        desc.addSlot(dataSlot);

        return desc;
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
}
