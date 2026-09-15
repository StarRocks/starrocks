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

import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TIcebergTableSink;
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
 * The INSERT sink's encryption path, tested through Mockito rather than JMockit.
 *
 * <p>{@link IcebergTableSinkTest} already covers this logic and passes, but it is a JMockit test, and
 * JMockit's instrumentation stops JaCoCo recording coverage for the classes it touches — so the
 * {@code toThrift()} encryption branch showed as uncovered while the Mockito-tested DELETE and
 * UPDATE/MERGE sinks did not. That is not cosmetic: with no recorded coverage, the coverage signal
 * cannot catch a regression in these lines. Same assertions, a framework JaCoCo can see through.
 */
public class IcebergTableSinkEncryptionThriftTest {

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
        // resolveTargetMaxFileSize and tableDataLocation are static on the same class, so the static
        // mock intercepts them too; give them values toThrift() can serialize.
        when(IcebergUtil.tableDataLocation(any(org.apache.iceberg.Table.class))).thenReturn("/tmp/iceberg/data");
        when(IcebergUtil.resolveTargetMaxFileSize(any(org.apache.iceberg.Table.class), any(SessionVariable.class)))
                .thenReturn(1024L * 1024L * 1024L);
    }

    @AfterEach
    public void tearDown() {
        if (mockedIcebergUtil != null) {
            mockedIcebergUtil.close();
        }
    }

    @Test
    public void testEncryptedTableSetsParquetEncryptionInfoOnThrift() {
        IcebergTableSink sink = sinkFor(encryptedProperties("32"), new StubEncryptionManager());

        TIcebergTableSink t = sink.toThrift().getIceberg_table_sink();
        assertTrue(t.isSetParquet_encryption_info(),
                "an INSERT into an encrypted table must tell the BE to encrypt the files it writes");
        assertEquals(32, t.getParquet_encryption_info().getDek_length());
        assertEquals("AES_GCM_V1", t.getParquet_encryption_info().getEncryption_algorithm());
        // The BE generates its own per-file DEK and returns it at commit; FE never ships key material.
        assertFalse(t.getParquet_encryption_info().isSetFile_dek());
        assertFalse(t.getParquet_encryption_info().isSetAad_prefix());
    }

    @Test
    public void testDekLengthDefaultsToIcebergsValue() {
        IcebergTableSink sink = sinkFor(encryptedProperties(null), new StubEncryptionManager());

        TIcebergTableSink t = sink.toThrift().getIceberg_table_sink();
        assertTrue(t.isSetParquet_encryption_info());
        assertEquals(TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT,
                t.getParquet_encryption_info().getDek_length());
    }

    /** The common case: the branch must be skipped, not set with an empty struct. */
    @Test
    public void testUnencryptedTableSetsNoEncryptionInfoOnThrift() {
        IcebergTableSink sink = sinkFor(new HashMap<>(), PlaintextEncryptionManager.instance());

        assertFalse(sink.toThrift().getIceberg_table_sink().isSetParquet_encryption_info());
    }

    /** A key-length property with no key-id must not be enough to turn encryption on. */
    @Test
    public void testKeyLengthWithoutAKeyIdSetsNoEncryptionInfo() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.ENCRYPTION_DEK_LENGTH, "32");

        IcebergTableSink sink = sinkFor(props, new StubEncryptionManager());
        assertFalse(sink.toThrift().getIceberg_table_sink().isSetParquet_encryption_info(),
                "a table that does not declare encryption.key-id must never be signalled as encrypted");
    }

    /** Declared-but-unenforceable must fail at planning, before any fragment reaches a BE. */
    @Test
    public void testDeclaredEncryptionWithAPlaintextManagerIsRefusedAtPlanning() {
        StarRocksConnectorException e = assertThrows(StarRocksConnectorException.class,
                () -> sinkFor(encryptedProperties("32"), PlaintextEncryptionManager.instance()));
        assertTrue(e.getMessage().contains("refusing to write"), e.getMessage());
    }

    private static Map<String, String> encryptedProperties(String dekLength) {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.ENCRYPTION_TABLE_KEY, "my-kms-key");
        if (dekLength != null) {
            props.put(TableProperties.ENCRYPTION_DEK_LENGTH, dekLength);
        }
        return props;
    }

    private static IcebergTableSink sinkFor(Map<String, String> properties,
                                            org.apache.iceberg.encryption.EncryptionManager encryptionManager) {
        IcebergTable icebergTable = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        when(icebergTable.getNativeTable()).thenReturn(nativeTable);
        when(icebergTable.getId()).thenReturn(200L);
        when(icebergTable.getUUID()).thenReturn("iceberg_catalog.db.table");
        when(icebergTable.getCatalogName()).thenReturn("iceberg_catalog");
        when(nativeTable.location()).thenReturn("/tmp/iceberg");
        when(nativeTable.properties()).thenReturn(properties);
        // Stubbed explicitly: an unstubbed encryption() returns null, and the plaintext-manager test
        // is false for null, so the refusal gate would be skipped and the assertions would establish
        // nothing about it.
        when(nativeTable.encryption()).thenReturn(encryptionManager);

        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        return new IcebergTableSink(icebergTable, desc, false, new SessionVariable(), null);
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
