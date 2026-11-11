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

// language: java
package com.starrocks.planner;

import com.google.common.collect.Maps;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TIcebergTableSink;
import com.starrocks.thrift.TParquetEncryptionInfo;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;

public class IcebergTableSinkTest {

    @Test
    public void testCompressionFromNativeTableProperty(@Mocked GlobalStateMgr globalStateMgr, @Mocked ConnectorMgr connectorMgr,
                                                       @Mocked CatalogConnector connector, @Mocked IcebergTable icebergTable,
                                                       @Mocked Table nativeTable, @Mocked FileIO fileIO,
                                                       @Mocked SessionVariable sessionVariable) {
        // nativeTable.properties contains parquet compression -> should use it
        Map<String, String> nativeProps = Maps.newHashMap();
        nativeProps.put(PARQUET_COMPRESSION, "zstd");

        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(new HashMap<>());

        new Expectations() {
            {
                // iceberg table / native table basic
                icebergTable.getNativeTable();
                result = nativeTable;

                icebergTable.getCatalogName();
                result = "catA";

                icebergTable.getId();
                result = 101L;

                icebergTable.getUUID();
                result = "uuidA";

                nativeTable.location();
                result = "s3://bucket/a";

                nativeTable.properties();
                result = nativeProps;

                nativeTable.io();
                result = fileIO;

                fileIO.properties();
                result = new HashMap<String, String>();

                // Global state and connector metadata fallback path
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getConnectorMgr();
                result = connectorMgr;

                connectorMgr.getConnector("catA");
                result = connector;

                connector.getMetadata().getCloudConfiguration();
                result = cc;

                // session variable should not be used for compression in this case, but stub anyway
                sessionVariable.getConnectorSinkCompressionCodec();
                result = "gzip";

                sessionVariable.getConnectorSinkTargetMaxFileSize();
                result = 0L;
            }
        };

        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        IcebergTableSink sink = new IcebergTableSink(icebergTable, desc, false, sessionVariable, null);
        TDataSink t = sink.toThrift();
        // compression_type should map "zstd" -> TCompressionType.ZSTD
        Assertions.assertEquals(TCompressionType.ZSTD, t.getIceberg_table_sink().getCompression_type());
    }

    @Test
    public void testCompressionFromSessionVariableFallback(@Mocked GlobalStateMgr globalStateMgr,
                                                           @Mocked ConnectorMgr connectorMgr, @Mocked CatalogConnector connector,
                                                           @Mocked IcebergTable icebergTable, @Mocked Table nativeTable,
                                                           @Mocked FileIO fileIO, @Mocked SessionVariable sessionVariable) {
        // nativeTable.properties does NOT contain parquet compression -> should fallback to sessionVariable
        Map<String, String> nativeProps = Maps.newHashMap(); // empty

        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(new HashMap<>());

        new Expectations() {
            {
                icebergTable.getNativeTable();
                result = nativeTable;

                icebergTable.getCatalogName();
                result = "catB";

                icebergTable.getId();
                result = 102L;

                icebergTable.getUUID();
                result = "uuidB";

                nativeTable.location();
                result = "s3://bucket/b";

                nativeTable.properties();
                result = nativeProps;

                nativeTable.io();
                result = fileIO;

                fileIO.properties();
                result = new HashMap<String, String>();

                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getConnectorMgr();
                result = connectorMgr;

                connectorMgr.getConnector("catB");
                result = connector;

                connector.getMetadata().getCloudConfiguration();
                result = cc;

                // session variable provides fallback compression codec
                sessionVariable.getConnectorSinkCompressionCodec();
                result = "gzip";
                sessionVariable.getConnectorSinkTargetMaxFileSize();
                result = 0L;
            }
        };

        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        IcebergTableSink sink = new IcebergTableSink(icebergTable, desc, false, sessionVariable, null);
        TDataSink t = sink.toThrift();
        // fallback "gzip" -> TCompressionType.GZIP
        Assertions.assertEquals(TCompressionType.GZIP, t.getIceberg_table_sink().getCompression_type());
    }

    /**
     * Minimal non-plaintext EncryptionManager. StandardEncryptionManager needs a KMS client, and none
     * of these tests reach a KMS -- initEncryption only asks "is this the plaintext manager?", so any
     * other implementation exercises the encrypted branch.
     */
    private static class StubEncryptionManager implements EncryptionManager {
        @Override
        public InputFile decrypt(EncryptedInputFile encrypted) {
            throw new UnsupportedOperationException("not reached: FE never decrypts on the write path");
        }

        @Override
        public EncryptedOutputFile encrypt(OutputFile rawOutput) {
            throw new UnsupportedOperationException("not reached: BE does the encrypting");
        }
    }

    private TIcebergTableSink encryptedSinkThrift(Map<String, String> nativeProps,
                                                 EncryptionManager encryptionManager,
                                                 GlobalStateMgr globalStateMgr, ConnectorMgr connectorMgr,
                                                 CatalogConnector connector, IcebergTable icebergTable,
                                                 Table nativeTable, FileIO fileIO,
                                                 SessionVariable sessionVariable) {
        CloudConfiguration cc = CloudConfigurationFactory.buildCloudConfigurationForStorage(new HashMap<>());
        new Expectations() {
            {
                icebergTable.getNativeTable();
                result = nativeTable;
                icebergTable.getCatalogName();
                result = "catEnc";
                icebergTable.getId();
                result = 200L;
                icebergTable.getUUID();
                result = "uuidEnc";

                nativeTable.location();
                result = "s3://bucket/enc";
                nativeTable.properties();
                result = nativeProps;
                nativeTable.io();
                result = fileIO;
                nativeTable.encryption();
                result = encryptionManager;

                fileIO.properties();
                result = new HashMap<String, String>();

                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getConnectorMgr();
                result = connectorMgr;
                connectorMgr.getConnector("catEnc");
                result = connector;
                connector.getMetadata().getCloudConfiguration();
                result = cc;

                sessionVariable.getConnectorSinkCompressionCodec();
                result = "gzip";
                sessionVariable.getConnectorSinkTargetMaxFileSize();
                result = 0L;
            }
        };

        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        return new IcebergTableSink(icebergTable, desc, false, sessionVariable, null)
                .toThrift().getIceberg_table_sink();
    }

    @Test
    public void testEncryptionSignalCarriesTableDekLengthAndNoKeyMaterial(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked ConnectorMgr connectorMgr,
            @Mocked CatalogConnector connector, @Mocked IcebergTable icebergTable, @Mocked Table nativeTable,
            @Mocked FileIO fileIO, @Mocked SessionVariable sessionVariable) {
        Map<String, String> props = Maps.newHashMap();
        props.put(TableProperties.ENCRYPTION_TABLE_KEY, "my-kms-key");
        props.put(TableProperties.ENCRYPTION_DEK_LENGTH, "32");

        TIcebergTableSink sink = encryptedSinkThrift(props, new StubEncryptionManager(), globalStateMgr,
                connectorMgr, connector, icebergTable, nativeTable, fileIO, sessionVariable);

        Assertions.assertTrue(sink.isSetParquet_encryption_info(), "an encrypted table must signal encryption to BE");
        TParquetEncryptionInfo enc = sink.getParquet_encryption_info();
        Assertions.assertEquals(32, enc.getDek_length(), "the table's encryption.data-key-length must reach BE");
        Assertions.assertEquals("AES_GCM_V1", enc.getEncryption_algorithm());
        // The whole point of the write protocol: FE says how long a key to make and never sends one.
        // Key material on this leg would mean the DEK was generated somewhere it should not have been.
        Assertions.assertFalse(enc.isSetFile_dek(), "FE must never send key material on the write path");
        Assertions.assertFalse(enc.isSetAad_prefix(), "FE must not send an AAD prefix on the write path");
    }

    @Test
    public void testEncryptionSignalDefaultsDekLengthWhenPropertyAbsent(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked ConnectorMgr connectorMgr,
            @Mocked CatalogConnector connector, @Mocked IcebergTable icebergTable, @Mocked Table nativeTable,
            @Mocked FileIO fileIO, @Mocked SessionVariable sessionVariable) {
        // No encryption.data-key-length on the table. FE is the only side that can see the table, so
        // FE is the only side that may default it -- and it defaults to Iceberg's own value, so the
        // file matches what another Iceberg engine would have written.
        Map<String, String> props = Maps.newHashMap();
        props.put(TableProperties.ENCRYPTION_TABLE_KEY, "my-kms-key");

        TIcebergTableSink sink = encryptedSinkThrift(props, new StubEncryptionManager(), globalStateMgr,
                connectorMgr, connector, icebergTable, nativeTable, fileIO, sessionVariable);

        Assertions.assertTrue(sink.isSetParquet_encryption_info());
        Assertions.assertEquals(TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT,
                sink.getParquet_encryption_info().getDek_length());
        // Guards the case that used to let BE silently pick a length for itself.
        Assertions.assertTrue(sink.getParquet_encryption_info().isSetDek_length(),
                "dek_length must always be populated when encryption is on");
    }

    @Test
    public void testPlaintextTableSendsNoEncryptionInfo(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked ConnectorMgr connectorMgr,
            @Mocked CatalogConnector connector, @Mocked IcebergTable icebergTable, @Mocked Table nativeTable,
            @Mocked FileIO fileIO, @Mocked SessionVariable sessionVariable) {
        // An unencrypted table must stay unencrypted: a stray signal here would encrypt a table whose
        // readers have no key for it. The key-length property is present on purpose -- it must not be
        // what decides.
        Map<String, String> props = Maps.newHashMap();
        props.put(TableProperties.ENCRYPTION_DEK_LENGTH, "32");

        TIcebergTableSink sink = encryptedSinkThrift(props, PlaintextEncryptionManager.instance(), globalStateMgr,
                connectorMgr, connector, icebergTable, nativeTable, fileIO, sessionVariable);

        Assertions.assertFalse(sink.isSetParquet_encryption_info(),
                "a plaintext table must not signal encryption even when it carries a key-length property");
    }

    @Test
    public void testUnparseableDekLengthFailsRatherThanFallingBackToTheDefault(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked ConnectorMgr connectorMgr,
            @Mocked CatalogConnector connector, @Mocked IcebergTable icebergTable, @Mocked Table nativeTable,
            @Mocked FileIO fileIO, @Mocked SessionVariable sessionVariable) {
        // PropertyUtil.propertyAsInt falls back to the default only when the property is ABSENT; a
        // present-but-unparseable value throws. That is the safe direction: silently reading
        // "thirty-two" as 16 would write a weaker key than the table asked for. It does not trim
        // either, so " 32 " throws too -- matching Iceberg, which rejects the same configuration.
        Map<String, String> props = Maps.newHashMap();
        props.put(TableProperties.ENCRYPTION_TABLE_KEY, "my-kms-key");
        props.put(TableProperties.ENCRYPTION_DEK_LENGTH, "thirty-two");

        Assertions.assertThrows(NumberFormatException.class,
                () -> encryptedSinkThrift(props, new StubEncryptionManager(), globalStateMgr, connectorMgr,
                        connector, icebergTable, nativeTable, fileIO, sessionVariable));
    }

    @Test
    public void testTableWithoutAKeyIdSendsNoEncryptionInfoWhateverTheManagerIs(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked ConnectorMgr connectorMgr,
            @Mocked CatalogConnector connector, @Mocked IcebergTable icebergTable, @Mocked Table nativeTable,
            @Mocked FileIO fileIO, @Mocked SessionVariable sessionVariable) {
        // encryption.key-id is what makes a table encrypted, and it is what the commit path reads:
        // IcebergMetadata.encryptionKeyMetadata builds key_metadata only when the property is present.
        // Keying this signal off the manager's type instead would tell the BE to encrypt a table whose
        // commit records no key, leaving files nothing can ever decrypt. A key-length property with no
        // key-id must not be enough to turn encryption on.
        Map<String, String> props = Maps.newHashMap();
        props.put(TableProperties.ENCRYPTION_DEK_LENGTH, "32");

        TIcebergTableSink sink = encryptedSinkThrift(props, new StubEncryptionManager(), globalStateMgr,
                connectorMgr, connector, icebergTable, nativeTable, fileIO, sessionVariable);

        Assertions.assertFalse(sink.isSetParquet_encryption_info(),
                "a table that does not declare encryption.key-id must never be signalled as encrypted");
    }
}
