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

package com.starrocks.connector.iceberg;

import com.google.common.collect.ImmutableMap;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.thrift.TParquetEncryptionInfo;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.iceberg.TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT;
import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;

/**
 * The state under test is a table whose properties declare encryption while the catalog hands back a
 * plaintext manager -- reachable on any catalog Iceberg 1.11.0 has not wired, REST included. Both gates
 * share one predicate, so each case is asserted on both to keep read and write from drifting.
 */
public class IcebergEncryptionTest {

    /** Non-plaintext, non-standard manager: stands in for a custom or unfamiliar implementation. */
    private static class StubEncryptionManager implements EncryptionManager {
        @Override
        public InputFile decrypt(EncryptedInputFile encrypted) {
            throw new UnsupportedOperationException("not reached");
        }

        @Override
        public EncryptedOutputFile encrypt(OutputFile rawOutput) {
            throw new UnsupportedOperationException("not reached");
        }
    }

    private void stub(Table nativeTable, EncryptionManager manager, Map<String, String> properties) {
        new Expectations() {
            {
                nativeTable.encryption();
                result = manager;
                minTimes = 0;
                nativeTable.properties();
                result = properties;
                minTimes = 0;
            }
        };
    }

    @Test
    public void testDeclaredEncryptionWithPlaintextManagerIsRefusedOnBothPaths(@Mocked Table nativeTable) {
        // The whole point: key-id present, manager plaintext. Writing would commit cleartext into a
        // table configured to be encrypted, with nothing surfacing it.
        stub(nativeTable, PlaintextEncryptionManager.instance(),
                ImmutableMap.of(ENCRYPTION_TABLE_KEY, "my-kms-key"));

        StarRocksConnectorException w = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> IcebergEncryption.checkWriteSupported(nativeTable, "db.t"));
        Assertions.assertTrue(w.getMessage().contains("refusing to write"), w.getMessage());

        StarRocksConnectorException r = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> IcebergEncryption.checkReadSupported(nativeTable, "db.t"));
        Assertions.assertTrue(r.getMessage().contains("cannot read"), r.getMessage());
    }

    @Test
    public void testPlaintextTableWithoutKeyIdIsAllowed(@Mocked Table nativeTable) {
        // The overwhelmingly common case. A plaintext manager is only suspicious when the table claims
        // otherwise; on its own it just means the table is not encrypted.
        stub(nativeTable, PlaintextEncryptionManager.instance(), ImmutableMap.of());

        IcebergEncryption.checkWriteSupported(nativeTable, "db.t");
        IcebergEncryption.checkReadSupported(nativeTable, "db.t");
    }

    @Test
    public void testDeclaredEncryptionWithARealManagerIsAllowed(@Mocked Table nativeTable) {
        // A wired catalog: key-id set and a non-plaintext manager. Nothing to refuse.
        stub(nativeTable, new StubEncryptionManager(), ImmutableMap.of(ENCRYPTION_TABLE_KEY, "my-kms-key"));

        IcebergEncryption.checkWriteSupported(nativeTable, "db.t");
        IcebergEncryption.checkReadSupported(nativeTable, "db.t");
    }

    @Test
    public void testUnfamiliarManagerWithoutKeyIdIsNotRefused(@Mocked Table nativeTable) {
        // The predicate must stay narrow. "Not a manager I recognise" is not grounds for refusal --
        // a wider version would reject every mock-backed Iceberg table in this suite, whose auto-mocked
        // encryption() is neither plaintext nor standard.
        stub(nativeTable, new StubEncryptionManager(), ImmutableMap.of());

        IcebergEncryption.checkWriteSupported(nativeTable, "db.t");
        IcebergEncryption.checkReadSupported(nativeTable, "db.t");
    }

    @Test
    public void testNullPropertiesAreTreatedAsNoDeclaration(@Mocked Table nativeTable) {
        // Defensive: a table whose properties come back null must not throw here. Failing closed would
        // mean refusing tables for a reason unrelated to encryption.
        stub(nativeTable, PlaintextEncryptionManager.instance(), null);

        IcebergEncryption.checkWriteSupported(nativeTable, "db.t");
        IcebergEncryption.checkReadSupported(nativeTable, "db.t");
    }

    @Test
    public void testWriteSignalCarriesPolicyAndNoKeyMaterial(@Mocked Table nativeTable) {
        // All three write sinks -- IcebergTableSink (INSERT), IcebergDeleteSink (DELETE) and
        // IcebergRowDeltaSink (UPDATE/MERGE) -- derive their signal here. When only the INSERT sink
        // built one, DELETE/UPDATE/MERGE wrote plaintext data and plaintext position-delete files into
        // encrypted tables. This is the single point that keeps them consistent.
        stub(nativeTable, new StubEncryptionManager(),
                ImmutableMap.of(ENCRYPTION_TABLE_KEY, "k", "encryption.data-key-length", "32"));

        TParquetEncryptionInfo enc = IcebergEncryption.writeSignalOrNull(nativeTable, "db.t");

        Assertions.assertNotNull(enc);
        Assertions.assertEquals(32, enc.getDek_length());
        Assertions.assertEquals("AES_GCM_V1", enc.getEncryption_algorithm());
        Assertions.assertFalse(enc.isSetFile_dek(), "FE must never send key material on the write path");
        Assertions.assertFalse(enc.isSetAad_prefix(), "FE must not send an AAD prefix on the write path");
    }

    @Test
    public void testWriteSignalDefaultsKeyLengthToIcebergsDefault(@Mocked Table nativeTable) {
        stub(nativeTable, new StubEncryptionManager(), ImmutableMap.of(ENCRYPTION_TABLE_KEY, "k"));

        TParquetEncryptionInfo enc = IcebergEncryption.writeSignalOrNull(nativeTable, "db.t");

        Assertions.assertNotNull(enc);
        Assertions.assertEquals(ENCRYPTION_DEK_LENGTH_DEFAULT, enc.getDek_length());
        Assertions.assertTrue(enc.isSetDek_length(), "dek_length must always be populated when encryption is on");
    }

    @Test
    public void testWriteSignalIsNullForAnUnencryptedTable(@Mocked Table nativeTable) {
        // The common case: no signal at all, so the BE writes plaintext as before.
        stub(nativeTable, PlaintextEncryptionManager.instance(), ImmutableMap.of());

        Assertions.assertNull(IcebergEncryption.writeSignalOrNull(nativeTable, "db.t"));
    }

    @Test
    public void testWriteSignalIsNullWhenNoKeyIdIsDeclaredWhateverTheManagerIs(@Mocked Table nativeTable) {
        // No encryption.key-id, but the catalog handed back some manager that is not the plaintext one.
        // Keying the signal off the manager's type would tell the BE to encrypt here, while
        // IcebergMetadata.encryptionKeyMetadata -- which gates on the property -- would commit the file
        // with no key_metadata. The DEK would exist only in the BE process that made it, so the data
        // would be unreadable by StarRocks or anything else, permanently and without an error.
        stub(nativeTable, new StubEncryptionManager(), ImmutableMap.of("write.format.default", "parquet"));

        Assertions.assertNull(IcebergEncryption.writeSignalOrNull(nativeTable, "db.t"),
                "a table that does not declare encryption must never be signalled as encrypted");
    }

    @Test
    public void testWriteSignalTreatsNullPropertiesAsNoDeclaration(@Mocked Table nativeTable) {
        // properties() is null for a table whose metadata has not been loaded, and for every
        // Mockito-mocked table in the sink tests. Reading the DEK length before establishing that the
        // table declares encryption threw NPE out of the sink constructors, which is a planning-time
        // failure for DELETE/UPDATE/MERGE against any such table.
        stub(nativeTable, new StubEncryptionManager(), null);

        Assertions.assertNull(IcebergEncryption.writeSignalOrNull(nativeTable, "db.t"));
    }
}
