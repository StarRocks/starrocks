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

import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;

/**
 * The state under test is a table whose properties declare encryption while the catalog hands back a
 * plaintext manager -- reachable on any catalog Iceberg 1.11.0 has not wired, REST included.
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
    public void testDeclaredEncryptionWithPlaintextManagerIsRefused(@Mocked Table nativeTable) {
        // The whole point: key-id present, manager plaintext. No data key can be recovered, so the scan
        // must not proceed as though the table were plaintext.
        stub(nativeTable, PlaintextEncryptionManager.instance(),
                ImmutableMap.of(ENCRYPTION_TABLE_KEY, "my-kms-key"));

        StarRocksConnectorException r = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> IcebergEncryption.checkReadSupported(nativeTable, "db.t"));
        Assertions.assertTrue(r.getMessage().contains("cannot read"), r.getMessage());
    }

    @Test
    public void testPlaintextTableWithoutKeyIdIsAllowed(@Mocked Table nativeTable) {
        // The overwhelmingly common case. A plaintext manager is only suspicious when the table claims
        // otherwise; on its own it just means the table is not encrypted.
        stub(nativeTable, PlaintextEncryptionManager.instance(), ImmutableMap.of());

        IcebergEncryption.checkReadSupported(nativeTable, "db.t");
    }

    @Test
    public void testDeclaredEncryptionWithARealManagerIsAllowed(@Mocked Table nativeTable) {
        // A wired catalog: key-id set and a non-plaintext manager. Nothing to refuse.
        stub(nativeTable, new StubEncryptionManager(), ImmutableMap.of(ENCRYPTION_TABLE_KEY, "my-kms-key"));

        IcebergEncryption.checkReadSupported(nativeTable, "db.t");
    }

    @Test
    public void testUnfamiliarManagerWithoutKeyIdIsNotRefused(@Mocked Table nativeTable) {
        // The predicate must stay narrow. "Not a manager I recognise" is not grounds for refusal --
        // a wider version would reject every mock-backed Iceberg table in this suite, whose auto-mocked
        // encryption() is neither plaintext nor standard.
        stub(nativeTable, new StubEncryptionManager(), ImmutableMap.of());

        IcebergEncryption.checkReadSupported(nativeTable, "db.t");
    }

    @Test
    public void testNullPropertiesAreTreatedAsNoDeclaration(@Mocked Table nativeTable) {
        // Defensive: a table whose properties come back null must not throw here. Failing closed would
        // mean refusing tables for a reason unrelated to encryption.
        stub(nativeTable, PlaintextEncryptionManager.instance(), null);

        IcebergEncryption.checkReadSupported(nativeTable, "db.t");
    }
}
