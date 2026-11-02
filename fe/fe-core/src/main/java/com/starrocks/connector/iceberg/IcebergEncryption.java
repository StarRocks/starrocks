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

import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;

import java.util.Map;

import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;

/**
 * Gates for the one encryption state StarRocks must not act on silently: a table that declares
 * encryption in its properties while the Iceberg library hands back a plaintext
 * {@link EncryptionManager}.
 *
 * <p>That combination is reachable because the two signals come from different places:
 * {@code encryption.key-id} is a table property and travels with the metadata for every catalog, while
 * the manager is built by the catalog's {@code TableOperations}. A catalog that has not implemented
 * {@code encryption()} inherits {@code TableOperations}' default of {@link PlaintextEncryptionManager}
 * and its tables report themselves unencrypted whatever their properties say. As of Iceberg 1.11.0
 * that is every catalog except Hive -- {@code RESTTableOperations} has no override, and
 * apache/iceberg#13225 is the pending fix -- but nothing here depends on which catalogs those are.
 *
 * <p>Deliberately expressed as a state, not a version or a catalog list: the gates read the manager
 * the library actually handed back, so when a catalog gains encryption support its tables start
 * passing with no change here.
 */
public final class IcebergEncryption {
    private IcebergEncryption() {}

    /**
     * Whether the table's properties declare encryption. This is the authoritative signal, and it is
     * the same one Iceberg itself uses: {@code EncryptionUtil.createEncryptionManager} returns
     * {@link PlaintextEncryptionManager} exactly when {@code encryption.key-id} is absent. Reading the
     * property rather than the manager is what makes the mismatch below detectable at all.
     */
    private static boolean declaresEncryption(Map<String, String> tableProperties) {
        return tableProperties != null && tableProperties.get(ENCRYPTION_TABLE_KEY) != null;
    }

    /**
     * True when the table claims encryption but the library will not encrypt.
     *
     * <p>Deliberately narrow. It fires only on the positively-identified mismatch, never on "the
     * manager is not one I recognise" -- an unfamiliar or custom manager is left exactly as it was, and
     * a table with no {@code encryption.key-id} is untouched. A wider predicate would refuse every
     * mock-backed Iceberg table in the FE suite, whose auto-mocked {@code encryption()} is neither
     * plaintext nor standard.
     */
    private static boolean declaredButNotEnforced(EncryptionManager encMgr, Map<String, String> tableProperties) {
        return declaresEncryption(tableProperties) && encMgr instanceof PlaintextEncryptionManager;
    }

    /**
     * Refuse to <b>read</b> a table that declares encryption the library will not honour.
     *
     * <p>Reading is not a confidentiality problem in itself -- whatever is on disk is already there --
     * but the two outcomes are both wrong to serve quietly: if the files really are encrypted the scan
     * gets no key and fails deep in the BE with a decryption error that names nothing useful, and if
     * they are plaintext then the query silently succeeds against data the table promised was
     * protected. Failing here says which of those is happening.
     */
    public static void checkReadSupported(Table nativeTable, String tableName) {
        if (!declaredButNotEnforced(nativeTable.encryption(), nativeTable.properties())) {
            return;
        }
        throw new StarRocksConnectorException(
                "cannot read Iceberg table %s: the table declares encryption (%s is set) but the catalog " +
                        "supplied a plaintext encryption manager (%s), so no data key can be recovered. " +
                        "This means the catalog in use does not implement Iceberg table encryption; use a " +
                        "catalog that does.",
                tableName, ENCRYPTION_TABLE_KEY, PlaintextEncryptionManager.class.getSimpleName());
    }
}
