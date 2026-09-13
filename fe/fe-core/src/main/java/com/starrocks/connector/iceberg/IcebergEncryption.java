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
import com.starrocks.thrift.TParquetEncryptionInfo;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.util.PropertyUtil;

import java.util.Map;

import static org.apache.iceberg.TableProperties.ENCRYPTION_DEK_LENGTH;
import static org.apache.iceberg.TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT;
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
 *
 * <p>Without a gate the write path fails <b>open</b>: {@code initEncryption} sees a plaintext manager,
 * skips encryption, and commits cleartext data files into a table whose properties promise encryption
 * -- no error anywhere. This is not specific to StarRocks; {@code OutputFileFactory.build()} and
 * {@code EncryptingFileIO.combine()} both take the manager from {@code table.encryption()}, so any
 * engine on an unwired catalog does the same. It is specific to us only in that we can refuse.
 *
 * <p>Read and write share one predicate on purpose. Letting them drift is how a table gets written in
 * a mode the read path will not serve, and the symmetry should be structural rather than two call
 * sites happening to agree.
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
     * Refuse to <b>write</b> a table that declares encryption the library will not apply.
     *
     * <p>Called from {@code IcebergTableSink.initEncryption}, i.e. at planning, so the statement fails
     * before any fragment reaches a BE and no file is produced. Writing anyway is the actual harm here:
     * the data lands in cleartext under a table configured to protect it, and nothing surfaces it.
     */
    public static void checkWriteSupported(Table nativeTable, String tableName) {
        if (!declaredButNotEnforced(nativeTable.encryption(), nativeTable.properties())) {
            return;
        }
        throw new StarRocksConnectorException(
                "refusing to write Iceberg table %s: the table declares encryption (%s is set) but the " +
                        "catalog supplied a plaintext encryption manager (%s), so the data files would be " +
                        "written unencrypted into a table configured to be encrypted. This means the " +
                        "catalog in use does not implement Iceberg table encryption; use a catalog that " +
                        "does.",
                tableName, ENCRYPTION_TABLE_KEY, PlaintextEncryptionManager.class.getSimpleName());
    }

    /**
     * Refuse to <b>read</b> a table that declares encryption the library will not honour.
     *
     * <p>Gated on the same predicate as writing, so the two cannot diverge. Reading is not a
     * confidentiality problem in itself -- whatever is on disk is already there -- but the two outcomes
     * are both wrong to serve quietly: if the files really are encrypted the scan gets no key and fails
     * deep in the BE with a decryption error that names nothing useful, and if they are plaintext then
     * the query silently succeeds against data the table promised was protected. Failing here says
     * which of those is happening.
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

    /** Parquet cipher for Iceberg Standard/PME data files. */
    private static final String DEFAULT_ALGORITHM = "AES_GCM_V1";

    /**
     * The FE→BE write signal for this table, or null when the table is not encrypted.
     *
     * <p>Every sink that writes files into an Iceberg table must use this, and there are three:
     * {@code IcebergTableSink} (INSERT), {@code IcebergDeleteSink} (DELETE) and
     * {@code IcebergRowDeltaSink} (UPDATE / MERGE). Deriving the signal in one place is the point --
     * when only the INSERT sink set it, DELETE/UPDATE/MERGE on an encrypted table wrote plaintext data
     * and plaintext position-delete files and committed them, with nothing surfacing it. A per-sink
     * copy of this logic is exactly how that recurs.
     *
     * <p>Carries the algorithm and the key length only, never key material: the BE generates a fresh
     * per-file DEK and returns it at commit so the FE can build the per-file {@code key_metadata}.
     * The length is resolved here because the FE is the only side that can see the table properties,
     * and it uses Iceberg's own constant and default so a StarRocks-written file matches what any other
     * Iceberg engine would produce.
     *
     * <p>Also runs {@link #checkWriteSupported} first, so an unsupported catalog fails the statement at
     * planning rather than writing cleartext into a table configured to protect it.
     *
     * <p>Keyed on the declaration, not on the manager's type, and it has to be: the commit path
     * ({@code IcebergMetadata.encryptionKeyMetadata}) builds {@code key_metadata} only when
     * {@code encryption.key-id} is present. Signalling on "the manager is not the plaintext one" instead
     * would send a signal for a table with no key-id whose catalog returned some other manager -- the BE
     * would encrypt, the commit would record no key, and the files would be unreadable by anything,
     * forever. The two sides must agree on which tables are encrypted, so both read the property.
     */
    public static TParquetEncryptionInfo writeSignalOrNull(Table nativeTable, String tableName) {
        checkWriteSupported(nativeTable, tableName);

        Map<String, String> tableProperties = nativeTable.properties();
        if (!declaresEncryption(tableProperties)) {
            return null;
        }

        TParquetEncryptionInfo encInfo = new TParquetEncryptionInfo();
        encInfo.setEncryption_algorithm(DEFAULT_ALGORITHM);
        encInfo.setDek_length(
                PropertyUtil.propertyAsInt(tableProperties, ENCRYPTION_DEK_LENGTH, ENCRYPTION_DEK_LENGTH_DEFAULT));
        return encInfo;
    }
}
