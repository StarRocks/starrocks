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

package com.starrocks.epack.connector.lakeformation;

import com.starrocks.catalog.Column;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.connector.hive.glue.projection.PartitionProjectionProperties;
import com.starrocks.connector.unified.UnifiedMetadata;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * The fail-closed allowlist for Lake Formation governed Hive tables.
 *
 * It runs on the Lake Formation response, before the table can reach hmsOps.getTable. That ordering is load
 * bearing for two reasons, neither of them about avro: HiveMetastore.getTable calls the Glue client directly
 * with the catalog's own credentials, and its result is put into CachingHiveMetastore's cross-query tableCache,
 * where the next principal would hit an untrimmed physical schema.
 *
 * apiTable is the Hive model produced by CatalogToHiveConverter.convertTable - the caller converts once and
 * hands the same object to both this check and toHiveTable, so what was validated is what gets built.
 *
 * Every table-shape predicate here is the one HiveMetastore.getTable already uses. Writing a second set of
 * literals would let this check and the branch that actually decides engine behaviour drift apart.
 */
public final class LakeFormationTableGuard {
    private static final Logger LOG = LogManager.getLogger(LakeFormationTableGuard.class);

    // No constant for this exists anywhere in the repo; symlink tables are how Delta is exposed through Hive.
    private static final String SYMLINK_TEXT_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat";
    private static final String VIRTUAL_VIEW = "VIRTUAL_VIEW";

    private LakeFormationTableGuard() {
    }

    public static void check(AuthorizedTableMetadata metadata, Table apiTable,
                             LakeFormationTableIdentity identity) {
        if (metadata.hasFilters()) {
            // Unreachable in phase 1: StarRocks advertises COLUMN_PERMISSION only, so Lake Formation raises
            // PermissionTypeMismatchException before it ever returns a filter. Reaching here means what we
            // advertise and what Lake Formation enforces have drifted apart, which is a correctness bug
            // rather than a user error. Whoever adds CELL_FILTER_PERMISSION back to the gateway turns this
            // from a backstop into the only line of defence.
            throw new LakeFormationTableAccessException(
                    "Lake Formation returned a row or cell filter for " + identity
                            + " even though StarRocks only advertises column level permissions."
                            + " Refusing to plan this table.");
        }

        if (VIRTUAL_VIEW.equalsIgnoreCase(apiTable.getTableType())) {
            throw unsupported(identity, "Hive views are not supported on a Lake Formation catalog in this version");
        }

        // Non-null by construction: CatalogToHiveConverter.convertTable throws when Glue returned no
        // storage descriptor, and it is the only way an apiTable reaches this method.
        StorageDescriptor sd = apiTable.getSd();

        Map<String, String> parameters = apiTable.getParameters() == null ? new HashMap<>() : apiTable.getParameters();
        // The same predicate HiveMetastoreOperations uses to bypass Glue entirely. Projection tables enumerate
        // partitions from storage.location.template, which may point anywhere, so a table level credential
        // provably cannot cover them.
        if (PartitionProjectionProperties.isProjectionEnabled(parameters)) {
            throw unsupported(identity, "Glue partition projection is enabled on this table (projection.enabled)");
        }

        // Iceberg and Delta announce themselves in the table parameters, not in the storage descriptor: a
        // Delta table can carry an ordinary Parquet inputFormat and serde and would otherwise be taken for a
        // plain Hive table and read as one, which its layout is not. Checked ahead of the format allowlist so
        // the user is told the real reason rather than "not Parquet".
        if (UnifiedMetadata.isIcebergTable(parameters)) {
            throw unsupported(identity, "Iceberg tables are not supported on a Lake Formation catalog in"
                    + " this version");
        }
        if (UnifiedMetadata.isDeltaLakeTable(parameters)) {
            throw unsupported(identity, "Delta Lake tables are not supported on a Lake Formation catalog in"
                    + " this version");
        }

        String inputFormat = sd.getInputFormat();
        // The other way Delta reaches Hive: a symlink manifest, which is visible in the storage descriptor.
        if (SYMLINK_TEXT_INPUT_FORMAT.equals(inputFormat)) {
            throw unsupported(identity, "symlink manifest tables (typically Delta exposed through Hive) are not"
                    + " supported on a Lake Formation catalog in this version");
        }
        if (HiveMetastoreApiConverter.isHudiTable(inputFormat)) {
            throw unsupported(identity, "Hudi tables are not supported on a Lake Formation catalog in this version");
        }
        if (HiveMetastoreApiConverter.isKuduTable(inputFormat)) {
            throw unsupported(identity, "Kudu tables are not supported on a Lake Formation catalog in this version");
        }

        // Wrapped so metadata enumeration can leave a table with an unsupported type out instead of failing.
        try {
            HiveMetastoreApiConverter.validateHiveTableType(apiTable.getTableType());
        } catch (RuntimeException e) {
            throw LakeFormationTableAccessException.nothingToDescribe(
                    "Cannot query " + identity + ": " + e.getMessage(), e);
        }

        // Checked explicitly even though the Parquet allowlist below already excludes ACID tables, which are
        // ORC: without this a user on a transactional table would only be told "not Parquet", which is true
        // but not the reason.
        if (AcidUtils.isFullAcidTable(apiTable)) {
            throw unsupported(identity, "full ACID transactional tables are not supported on a Lake Formation"
                    + " catalog in this version");
        }
        if (apiTable.getParameters() != null && AcidUtils.isInsertOnlyTable(apiTable.getParameters())) {
            throw unsupported(identity, "insert-only transactional tables are not supported on a Lake Formation"
                    + " catalog in this version");
        }

        if (!isSupportedStorageFormat(sd)) {
            String serde = sd.getSerdeInfo() == null ? null : sd.getSerdeInfo().getSerializationLib();
            throw unsupported(identity, "only Parquet is supported on a Lake Formation catalog in this version"
                    + " (inputFormat=" + inputFormat + ", serde=" + serde + ")");
        }
    }

    /** Input format and serde both Parquet; reused by the partition level check so the two cannot differ. */
    static boolean isSupportedStorageFormat(StorageDescriptor sd) {
        if (sd == null) {
            return false;
        }
        String inputFormat = sd.getInputFormat();
        String serde = sd.getSerdeInfo() == null ? null : sd.getSerdeInfo().getSerializationLib();
        return HiveStorageFormat.get(String.valueOf(inputFormat), String.valueOf(serde))
                == HiveStorageFormat.PARQUET;
    }

    /**
     * Every physical partition column has to be authorized, or the table is refused outright.
     *
     * Called after the schema projection, with the projected columns, so that the names being compared have
     * already been resolved against the physical schema exactly once.
     *
     * Why refuse rather than hide: the planner assumes the partition getter, the HMS partition values and the
     * PartitionKey are all the physical spec. Narrowing fullSchema breaks that invariant, and no choice of
     * getter puts it back - returning the authorized subset fails the size check inside createPartitionKey,
     * returning the physical list hands a null ColumnRef to a concurrent map, and not overriding at all
     * produces a list with null elements.
     *
     * The message deliberately does not say which column. Naming it would confirm to a user without access
     * that the column exists, that it is a partition column, and what it is called.
     */
    public static void checkPartitionColumnsAuthorized(List<Column> projectedColumns, Table apiTable,
                                                       LakeFormationTableIdentity identity) {
        if (apiTable.getPartitionKeys() == null || apiTable.getPartitionKeys().isEmpty()) {
            return;
        }
        Set<String> authorized = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        projectedColumns.forEach(column -> authorized.add(column.getName()));

        List<String> unauthorized = new ArrayList<>();
        for (FieldSchema partitionKey : apiTable.getPartitionKeys()) {
            if (!authorized.contains(partitionKey.getName())) {
                unauthorized.add(partitionKey.getName());
            }
        }
        if (!unauthorized.isEmpty()) {
            // Admin-only detail; the user facing message below stays deliberately vague.
            LOG.warn("Refusing {}: partition columns {} are not authorized by Lake Formation",
                    identity, unauthorized);
            throw new LakeFormationTableAccessException("One or more physical partition columns of "
                    + identity.dbName() + "." + identity.tableName()
                    + " are not authorized by Lake Formation, so this table cannot be queried."
                    + " Ask your Lake Formation administrator to grant the table's partition columns.");
        }
    }

    /** A shape this catalog cannot represent; enumeration leaves such a table out instead of failing. */
    private static LakeFormationTableAccessException unsupported(LakeFormationTableIdentity identity, String why) {
        return LakeFormationTableAccessException.nothingToDescribe(
                "Cannot query " + identity + ": " + why + ".");
    }
}
