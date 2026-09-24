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

import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.HiveTable;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Builds the table that SHOW CREATE TABLE is allowed to print for a Lake Formation governed table.
 *
 * This is a display-only object. It is the one place allowed to narrow partColumnNames and dataColumnNames,
 * because it never reaches the planner, the scan or a descriptor - narrowing those on an executable table
 * would break partition paths and the BE's positional column mapping.
 *
 * Properties are an allowlist by key AND a rebuild by value. Filtering keys alone is not enough:
 * toHiveProperties ends by copying every Glue table parameter over the computed ones, so a parameter named
 * exactly like an allowlisted key can carry any string at all - an unauthorized column name included.
 */
public final class LakeFormationDdlProjection {

    /**
     * An allowlist, not a denylist. Several Glue parameters carry the full physical schema -
     * spark.sql.sources.schema.part.N is written on virtually every Spark table, and so are columns,
     * columns.types and presto_view. Enumerating what to hide cannot be done; enumerating what is safe to
     * show can.
     */
    // Spelled exactly as Hive writes them (StatsSetupConst.NUM_FILES = "numFiles", DDL_TIME =
    // "transient_lastDdlTime", EXTERNAL is upper case). toHiveProperties copies Glue's parameter map
    // verbatim, and the lookup below is case sensitive, so a folded spelling here silently matches
    // nothing and SHOW CREATE TABLE loses the property instead of showing it.
    private static final Set<String> DISPLAYABLE_PROPERTIES = ImmutableSet.of(
            "location",
            HiveTable.HIVE_TABLE_INPUT_FORMAT,
            HiveTable.HIVE_TABLE_SERDE_LIB,
            "transient_lastDdlTime",
            "totalSize",
            "numFiles",
            "numRows",
            "EXTERNAL",
            "table_type",
            // Format labels on virtually every crawler or Spark table. Matched exactly, which keeps the
            // dangerous sibling spark.sql.sources.schema.part.N out.
            "classification",
            "spark.sql.sources.provider");

    private static final Set<String> NUMERIC_PROPERTIES = ImmutableSet.of(
            "transient_lastDdlTime", "totalSize", "numFiles", "numRows");

    private static final Set<String> BOOLEAN_LIKE_PROPERTIES = ImmutableSet.of("EXTERNAL");

    /**
     * Free-form labels with no typed source to rebuild from, so they are validated by shape: one token that
     * is not a physical column name. A schema dump cannot take that shape, and a value naming a column is
     * refused even if it does.
     */
    private static final Set<String> TOKEN_PROPERTIES =
            ImmutableSet.of("classification", "spark.sql.sources.provider");

    private static final Pattern SINGLE_TOKEN = Pattern.compile("[A-Za-z0-9_.+-]{1,64}");

    private static final Set<String> TABLE_TYPE_VALUES = ImmutableSet.of(
            "MANAGED_TABLE", "EXTERNAL_TABLE", "VIRTUAL_VIEW");

    private LakeFormationDdlProjection() {
    }

    public static HiveTable projectForDisplay(LakeFormationHiveTable table) {
        List<String> visiblePartitionColumns = table.getPartitionColumnNames().stream()
                .filter(table::isColumnAuthorized)
                .collect(Collectors.toList());
        List<String> visibleDataColumns = table.getDataColumnNames().stream()
                .filter(table::isColumnAuthorized)
                .collect(Collectors.toList());

        // Already the wrapper's own immutable snapshot, so reading it does not rewrite the authorized view
        // the way HiveTable.getProperties() would.
        Map<String, String> physicalProperties = table.getProperties();
        Map<String, String> visibleProperties = new HashMap<>();
        for (String key : DISPLAYABLE_PROPERTIES) {
            rebuildInto(visibleProperties, key, physicalProperties, table);
        }

        return HiveTable.builder()
                .setId(table.getId())
                .setTableName(table.getName())
                .setCatalogName(table.getCatalogName())
                .setResourceName(table.getResourceName())
                .setHiveDbName(table.getCatalogDBName())
                .setHiveTableName(table.getCatalogTableName())
                .setTableLocation(table.getTableLocation())
                .setComment(table.getComment())
                .setCreateTime(table.getCreateTime())
                .setFullSchema(table.getFullSchema())
                .setPartitionColumnNames(visiblePartitionColumns)
                .setDataColumnNames(visibleDataColumns)
                .setProperties(visibleProperties)
                // Deliberately verbatim: the DDL formatter never reads serdeProperties, so filtering it here
                // would be a no-op. The test asserts it stays equal to the physical one, so that if anyone
                // later teaches the formatter to print it, the test goes red first.
                .setSerdeProperties(table.getSerdeProperties())
                .setStorageFormat(table.getStorageFormat())
                .setHiveTableType(table.getHiveTableType())
                .build();
    }

    /**
     * Rebuilds one allowlisted property from a typed source instead of copying the string through. Anything
     * that does not survive the round trip is dropped rather than shown, because a value that cannot be
     * validated is exactly the case a Glue parameter override would produce.
     */
    private static void rebuildInto(Map<String, String> target, String key,
                                    Map<String, String> physicalProperties, LakeFormationHiveTable table) {
        if (HiveTable.HIVE_TABLE_INPUT_FORMAT.equals(key)) {
            // From the enum the guard already validated, never from the property string.
            putIfPresent(target, key, table.getStorageFormat() == null ? null
                    : table.getStorageFormat().getInputFormat());
            return;
        }
        if (HiveTable.HIVE_TABLE_SERDE_LIB.equals(key)) {
            putIfPresent(target, key, table.getStorageFormat() == null ? null
                    : table.getStorageFormat().getSerde());
            return;
        }
        if ("location".equals(key)) {
            putIfPresent(target, key, table.getTableLocation());
            return;
        }

        String raw = physicalProperties.get(key);
        if (raw == null) {
            return;
        }
        if (NUMERIC_PROPERTIES.contains(key)) {
            try {
                // Parsed and re-serialized: a parameter override carrying arbitrary text does not survive.
                putIfPresent(target, key, String.valueOf(Long.parseLong(raw.trim())));
            } catch (NumberFormatException ignored) {
                // Not a number, so not the property it claims to be. Showing it is what we are avoiding.
            }
            return;
        }
        if (BOOLEAN_LIKE_PROPERTIES.contains(key)) {
            String normalized = raw.trim().toLowerCase(Locale.ROOT);
            if ("true".equals(normalized) || "false".equals(normalized)) {
                putIfPresent(target, key, normalized.toUpperCase(Locale.ROOT));
            }
            return;
        }
        if ("table_type".equals(key)) {
            String normalized = raw.trim().toUpperCase(Locale.ROOT);
            if (TABLE_TYPE_VALUES.contains(normalized)) {
                putIfPresent(target, key, normalized);
            }
            return;
        }
        if (TOKEN_PROPERTIES.contains(key)) {
            String trimmed = raw.trim();
            if (SINGLE_TOKEN.matcher(trimmed).matches() && !table.isPhysicalColumn(trimmed)) {
                putIfPresent(target, key, trimmed);
            }
        }
    }

    private static void putIfPresent(Map<String, String> target, String key, String value) {
        if (value != null && !value.isEmpty()) {
            target.put(key, value);
        }
    }
}
