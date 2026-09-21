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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Turns a physical Hive schema plus Lake Formation's AuthorizedColumns into the schema a user is allowed
 * to see. Every disagreement between the two is an error: silently dropping a column the caller believes
 * is authorized, or silently keeping one it does not, are both wrong in a way nobody would notice.
 *
 * Matching is by name and case insensitive, because Table.updateSchemaIndex indexes columns in a
 * CASE_INSENSITIVE_ORDER map - a case sensitive intersection here would produce a table whose
 * getColumn("Id") resolves to a column that is not in its own fullSchema.
 *
 * The result keeps the physical order, not the order Lake Formation listed the columns in: toThrift builds
 * the BE's column list from getBaseSchema() while hive_column_names stays physical, and the two disagreeing
 * would show up as columns silently reading each other's data.
 */
public final class LakeFormationSchemaProjection {
    private static final Logger LOG = LogManager.getLogger(LakeFormationSchemaProjection.class);

    private LakeFormationSchemaProjection() {
    }

    public static List<Column> project(List<Column> physicalSchema, AuthorizedTableMetadata metadata,
                                       LakeFormationTableIdentity identity) {
        // Two different answers, two different messages. authorizedColumns() is never null - the value
        // object copies it with ImmutableList.copyOf and the SDK auto-constructs an empty list for an
        // absent field - so hasAuthorizedColumns() is the only way to tell "Lake Formation said nothing
        // about columns" apart from "Lake Formation authorized none of them". A null check would be dead.
        if (!metadata.hasAuthorizedColumns()) {
            throw new LakeFormationTableAccessException(
                    "Lake Formation returned no AuthorizedColumns field for " + identity
                            + ". Refusing to fall back to the physical schema.");
        }
        List<String> authorizedColumns = metadata.authorizedColumns();
        if (authorizedColumns.isEmpty()) {
            throw new LakeFormationTableAccessException(
                    "Lake Formation authorized no columns on " + identity
                            + ". The principal can describe the table but has no SELECT grant on any column."
                            + " Note that renaming a table clears its column level grants.");
        }

        Map<String, Column> byName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Column column : physicalSchema) {
            if (byName.put(column.getName(), column) != null) {
                // The duplicated name is not repeated back: the caller may have no grant on it, and the same
                // rule that keeps unauthorized column names out of every other refusal applies here.
                LOG.warn("Physical schema of {} has two columns named '{}' ignoring case",
                        identity, column.getName());
                throw new LakeFormationTableAccessException("Physical schema of " + identity
                        + " has two columns whose names differ only in case; cannot intersect it with"
                        + " AuthorizedColumns safely.");
            }
        }

        Set<String> authorized = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        List<String> unknown = new ArrayList<>();
        for (String name : authorizedColumns) {
            if (byName.containsKey(name)) {
                authorized.add(name);
            } else {
                unknown.add(name);
            }
        }
        if (!unknown.isEmpty()) {
            throw new LakeFormationTableAccessException(
                    "Lake Formation authorized columns " + unknown + " that do not exist in the physical schema of "
                            + identity + ". The Glue schema and the data files disagree; refusing to guess."
                            + " A crawler run after a schema change usually fixes this.");
        }

        List<Column> projected = new ArrayList<>(authorized.size());
        for (Column column : physicalSchema) {
            if (authorized.contains(column.getName())) {
                projected.add(column);
            }
        }
        return projected;
    }
}
