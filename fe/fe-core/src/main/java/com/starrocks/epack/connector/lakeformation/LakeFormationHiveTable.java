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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.TableOperation;
import com.starrocks.connector.QueryScopedCredentials;
import com.starrocks.connector.QueryScopedCredentialsSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * A Hive table as one principal is allowed to see it, for the duration of one query.
 *
 * Its visible schema is the AuthorizedColumns subset. Every physical partition column is guaranteed to be
 * authorized - the guard refuses the table otherwise - which is what lets every inherited partition accessor
 * stay correct without an override. The type itself is the marker that this table is governed by Lake
 * Formation: unlike a nullable field it cannot be lost by a copy, and no code path produces one by accident.
 *
 * Passing the authorized subset as fullSchema is what makes this self consistent: Table's constructor runs
 * updateSchemaIndex, which rebuilds nameToColumn and idToColumn from fullSchema alone, so getColumn and
 * containColumn only ever see authorized columns. Nothing here has to keep an index in sync.
 *
 * Only fullSchema is narrowed. Every list that carries a name or a position - partColumnNames,
 * dataColumnNames - stays physical, because partition paths and the BE's column mapping are built
 * positionally from them.
 *
 * This is not an immutable object, and it does not try to be. getFullSchema, getIdToColumn, getNameToColumn,
 * getPartitionColumnNames, getDataColumnNames and getSerdeProperties all hand back the underlying field,
 * exactly as they do on any HiveTable, and Column itself has setters. What the sealed mutators below buy is
 * narrower than it looks: they stop the known persistence and schema-rewrite entry points, so an authorized
 * view cannot be re-widened through modifyTableSchema or written to the edit log. They are not an object
 * level guarantee that nobody can reach in and change it.
 *
 * Beware: this type equals its own physical table. HiveTable.equals tests `instanceof HiveTable` and compares
 * catalog, db and table identifier, so the two are the same key in any Map&lt;Table, ...&gt;. One query must
 * never put both into the same DescriptorTable.
 */
public class LakeFormationHiveTable extends HiveTable
        implements QueryScopedCredentialsSource, LakeFormationGovernedTable {

    private final Set<String> authorizedColumnNames;
    private final LakeFormationTableIdentity lakeFormationIdentity;

    /**
     * How this table finds what authorized it. Never the credentials: a Table outlives its query in several caches,
     * and a handle from a finished attempt simply stops resolving. Transient against gson reflection.
     */
    private final transient LakeFormationTableHandle handle;

    /**
     * Own snapshot, because HiveTable.getProperties() is not a plain getter: on a resource backed table it
     * puts the metastore URIs into the map and returns the field itself. Calling super's version would keep
     * rewriting this authorized view's own property map while a query is running.
     */
    private final Map<String, String> propertiesSnapshot;

    public static LakeFormationHiveTable of(HiveTable physical, List<Column> authorizedSchema,
                                            LakeFormationTableIdentity identity,
                                            LakeFormationTableHandle handle) {
        return new LakeFormationHiveTable(physical, authorizedSchema, identity, handle);
    }

    private LakeFormationHiveTable(HiveTable physical, List<Column> authorizedSchema,
                                   LakeFormationTableIdentity identity,
                                   LakeFormationTableHandle handle) {
        // Every collection is copied: HiveTable's constructor stores these by reference and
        // modifyTableSchemaInternal clears them in place, so sharing them would let one table mutate the
        // other. createTime is passed through because getUUID is built from it and the masking and
        // row access policies are keyed by UUID.
        super(physical.getId(), physical.getName(), deepCopy(authorizedSchema), physical.getResourceName(),
                physical.getCatalogName(), physical.getCatalogDBName(), physical.getCatalogTableName(),
                physical.getTableLocation(), physical.getComment(), physical.getCreateTime(),
                new ArrayList<>(physical.getPartitionColumnNames()),
                new ArrayList<>(physical.getDataColumnNames()),
                new HashMap<>(physical.getProperties()),
                new HashMap<>(physical.getSerdeProperties()),
                physical.getStorageFormat(), physical.getHiveTableType());
        // Not a constructor argument, and toThrift reads it.
        setAvroSchemaJson(physical.getAvroSchemaJson());
        // Snapshot taken once. super's getProperties reaches the ResourceMgr and mutates the map as it
        // goes, so from here on this table answers from the snapshot and stops changing under a running query.
        this.propertiesSnapshot = ImmutableMap.copyOf(super.getProperties());
        Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        authorizedSchema.forEach(column -> names.add(column.getName()));
        this.authorizedColumnNames = Collections.unmodifiableSet(names);
        this.lakeFormationIdentity = requireNonNull(identity, "identity is null");
        this.handle = requireNonNull(handle, "handle is null");
    }

    public LakeFormationTableHandle getLakeFormationHandle() {
        return handle;
    }

    /** The credentials of the attempt planning now, via the handle; empty anywhere outside that attempt. */
    @Override
    public Optional<QueryScopedCredentials> currentQueryScopedCredentials() {
        return LakeFormationQueryScope.current()
                .filter(handle::isReusableIn)
                .map(scope -> scope.find(lakeFormationIdentity, handle.purpose()))
                .filter(resolution -> resolution != null && resolution.isAccessReady())
                .map(resolution -> (QueryScopedCredentials) resolution.lease());
    }

    /*
     * Deliberately no getPartitionColumns override. The inherited implementation resolves partColumnNames
     * through nameToColumn, and the guard has already established that every partition column is authorized,
     * so it resolves cleanly. Overriding it either way breaks a different part of the planner.
     */

    /**
     * The snapshot, never super's live map. This is not a filtered view - the content is the complete physical
     * property set, which is what toThrift and the format resolvers need - it just stops being rewritten
     * mid query. toThrift is unaffected: it reads super's private hiveProperties field directly, which the
     * constructor already initialized.
     */
    @Override
    public Map<String, String> getProperties() {
        return propertiesSnapshot;
    }

    @Override
    public boolean isColumnAuthorized(String columnName) {
        return authorizedColumnNames.contains(columnName);
    }

    /** From the physical lists, so an unauthorized real column is refused rather than taken for a placeholder. */
    @Override
    public boolean isPhysicalColumn(String columnName) {
        return containsIgnoreCase(getDataColumnNames(), columnName)
                || containsIgnoreCase(getPartitionColumnNames(), columnName);
    }

    private static boolean containsIgnoreCase(List<String> names, String column) {
        if (names == null) {
            return false;
        }
        for (String name : names) {
            if (name != null && name.equalsIgnoreCase(column)) {
                return true;
            }
        }
        return false;
    }

    /** Immutable, and case insensitive to match the comparison semantics of Table's own schema index. */
    public Set<String> getAuthorizedColumnNames() {
        return authorizedColumnNames;
    }

    @Override
    public LakeFormationTableIdentity getLakeFormationIdentity() {
        return lakeFormationIdentity;
    }

    /**
     * Read only, as a capability statement: writing to a Lake Formation governed table has not been
     * implemented or verified in this version. MetaUtils.checkNotSupportCatalog consults this for ALTER,
     * DELETE and CREATE TABLE LIKE; the statement guard covers the rest.
     */
    @Override
    public Set<TableOperation> getSupportedOperations() {
        return ImmutableSet.of(TableOperation.READ);
    }

    /**
     * Separate from getSupportedOperations because InsertAnalyzer consults this one, not that one, and
     * HiveTable hardcodes it to true. Overriding only the operation set would leave INSERT open.
     */
    @Override
    public boolean supportInsert() {
        return false;
    }

    /**
     * The schema mutators are sealed, not just modifyTableSchema. An authorized snapshot that can be
     * re-widened in place is not a snapshot, and modifyTableSchema additionally writes a
     * ModifyTableColumnOperationLog to the edit log.
     *
     * addColumn is sealed for a second reason: upstream's version updates fullSchema and nameToColumn but
     * not idToColumn, so calling it would leave the two indexes disagreeing.
     */
    @Override
    public void modifyTableSchema(String dbName, String tableName, HiveTable updatedTable) {
        throw refuseMutation("modifyTableSchema");
    }

    @Override
    public void setNewFullSchema(List<Column> newSchema) {
        throw refuseMutation("setNewFullSchema");
    }

    @Override
    public void addColumn(Column column) {
        throw refuseMutation("addColumn");
    }

    /**
     * Column is mutable - setComment, setType and setName all exist - and Table's constructor copies the
     * list but not the elements. Without this the authorized snapshot and the physical table would share
     * every Column instance, so mutating one would rewrite the other.
     */
    private static List<Column> deepCopy(List<Column> columns) {
        return columns.stream().map(Column::deepCopy).collect(Collectors.toCollection(ArrayList::new));
    }

    private LakeFormationTableAccessException refuseMutation(String operation) {
        return new LakeFormationTableAccessException("Refusing " + operation + " on the Lake Formation "
                + "authorized view of " + lakeFormationIdentity + ": its schema is one principal's "
                + "authorized subset and must not be changed or persisted");
    }
}
