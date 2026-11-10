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

package com.starrocks.connector.paimon;

import com.starrocks.common.util.concurrent.lock.BlockingCallValidator;
import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.consumer.ConsumerInfo;
import org.apache.paimon.function.Function;
import org.apache.paimon.function.FunctionChange;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.rest.responses.GetTagResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.utils.SnapshotNotExistException;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewChange;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * The paimon catalog's per-call door, placed between the cache and the catalog that actually talks
 * to the metastore.
 *
 * <h3>Why the door cannot go anywhere else</h3>
 *
 * Every other connector's door sits on the client the FE owns. Paimon has no such client: the FE
 * holds a {@link Catalog} built by paimon's own factory, and the cache in front of it --
 * {@link CachingPaimonCatalog}, which extends paimon's {@code CachingCatalog} -- resolves a hit
 * inside {@code super.getTable}, above anything the FE can hook. A guard on
 * {@code CachingPaimonCatalog} or on {@code PaimonMetadata} would therefore fire on cache hits,
 * which is the false positive the whole guard design exists to avoid.
 *
 * <p>Below the cache there is no such ambiguity: a call that gets here missed the cache, so it is
 * going to the metastore or the warehouse's file system. That is what makes this the honest place
 * for the door, and why the guard is unconditional here rather than hedged with a cache check.
 *
 * <h3>Why it extends DelegateCatalog rather than being a proxy</h3>
 *
 * A {@link java.lang.reflect.Proxy} would cover all 79 methods in a dozen lines instead of the
 * boilerplate below, and it was rejected: {@code PrivilegedCatalog.tryToCreate}, which the FE calls
 * on the catalog it builds, asks {@code DelegateCatalog.rootCatalog(catalog) instanceof
 * AbstractCatalog} and then casts. {@code rootCatalog} unwraps through {@link DelegateCatalog} and
 * nothing else, so a proxy in the chain would make that test fail and paimon privileges would be
 * dropped -- silently, since {@code tryToCreate} just returns the catalog unwrapped. Extending
 * {@link DelegateCatalog} keeps the chain unwrappable, which is what paimon's own composition
 * expects.
 *
 * <h3>Which methods get a door, and which are left without one</h3>
 *
 * Guarded: everything {@link Catalog} declares abstract. An implementation has to provide those, so
 * reaching this layer means a request is about to go out.
 *
 * <p>Not guarded, first reason -- it answers from configuration or does nothing here:
 * {@code options}, {@code catalogLoader}, {@code caseSensitive}, the five {@code supports*}
 * predicates, {@code invalidateTable}.
 *
 * <p>Not guarded, second reason -- <b>a known blind spot, taken deliberately</b>: the view, repair
 * and global-paging methods, which {@link Catalog} gives a default body that contacts nothing.
 * {@code FileSystemCatalog} inherits it and answers {@code getView} with
 * {@code ViewNotExistException} locally, while {@code RESTCatalog} overrides it and goes remote --
 * so whether these are a wait is a property of the catalog underneath, not of the interface.
 * Guarding them anyway would report waits that never happen on every non-REST catalog, and in error
 * mode would turn "this catalog has no views" into a lock violation; that is the false positive this
 * whole design exists to avoid, and it is the same reason the Glue client's construction has no
 * door. Deciding per implementation is possible -- ask reflectively whether the class overrides the
 * default -- and was rejected as machinery that keys production behaviour on method-name strings,
 * for a path the FE reaches only through {@code PaimonMetadata}'s four view calls.
 *
 * <p>So a paimon REST catalog's view lookups can reach the network under a lock unreported. Of the
 * seventeen methods in this group, those four are the only ones the FE calls at all.
 *
 * <p>{@code GuardedPaimonCatalogTest} pins the split: it walks {@link Catalog} by reflection and
 * fails if a method is not overridden here, so a paimon upgrade that adds one cannot quietly add a
 * path that is neither guarded nor listed above.
 */
public class GuardedPaimonCatalog extends DelegateCatalog {
    private final String catalogName;

    public GuardedPaimonCatalog(String catalogName, Catalog wrapped) {
        super(wrapped);
        this.catalogName = catalogName;
    }

    /**
     * The door. Named for what it says rather than for the check it runs, because that is what the
     * call sites below are asserting: control reaching here means the answer was not cached and the
     * thread is about to wait on paimon's metastore.
     */
    private void goingRemote() {
        BlockingCallValidator.validateNotUnderLock("paimon", catalogName);
    }

    @Override
    public List<String> listDatabases() {
        goingRemote();
        return wrapped.listDatabases();
    }

    @Override
    public PagedList<String> listDatabasesPaged(
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String databaseNamePattern) {
        goingRemote();
        return wrapped.listDatabasesPaged(maxResults, pageToken, databaseNamePattern);
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists) throws Catalog.DatabaseAlreadyExistException {
        goingRemote();
        wrapped.createDatabase(name, ignoreIfExists);
    }

    @Override
    public void createDatabase(
            String name,
            boolean ignoreIfExists,
            Map<String, String> properties) throws Catalog.DatabaseAlreadyExistException {
        goingRemote();
        wrapped.createDatabase(name, ignoreIfExists, properties);
    }

    @Override
    public Database getDatabase(String name) throws Catalog.DatabaseNotExistException {
        goingRemote();
        return wrapped.getDatabase(name);
    }

    @Override
    public void dropDatabase(
            String name,
            boolean ignoreIfNotExists,
            boolean cascade) throws Catalog.DatabaseNotExistException, Catalog.DatabaseNotEmptyException {
        goingRemote();
        wrapped.dropDatabase(name, ignoreIfNotExists, cascade);
    }

    @Override
    public void alterDatabase(
            String name,
            List<PropertyChange> changes,
            boolean ignoreIfNotExists) throws Catalog.DatabaseNotExistException {
        goingRemote();
        wrapped.alterDatabase(name, changes, ignoreIfNotExists);
    }

    @Override
    public Table getTable(Identifier identifier) throws Catalog.TableNotExistException {
        goingRemote();
        return wrapped.getTable(identifier);
    }

    @Override
    public Table getTableById(String tableId) throws Catalog.TableIdNotExistException {
        goingRemote();
        return wrapped.getTableById(tableId);
    }

    @Override
    public List<String> listTables(String databaseName) throws Catalog.DatabaseNotExistException {
        goingRemote();
        return wrapped.listTables(databaseName);
    }

    @Override
    public PagedList<String> listTablesPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType) throws Catalog.DatabaseNotExistException {
        goingRemote();
        return wrapped.listTablesPaged(databaseName, maxResults, pageToken, tableNamePattern, tableType);
    }

    @Override
    public PagedList<Table> listTableDetailsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType) throws Catalog.DatabaseNotExistException {
        goingRemote();
        return wrapped.listTableDetailsPaged(databaseName, maxResults, pageToken, tableNamePattern, tableType);
    }

    @Override
    public List<Table> listTableDetails(String databaseName) throws Catalog.DatabaseNotExistException {
        goingRemote();
        return wrapped.listTableDetails(databaseName);
    }

    @Override
    public PagedList<Identifier> listTablesPagedGlobally(
            @Nullable String databaseNamePattern,
            @Nullable String tableNamePattern,
            @Nullable Integer maxResults,
            @Nullable String pageToken) {
        return wrapped.listTablesPagedGlobally(databaseNamePattern, tableNamePattern, maxResults, pageToken);
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists) throws Catalog.TableNotExistException {
        goingRemote();
        wrapped.dropTable(identifier, ignoreIfNotExists);
    }

    @Override
    public void createTable(
            Identifier identifier,
            Schema schema,
            boolean ignoreIfExists) throws Catalog.TableAlreadyExistException, Catalog.DatabaseNotExistException {
        goingRemote();
        wrapped.createTable(identifier, schema, ignoreIfExists);
    }

    @Override
    public void replaceTable(
            Identifier identifier,
            Schema newSchema,
            boolean ignoreIfNotExists) throws Catalog.TableNotExistException {
        goingRemote();
        wrapped.replaceTable(identifier, newSchema, ignoreIfNotExists);
    }

    @Override
    public void renameTable(
            Identifier fromTable,
            Identifier toTable,
            boolean ignoreIfNotExists) throws Catalog.TableNotExistException, Catalog.TableAlreadyExistException {
        goingRemote();
        wrapped.renameTable(fromTable, toTable, ignoreIfNotExists);
    }

    @Override
    public void alterTable(
            Identifier identifier,
            List<SchemaChange> changes,
            boolean ignoreIfNotExists)
            throws Catalog.TableNotExistException, Catalog.ColumnAlreadyExistException, Catalog.ColumnNotExistException {
        goingRemote();
        wrapped.alterTable(identifier, changes, ignoreIfNotExists);
    }

    @Override
    public void invalidateTable(Identifier identifier) {
        wrapped.invalidateTable(identifier);
    }

    @Override
    public void alterTable(
            Identifier identifier,
            SchemaChange change,
            boolean ignoreIfNotExists)
            throws Catalog.TableNotExistException, Catalog.ColumnAlreadyExistException, Catalog.ColumnNotExistException {
        goingRemote();
        wrapped.alterTable(identifier, change, ignoreIfNotExists);
    }

    @Override
    public void markDonePartitions(
            Identifier identifier,
            List<Map<String, String>> partitions) throws Catalog.TableNotExistException {
        goingRemote();
        wrapped.markDonePartitions(identifier, partitions);
    }

    @Override
    public List<Partition> listPartitions(Identifier identifier) throws Catalog.TableNotExistException {
        goingRemote();
        return wrapped.listPartitions(identifier);
    }

    @Override
    public PagedList<Partition> listPartitionsPaged(
            Identifier identifier,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String partitionNamePattern) throws Catalog.TableNotExistException {
        goingRemote();
        return wrapped.listPartitionsPaged(identifier, maxResults, pageToken, partitionNamePattern);
    }

    @Override
    public List<Partition> listPartitionsByNames(
            Identifier identifier,
            List<Map<String, String>> partitions) throws Catalog.TableNotExistException {
        goingRemote();
        return wrapped.listPartitionsByNames(identifier, partitions);
    }

    @Override
    public PagedList<Partition> listPartitionsByFilterPaged(
            Identifier identifier,
            Predicate predicate,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String partitionNamePattern) throws Catalog.TableNotExistException {
        goingRemote();
        return wrapped.listPartitionsByFilterPaged(
                identifier, predicate, maxResults, pageToken, partitionNamePattern);
    }

    @Override
    public View getView(Identifier identifier) throws Catalog.ViewNotExistException {
        return wrapped.getView(identifier);
    }

    @Override
    public void dropView(Identifier identifier, boolean ignoreIfNotExists) throws Catalog.ViewNotExistException {
        wrapped.dropView(identifier, ignoreIfNotExists);
    }

    @Override
    public void createView(
            Identifier identifier,
            View view,
            boolean ignoreIfExists) throws Catalog.ViewAlreadyExistException, Catalog.DatabaseNotExistException {
        wrapped.createView(identifier, view, ignoreIfExists);
    }

    @Override
    public List<String> listViews(String databaseName) throws Catalog.DatabaseNotExistException {
        return wrapped.listViews(databaseName);
    }

    @Override
    public PagedList<String> listViewsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String viewNamePattern) throws Catalog.DatabaseNotExistException {
        return wrapped.listViewsPaged(databaseName, maxResults, pageToken, viewNamePattern);
    }

    @Override
    public PagedList<View> listViewDetailsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String viewNamePattern) throws Catalog.DatabaseNotExistException {
        return wrapped.listViewDetailsPaged(databaseName, maxResults, pageToken, viewNamePattern);
    }

    @Override
    public PagedList<Identifier> listViewsPagedGlobally(
            @Nullable String databaseNamePattern,
            @Nullable String viewNamePattern,
            @Nullable Integer maxResults,
            @Nullable String pageToken) {
        return wrapped.listViewsPagedGlobally(databaseNamePattern, viewNamePattern, maxResults, pageToken);
    }

    @Override
    public void renameView(
            Identifier fromView,
            Identifier toView,
            boolean ignoreIfNotExists) throws Catalog.ViewNotExistException, Catalog.ViewAlreadyExistException {
        wrapped.renameView(fromView, toView, ignoreIfNotExists);
    }

    @Override
    public void alterView(
            Identifier view,
            List<ViewChange> viewChanges,
            boolean ignoreIfNotExists)
            throws Catalog.ViewNotExistException, Catalog.DialectAlreadyExistException, Catalog.DialectNotExistException {
        wrapped.alterView(view, viewChanges, ignoreIfNotExists);
    }

    @Override
    public void repairCatalog() {
        wrapped.repairCatalog();
    }

    @Override
    public void repairDatabase(String databaseName) {
        wrapped.repairDatabase(databaseName);
    }

    @Override
    public void repairTable(Identifier identifier) throws Catalog.TableNotExistException {
        wrapped.repairTable(identifier);
    }

    @Override
    public void registerTable(Identifier identifier, String path) throws Catalog.TableAlreadyExistException {
        wrapped.registerTable(identifier, path);
    }

    @Override
    public boolean supportsListObjectsPaged() {
        return wrapped.supportsListObjectsPaged();
    }

    @Override
    public boolean supportsListByPattern() {
        return wrapped.supportsListByPattern();
    }

    @Override
    public boolean supportsListTableByType() {
        return wrapped.supportsListTableByType();
    }

    @Override
    public boolean supportsVersionManagement() {
        return wrapped.supportsVersionManagement();
    }

    @Override
    public boolean supportsPartitionModification() {
        return wrapped.supportsPartitionModification();
    }

    @Override
    public boolean commitSnapshot(
            Identifier identifier,
            @Nullable String tableUuid,
            @Nullable String baseSnapshotUuid,
            Snapshot snapshot,
            List<PartitionStatistics> statistics) throws Catalog.TableNotExistException {
        goingRemote();
        return wrapped.commitSnapshot(identifier, tableUuid, baseSnapshotUuid, snapshot, statistics);
    }

    @Override
    public Optional<TableSnapshot> loadSnapshot(Identifier identifier) throws Catalog.TableNotExistException {
        goingRemote();
        return wrapped.loadSnapshot(identifier);
    }

    @Override
    public Optional<Snapshot> loadSnapshot(
            Identifier identifier,
            String version) throws Catalog.TableNotExistException {
        goingRemote();
        return wrapped.loadSnapshot(identifier, version);
    }

    @Override
    public PagedList<Snapshot> listSnapshotsPaged(
            Identifier identifier,
            @Nullable Integer maxResults,
            @Nullable String pageToken) throws Catalog.TableNotExistException {
        goingRemote();
        return wrapped.listSnapshotsPaged(identifier, maxResults, pageToken);
    }

    @Override
    public void rollbackTo(Identifier identifier, Instant instant) throws Catalog.TableNotExistException {
        goingRemote();
        wrapped.rollbackTo(identifier, instant);
    }

    @Override
    public void rollbackTo(
            Identifier identifier,
            Instant instant,
            @Nullable Long fromSnapshot) throws Catalog.TableNotExistException {
        goingRemote();
        wrapped.rollbackTo(identifier, instant, fromSnapshot);
    }

    @Override
    public void rollbackSchema(Identifier identifier, long schemaId) throws Catalog.TableNotExistException {
        goingRemote();
        wrapped.rollbackSchema(identifier, schemaId);
    }

    @Override
    public void createBranch(
            Identifier identifier,
            String branch,
            @Nullable String fromTag)
            throws Catalog.TableNotExistException, Catalog.BranchAlreadyExistException, Catalog.TagNotExistException {
        goingRemote();
        wrapped.createBranch(identifier, branch, fromTag);
    }

    @Override
    public void createBranch(
            Identifier identifier,
            String branch,
            @Nullable String fromTag,
            boolean ignoreIfExists)
            throws Catalog.TableNotExistException, Catalog.BranchAlreadyExistException, Catalog.TagNotExistException {
        goingRemote();
        wrapped.createBranch(identifier, branch, fromTag, ignoreIfExists);
    }

    @Override
    public void renameBranch(
            Identifier identifier,
            String fromBranch,
            String toBranch)
            throws Catalog.BranchNotExistException, Catalog.BranchAlreadyExistException {
        goingRemote();
        wrapped.renameBranch(identifier, fromBranch, toBranch);
    }

    @Override
    public void dropBranch(Identifier identifier, String branch) throws Catalog.BranchNotExistException {
        goingRemote();
        wrapped.dropBranch(identifier, branch);
    }

    @Override
    public void fastForward(Identifier identifier, String branch) throws Catalog.BranchNotExistException {
        goingRemote();
        wrapped.fastForward(identifier, branch);
    }

    @Override
    public List<String> listBranches(Identifier identifier) throws Catalog.TableNotExistException {
        goingRemote();
        return wrapped.listBranches(identifier);
    }

    @Override
    public void createTag(
            Identifier identifier,
            String tagName,
            @Nullable Long snapshotId,
            @Nullable String timeRetained,
            boolean ignoreIfExists)
            throws Catalog.TableNotExistException, SnapshotNotExistException, Catalog.TagAlreadyExistException {
        goingRemote();
        wrapped.createTag(identifier, tagName, snapshotId, timeRetained, ignoreIfExists);
    }

    @Override
    public void deleteTag(
            Identifier identifier,
            String tagName) throws Catalog.TableNotExistException, Catalog.TagNotExistException {
        goingRemote();
        wrapped.deleteTag(identifier, tagName);
    }

    @Override
    public GetTagResponse getTag(
            Identifier identifier,
            String tagName) throws Catalog.TableNotExistException, Catalog.TagNotExistException {
        goingRemote();
        return wrapped.getTag(identifier, tagName);
    }

    @Override
    public PagedList<String> listTagsPaged(
            Identifier identifier,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tagNamePrefix) throws Catalog.TableNotExistException {
        goingRemote();
        return wrapped.listTagsPaged(identifier, maxResults, pageToken, tagNamePrefix);
    }

    @Override
    public PagedList<ConsumerInfo> listConsumersPaged(
            Identifier identifier,
            @Nullable Integer maxResults,
            @Nullable String pageToken) throws Catalog.TableNotExistException {
        goingRemote();
        return wrapped.listConsumersPaged(identifier, maxResults, pageToken);
    }

    @Override
    public void resetConsumer(
            Identifier identifier,
            String consumerId,
            @Nullable Long nextSnapshotId) throws Catalog.TableNotExistException {
        goingRemote();
        wrapped.resetConsumer(identifier, consumerId, nextSnapshotId);
    }

    @Override
    public void createPartitions(
            Identifier identifier,
            List<Map<String, String>> partitions) throws Catalog.TableNotExistException {
        goingRemote();
        wrapped.createPartitions(identifier, partitions);
    }

    @Override
    public void createPartitions(
            Identifier identifier,
            List<Map<String, String>> partitions,
            boolean ignoreIfExists) throws Catalog.TableNotExistException {
        goingRemote();
        wrapped.createPartitions(identifier, partitions, ignoreIfExists);
    }

    @Override
    public void dropPartitions(
            Identifier identifier,
            List<Map<String, String>> partitions) throws Catalog.TableNotExistException {
        goingRemote();
        wrapped.dropPartitions(identifier, partitions);
    }

    @Override
    public void alterPartitions(
            Identifier identifier,
            List<PartitionStatistics> partitions) throws Catalog.TableNotExistException {
        goingRemote();
        wrapped.alterPartitions(identifier, partitions);
    }

    @Override
    public List<String> listFunctions(String databaseName) throws Catalog.DatabaseNotExistException {
        goingRemote();
        return wrapped.listFunctions(databaseName);
    }

    @Override
    public PagedList<String> listFunctionsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String functionNamePattern) throws Catalog.DatabaseNotExistException {
        return wrapped.listFunctionsPaged(databaseName, maxResults, pageToken, functionNamePattern);
    }

    @Override
    public PagedList<Identifier> listFunctionsPagedGlobally(
            @Nullable String databaseNamePattern,
            @Nullable String functionNamePattern,
            @Nullable Integer maxResults,
            @Nullable String pageToken) {
        return wrapped.listFunctionsPagedGlobally(databaseNamePattern, functionNamePattern, maxResults, pageToken);
    }

    @Override
    public PagedList<Function> listFunctionDetailsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String functionNamePattern) throws Catalog.DatabaseNotExistException {
        return wrapped.listFunctionDetailsPaged(databaseName, maxResults, pageToken, functionNamePattern);
    }

    @Override
    public Function getFunction(Identifier identifier) throws Catalog.FunctionNotExistException {
        goingRemote();
        return wrapped.getFunction(identifier);
    }

    @Override
    public void createFunction(
            Identifier identifier,
            Function function,
            boolean ignoreIfExists) throws Catalog.FunctionAlreadyExistException, Catalog.DatabaseNotExistException {
        goingRemote();
        wrapped.createFunction(identifier, function, ignoreIfExists);
    }

    @Override
    public void dropFunction(Identifier identifier, boolean ignoreIfNotExists) throws Catalog.FunctionNotExistException {
        goingRemote();
        wrapped.dropFunction(identifier, ignoreIfNotExists);
    }

    @Override
    public void alterFunction(
            Identifier identifier,
            List<FunctionChange> changes,
            boolean ignoreIfNotExists)
            throws Catalog.FunctionNotExistException,
            Catalog.DefinitionAlreadyExistException,
            Catalog.DefinitionNotExistException {
        goingRemote();
        wrapped.alterFunction(identifier, changes, ignoreIfNotExists);
    }

    @Override
    public TableQueryAuthResult authTableQuery(
            Identifier identifier,
            @Nullable List<String> select) throws Catalog.TableNotExistException {
        goingRemote();
        return wrapped.authTableQuery(identifier, select);
    }

    @Override
    public Map<String, String> options() {
        return wrapped.options();
    }

    @Override
    public CatalogLoader catalogLoader() {
        return wrapped.catalogLoader();
    }

    @Override
    public boolean caseSensitive() {
        return wrapped.caseSensitive();
    }
}
