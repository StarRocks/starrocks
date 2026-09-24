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
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.HivePartitionDataInfo;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.QueryScopedCredentials;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.TableLoadPurpose;
import com.starrocks.connector.hive.HiveCacheUpdateProcessor;
import com.starrocks.connector.hive.HiveMetadata;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.HiveMetastoreOperations;
import com.starrocks.connector.hive.HiveStatisticsProvider;
import com.starrocks.connector.hive.Partition;
import com.starrocks.connector.hive.glue.converters.CatalogToHiveConverter;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.thrift.TSinkCommitInfo;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.services.glue.model.UnfilteredPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * HiveMetadata for a catalog governed by Lake Formation.
 *
 * MetadataMgr keeps one instance per (queryId, catalog), picked from the thread local ConnectContext. A
 * registered table is authorized, vended and listed through Lake Formation; everything else is delegated.
 */
public class LakeFormationHiveMetadata extends HiveMetadata {

    private final LakeFormationMetadataGateway gateway;
    private final LakeFormationCatalogProperties lakeFormationProperties;
    private final String lakeFormationCatalogName;

    private final LakeFormationPartitionReader partitionReader;
    // Read only for the S3 addressing options a vended configuration keeps.
    private final Map<String, String> catalogProperties;
    private final Configuration baseConfiguration;
    private final ExecutorService pullRemoteFileExecutor;
    private final boolean recursiveListing;

    // Listings no source has claimed yet; the backstop for paths that drop a coordinator without clearing it.
    private final Set<LakeFormationFileListing> unclaimedListings = ConcurrentHashMap.newKeySet();

    public LakeFormationHiveMetadata(String catalogName,
                                     HdfsEnvironment hdfsEnvironment,
                                     HiveMetastoreOperations hmsOps,
                                     RemoteFileOperations fileOperations,
                                     HiveStatisticsProvider statisticsProvider,
                                     Optional<HiveCacheUpdateProcessor> cacheUpdateProcessor,
                                     Executor updateExecutor,
                                     ConnectorProperties properties,
                                     LakeFormationMetadataGateway gateway,
                                     LakeFormationCatalogProperties lakeFormationProperties,
                                     LakeFormationPartitionReader partitionReader,
                                     Map<String, String> catalogProperties,
                                     Configuration baseConfiguration,
                                     ExecutorService pullRemoteFileExecutor,
                                     boolean recursiveListing) {
        super(catalogName, hdfsEnvironment, hmsOps, fileOperations, statisticsProvider, cacheUpdateProcessor,
                updateExecutor, properties);
        this.lakeFormationCatalogName = catalogName;
        this.gateway = gateway;
        this.lakeFormationProperties = lakeFormationProperties;
        this.partitionReader = partitionReader;
        this.catalogProperties = catalogProperties == null ? Map.of() : catalogProperties;
        this.baseConfiguration = baseConfiguration;
        this.pullRemoteFileExecutor = pullRemoteFileExecutor;
        this.recursiveListing = recursiveListing;
    }

    /**
     * Names the Lake Formation identity may see, not what the aws.glue.* identity can list, so a table granted to
     * nobody is not enumerated. Access to a listed table is still the access controller's decision.
     */
    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        return gateway.listTableNames(dbName);
    }

    /** An unannotated caller wants to read; defaulting to metadata-only would fail far from here. */
    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        return getTable(context, dbName, tblName, TableLoadPurpose.DATA_ACCESS);
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName, TableLoadPurpose purpose) {
        LakeFormationTableIdentity identity = identityOf(dbName, tblName);
        // Outside the scope: "no query context" belongs to the path, not the table, so it must not be memoized.
        LakeFormationQuerySession session = LakeFormationQuerySessions.of(context);
        LakeFormationQueryScope scope = LakeFormationQueryScope.current().orElse(null);

        if (scope == null) {
            // No attempt is open. Metadata-only readers such as DESC run at execution time and get a one-shot answer;
            // data access needs an attempt to account the credentials to.
            if (purpose != TableLoadPurpose.METADATA_ONLY) {
                // Except the table this statement already authorized while planning: ANALYZE resolves it again after
                // the attempt closed. It carries no credential and matches no open attempt, so it can never be scanned.
                Table planned = LakeFormationPlannedTables.find(session.queryId(), identity);
                if (planned != null && purpose == TableLoadPurpose.DATA_ACCESS) {
                    return planned;
                }
                // An ungoverned table needs no attempt, so it is served the ordinary way.
                if (governsNothingHere(identity, session)) {
                    return super.getTable(context, dbName, tblName);
                }
                throw new LakeFormationTableAccessException("Cannot read " + identity + " outside a planning"
                        + " attempt: credentials are issued per query, and there is no query here to issue"
                        + " them for.");
            }
            LakeFormationTableResolution oneShot =
                    resolveOnce(identity, session, TableLoadPurpose.METADATA_ONLY, null, context);
            oneShot.rethrowIfFailed();
            return oneShot.isUnregistered()
                    ? super.getTable(context, dbName, tblName) : oneShot.authorizedTable();
        }

        LakeFormationTableResolution resolution =
                scope.resolve(identity, purpose, key -> resolveOnce(identity, session, purpose, scope, context));
        resolution.rethrowIfFailed();
        if (resolution.isUnregistered()) {
            return super.getTable(context, dbName, tblName);
        }
        if (purpose == TableLoadPurpose.DATA_ACCESS) {
            LakeFormationPlannedTables.remember(session.queryId(), identity, resolution.authorizedTable());
        }
        return resolution.authorizedTable();
    }

    /** Never throws: computeIfAbsent drops a mapping whose function threw, and a failure must stay a failure. */
    private LakeFormationTableResolution resolveOnce(LakeFormationTableIdentity identity,
                                                     LakeFormationQuerySession session,
                                                     TableLoadPurpose purpose,
                                                     LakeFormationQueryScope scope,
                                                     ConnectContext context) {
        try {
            AuthorizedTableMetadata metadata = gateway.getTableMetadata(identity, session);
            // isRegistered throws on an absent flag; only an explicit false takes the ordinary path.
            if (!metadata.isRegistered(identity)) {
                return LakeFormationTableResolution.unregistered();
            }

            // One conversion shared by the guard and the builder. It can refuse a table with no storage descriptor, so
            // it reports that table rather than ending an enumeration of the whole catalog.
            org.apache.hadoop.hive.metastore.api.Table apiTable;
            try {
                apiTable = CatalogToHiveConverter.convertTable(metadata.table(), identity.dbName());
            } catch (RuntimeException e) {
                throw LakeFormationTableAccessException.nothingToDescribe(
                        "Cannot query " + identity + ": " + e.getMessage(), e);
            }

            LakeFormationTableGuard.check(metadata, apiTable, identity);

            // Never through hmsOps: that is a bare Glue call whose result lands in the cross-query tableCache.
            HiveTable physical = HiveMetastoreApiConverter.toHiveTable(apiTable, lakeFormationCatalogName);
            List<Column> authorized =
                    LakeFormationSchemaProjection.project(physical.getFullSchema(), metadata, identity);
            LakeFormationTableGuard.checkPartitionColumnsAuthorized(authorized, apiTable, identity);

            // A one-shot read gets an id nothing can match, so its table is never reusable or scannable.
            String attemptId = scope != null ? scope.attemptId() : "no-attempt-" + UUID.randomUUID();
            LakeFormationHiveTable table = LakeFormationHiveTable.of(physical, authorized, identity,
                    new LakeFormationTableHandle(identity, purpose, attemptId));
            if (purpose != TableLoadPurpose.DATA_ACCESS) {
                return LakeFormationTableResolution.metadataAuthorized(table, metadata);
            }

            // Vended while resolving, so no caller ever holds a table that looks readable and is not.
            String tableArn = LakeFormationTableArns.of(identity, metadata.table(), lakeFormationProperties);
            String queryAuthorizationId = metadata.queryAuthorizationId();
            if (queryAuthorizationId == null || queryAuthorizationId.isEmpty()) {
                throw new LakeFormationTableAccessException("Lake Formation reported " + identity
                        + " as registered but returned no QueryAuthorizationId for it, so no credentials"
                        + " can be requested.");
            }
            LakeFormationLeaseContext leaseContext = LakeFormationLeaseContext.create(identity,
                    scope.principal(), attemptId, session.queryId(),
                    context == null ? null : context.getExecutionId(), gateway, session, tableArn,
                    physical.getTableLocation(), lakeFormationProperties, catalogProperties,
                    gateway.cacheScopeSeed());
            LakeFormationTableAccess access =
                    gateway.vendTableCredentials(identity, tableArn, queryAuthorizationId, session);
            LakeFormationLease lease = LakeFormationLease.validated(leaseContext, access);
            leaseContext.holder().set(lease);
            LakeFormationTableResolution resolution =
                    LakeFormationTableResolution.accessReady(table, metadata, lease);
            // An unpartitioned table is listed through one Partition from its own storage descriptor, built while it is in hand.
            if (table.isUnPartitioned()) {
                leaseContext.tablePartition().set(HiveMetastoreApiConverter.toPartition(
                        apiTable.getSd(), apiTable.getParameters()));
            }
            return resolution;
        } catch (LakeFormationTableAccessException e) {
            return LakeFormationTableResolution.failed(e);
        } catch (Exception e) {
            return LakeFormationTableResolution.failed(new LakeFormationTableAccessException(
                    // toString: an NPE has no message.
                    "Failed to authorize " + identity + " with Lake Formation: " + e, e));
        }
    }

    // Data plane.

    /**
     * The catalog's own credentials, for tables Lake Formation does not govern. A governed table never gets here:
     * every scan asks the table for query scoped credentials first (HdfsScanNode), and refuseDataAccess backstops.
     */
    @Override
    public CloudConfiguration getCloudConfiguration() {
        return super.getCloudConfiguration();
    }

    /**
     * Lists with the credentials the scan node captured, carried in the request: nothing is left to look them up in.
     */
    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        LakeFormationLease lease = leaseForListing(table, params);
        if (lease == null) {
            return super.getRemoteFiles(table, params);
        }
        LakeFormationHiveTable lfTable = (LakeFormationHiveTable) table;
        LakeFormationFileListing listing = openListing(lease);
        try {
            return listing.operations()
                    .getRemoteFiles(table, partitionsFor(lfTable, lease, params), params);
        } finally {
            releaseListing(listing);
        }
    }

    @Override
    public RemoteFileInfoSource getRemoteFilesAsync(Table table, GetRemoteFilesParams params) {
        LakeFormationLease lease = leaseForListing(table, params);
        if (lease == null) {
            return super.getRemoteFilesAsync(table, params);
        }
        LakeFormationHiveTable lfTable = (LakeFormationHiveTable) table;
        // Consumed over time, so the source owns the listing and closes it.
        LakeFormationFileListing listing = openListing(lease);
        try {
            RemoteFileInfoSource delegate = listing.operations()
                    .getRemoteFilesAsync(table, params, p -> partitionsFor(lfTable, lease, p));
            unclaimedListings.remove(listing);
            return new LakeFormationRemoteFileInfoSource(delegate, listing);
        } catch (RuntimeException e) {
            releaseListing(listing);
            throw e;
        }
    }

    /**
     * @return the credentials to list with, or null for an ungoverned table. Refusals throw: null would read as
     *         "no credentials needed".
     */
    private LakeFormationLease leaseForListing(Table table, GetRemoteFilesParams params) {
        if (!(table instanceof LakeFormationHiveTable lfTable)) {
            refuseDataAccess(table, "enumerate its data files");
            return null;
        }
        QueryScopedCredentials credentials = params.getQueryScopedCredentials();
        if (credentials == null) {
            throw new LakeFormationTableAccessException("Cannot enumerate the data files of "
                    + lfTable.getLakeFormationIdentity() + ": no Lake Formation credentials were captured"
                    + " for this scan. The plan has to be rebuilt.");
        }
        if (!(credentials instanceof LakeFormationLease lease)) {
            throw new LakeFormationTableAccessException("Cannot enumerate the data files of "
                    + lfTable.getLakeFormationIdentity() + ": the credentials carried by this scan were not"
                    + " issued by Lake Formation.");
        }
        if (!lease.identity().equals(lfTable.getLakeFormationIdentity())) {
            throw new LakeFormationTableAccessException("Cannot enumerate the data files of "
                    + lfTable.getLakeFormationIdentity() + ": the credentials carried by this scan were"
                    + " issued for a different table.");
        }
        lease.cloudConfiguration(ConnectContext.get());
        return lease;
    }

    @Override
    public Optional<Map<String, Optional<HivePartitionDataInfo>>> getHivePartitionDataInfos(
            HiveTable table, List<String> partitionNames, int partitionLimit) {
        refuseDataAccess(table, "read its partition data layout");
        return super.getHivePartitionDataInfos(table, partitionNames, partitionLimit);
    }

    /**
     * Split by table type, not by the attempt's memo: the descriptor table is serialized after planning, when no
     * attempt is open, and must still read an ungoverned table's partitions. A governed table is answered from
     * the snapshot this attempt authorized, never from the bare metastore.
     */
    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        if (!(table instanceof LakeFormationHiveTable)) {
            refuseDataAccess(table, "read its partitions");
            return super.getPartitions(table, partitionNames);
        }
        if (((LakeFormationHiveTable) table).isUnPartitioned()) {
            // Lake Formation authorizes no partitions for an unpartitioned table; answer its single one.
            Partition single = tablePartitionFor(table.getCatalogDBName(), table.getCatalogTableName());
            if (single == null) {
                throw new LakeFormationTableAccessException("Cannot read the partitions of "
                        + identityOf(table.getCatalogDBName(), table.getCatalogTableName())
                        + ": it was not authorized while this statement was planned.");
            }
            return new ArrayList<>(Collections.nCopies(partitionNames.size(), single));
        }
        LakeFormationPartitionSnapshot snapshot =
                partitionSnapshotFor(table.getCatalogDBName(), table.getCatalogTableName());
        if (snapshot == null) {
            throw new LakeFormationTableAccessException("Cannot read the partitions of "
                    + identityOf(table.getCatalogDBName(), table.getCatalogTableName())
                    + ": Lake Formation authorized no partitions for this attempt.");
        }
        List<PartitionInfo> partitions = new ArrayList<>(partitionNames.size());
        for (String partitionName : partitionNames) {
            Partition partition = snapshot.partitionFor(partitionName);
            // Gone since planning: left out, as the ordinary path does, and the caller's size check decides.
            if (partition != null) {
                partitions.add(partition);
            }
        }
        return partitions;
    }

    /**
     * Delegates. Accepted: Glue statistics are read with the catalog identity and filtered to the requested
     * columns, and a missing numRows falls back to a listing that fails into unknown statistics.
     */
    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate,
                                         long limit,
                                         TvrVersionRange version) {
        if (table instanceof LakeFormationHiveTable) {
            return super.getTableStatistics(session, table, columns, partitionKeys, predicate, limit, version);
        }
        // Before super, which swallows every exception into unknown statistics.
        refuseDataAccess(table, "collect statistics for it");
        return super.getTableStatistics(session, table, columns, partitionKeys, predicate, limit, version);
    }

    /**
     * A no-op for a governed table: none of its state is in a shared cache, and refusing would break INSERT ...
     * SELECT, which refreshes its sources by default.
     */
    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames,
                             boolean onlyCachedPartitions) {
        if (table instanceof LakeFormationHiveTable) {
            return;
        }
        refuseDataAccess(table, "refresh it");
        super.refreshTable(srDbName, table, partitionNames, onlyCachedPartitions);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName,
                                           ConnectorMetadataRequestContext requestContext) {
        LakeFormationPartitionSnapshot snapshot = partitionSnapshotFor(dbName, tblName);
        if (snapshot == null) {
            return super.listPartitionNames(dbName, tblName, requestContext);
        }
        return snapshot.partitionNames();
    }

    @Override
    public List<String> listPartitionNamesByValue(String dbName, String tblName,
                                                  List<Optional<String>> partitionValues) {
        LakeFormationPartitionSnapshot snapshot = partitionSnapshotFor(dbName, tblName);
        if (snapshot == null) {
            return super.listPartitionNamesByValue(dbName, tblName, partitionValues);
        }
        // Filtered here: every partition is read anyway, since one outside the table root refuses the table.
        return LakeFormationPartitionSnapshot.filterByValues(snapshot.partitionNames(), partitionValues);
    }

    /** @return null when the table is not governed */
    private LakeFormationPartitionSnapshot partitionSnapshotFor(String dbName, String tblName) {
        LakeFormationTableIdentity identity = identityOf(dbName, tblName);
        if (noAttemptButMayAuthorize()) {
            return partitionsWithoutAnAttempt(identity);
        }
        LakeFormationTableResolution resolution = dataAccessResolutionFor(dbName, tblName);
        return resolution == null ? null : loadPartitions(resolution, identity);
    }

    private boolean noAttemptButMayAuthorize() {
        return LakeFormationQueryScope.current().isEmpty()
                && LakeFormationQuerySessions.mayAuthorizeWithoutAnAttempt();
    }

    /**
     * The single partition an unpartitioned table is listed through, or null. getPartitionNames and getPartitions
     * must agree on the count, or callers fail with "corrupted partition meta".
     */
    private Partition tablePartitionFor(String dbName, String tblName) {
        LakeFormationTableResolution resolution = dataAccessResolutionFor(dbName, tblName);
        if (resolution == null || resolution.lease() == null) {
            return null;
        }
        return resolution.lease().tablePartition();
    }

    private LakeFormationTableResolution dataAccessResolutionFor(String dbName, String tblName) {
        LakeFormationQueryScope scope = LakeFormationQueryScope.current().orElse(null);
        LakeFormationTableIdentity identity = identityOf(dbName, tblName);
        LakeFormationTableResolution resolution = scope == null
                ? null : scope.find(identity, TableLoadPurpose.DATA_ACCESS);
        if (resolution == null) {
            // Nothing recorded is not "unregistered"; deny by default.
            refuseIfRegistered(dbName, tblName, "list its partitions");
            return null;
        }
        resolution.rethrowIfFailed();
        if (resolution.isUnregistered()) {
            return null;
        }
        return resolution;
    }

    /**
     * For internal steps with no attempt - the ANALYZE worker listing partitions, an MV refresh recording versions
     * - open one for the length of the listing, with the same authorization the statement would get.
     */
    private LakeFormationPartitionSnapshot partitionsWithoutAnAttempt(LakeFormationTableIdentity identity) {
        ConnectContext context = ConnectContext.get();
        LakeFormationQuerySession session = LakeFormationQuerySessions.of(context);
        try (LakeFormationQueryScope.Scope ignored = LakeFormationQueryScope.open(context)) {
            LakeFormationQueryScope own = LakeFormationQueryScope.current().orElseThrow();
            LakeFormationTableResolution resolution = own.resolve(identity, TableLoadPurpose.DATA_ACCESS,
                    key -> resolveOnce(identity, session, TableLoadPurpose.DATA_ACCESS, own, context));
            resolution.rethrowIfFailed();
            return resolution.isUnregistered() ? null : loadPartitions(resolution, identity);
        }
    }

    // Package-private for tests.
    LakeFormationPartitionSnapshot loadPartitions(LakeFormationTableResolution resolution,
                                                  LakeFormationTableIdentity identity) {
        LakeFormationLease lease = resolution.lease();
        AtomicReference<LakeFormationLeaseContext.PartitionListing> cell = lease.context().partitions();
        LakeFormationLeaseContext.PartitionListing existing = cell.get();
        if (existing != null) {
            return existing.getOrRethrow();
        }
        // One request per table per attempt; a failure is published too, so it stays a failure.
        synchronized (cell) {
            LakeFormationLeaseContext.PartitionListing current = cell.get();
            if (current != null) {
                return current.getOrRethrow();
            }
            LakeFormationHiveTable table = (LakeFormationHiveTable) resolution.authorizedTable();
            try {
                LakeFormationQuerySession session = LakeFormationQuerySessions.of(ConnectContext.get());
                List<UnfilteredPartition> raw = partitionReader.readAll(identity, session, null);
                LakeFormationPartitionSnapshot snapshot = LakeFormationPartitionSnapshot.build(
                        table, raw, table.getAuthorizedColumnNames(), identity);
                cell.set(LakeFormationLeaseContext.PartitionListing.of(snapshot));
                return snapshot;
            } catch (LakeFormationTableAccessException e) {
                cell.set(LakeFormationLeaseContext.PartitionListing.failed(e));
                throw e;
            } catch (RuntimeException e) {
                LakeFormationTableAccessException failure = new LakeFormationTableAccessException(
                        "Failed to list the Lake Formation authorized partitions of " + identity + ": " + e, e);
                cell.set(LakeFormationLeaseContext.PartitionListing.failed(failure));
                throw failure;
            }
        }
    }

    /** From what this attempt authorized: a partition that appeared after planning was never checked. */
    private List<Partition> partitionsFor(LakeFormationHiveTable table, LakeFormationLease lease,
                                          GetRemoteFilesParams params) {
        if (table.isUnPartitioned()) {
            Partition tablePartition = lease.tablePartition();
            if (tablePartition == null) {
                throw new LakeFormationTableAccessException("Cannot enumerate the data files of "
                        + table.getLakeFormationIdentity() + ": it was not authorized while this query was"
                        + " planned.");
            }
            return List.of(tablePartition);
        }
        LakeFormationPartitionSnapshot snapshot = lease.partitions();
        if (snapshot == null) {
            throw new LakeFormationTableAccessException("Cannot enumerate the data files of "
                    + table.getLakeFormationIdentity() + ": its partitions were not resolved while this"
                    + " query was planned, and this version does not read partition metadata during"
                    + " execution.");
        }
        List<Partition> partitions = new ArrayList<>();
        for (PartitionKey partitionKey : params.getPartitionKeys()) {
            String name = PartitionUtil.toHivePartitionName(table.getPartitionColumnNames(), partitionKey);
            Partition partition = snapshot.partitionFor(name);
            if (partition == null) {
                throw new LakeFormationTableAccessException("Cannot enumerate the data files of "
                        + table.getLakeFormationIdentity() + ": partition " + name + " was not among the"
                        + " partitions Lake Formation authorized for this query.");
            }
            partitions.add(partition);
        }
        return partitions;
    }

    /** One per listing, so each can release its own file systems; re-validates the lease first. */
    private LakeFormationFileListing openListing(LakeFormationLease lease) {
        LakeFormationFileListing listing = LakeFormationFileListing.open(
                lease.cloudConfiguration(ConnectContext.get()), baseConfiguration, pullRemoteFileExecutor,
                recursiveListing);
        unclaimedListings.add(listing);
        return listing;
    }

    private void releaseListing(LakeFormationFileListing listing) {
        unclaimedListings.remove(listing);
        listing.close();
    }

    // Mutators: writing through Lake Formation is not supported in this version.

    @Override
    public void createDb(ConnectContext context, String dbName, Map<String, String> properties)
            throws AlreadyExistsException {
        throw refuseMutation("CREATE DATABASE");
    }

    @Override
    public void dropDb(ConnectContext context, String dbName, boolean isForceDrop) throws MetaNotFoundException {
        throw refuseMutation("DROP DATABASE");
    }

    @Override
    public boolean createTable(ConnectContext context, CreateTableStmt stmt) throws DdlException {
        throw refuseMutation("CREATE TABLE");
    }

    @Override
    public void createTableLike(CreateTableLikeStmt stmt) throws DdlException {
        throw refuseMutation("CREATE TABLE LIKE");
    }

    @Override
    public void truncateTable(TruncateTableStmt truncateTableStmt, ConnectContext context) throws DdlException {
        throw refuseMutation("TRUNCATE TABLE");
    }

    @Override
    public void dropTable(ConnectContext context, DropTableStmt stmt) throws DdlException {
        throw refuseMutation("DROP TABLE");
    }

    /** HiveMetadata runs HiveAlterTableExecutor against the remote Glue catalog. */
    @Override
    public ShowResultSet alterTable(ConnectContext context, AlterTableStmt stmt) throws StarRocksException {
        throw refuseMutation("ALTER TABLE");
    }

    @Override
    public void finishSink(String dbName, String tableName, List<TSinkCommitInfo> commitInfos, String branch) {
        throw refuseMutation("writing to");
    }

    @Override
    public void abortSink(String dbName, String tableName, List<TSinkCommitInfo> commitInfos) {
        throw refuseMutation("writing to");
    }

    @Override
    public void clear() {
        // Only unclaimed listings: this also runs on cache eviction, which says nothing about a running listing.
        unclaimedListings.forEach(LakeFormationFileListing::close);
        unclaimedListings.clear();
        super.clear();
    }

    /**
     * Registration is a deployment property, so it can be asked without an attempt. Never cached, and an absent
     * flag throws, so only an explicit "not registered" is served ordinarily.
     */
    private boolean governsNothingHere(LakeFormationTableIdentity identity,
                                       LakeFormationQuerySession session) {
        return !gateway.getTableMetadata(identity, session).isRegistered(identity);
    }

    /** awsCatalogId is nullable; absent means the caller's own account. */
    private LakeFormationTableIdentity identityOf(String dbName, String tblName) {
        return new LakeFormationTableIdentity(lakeFormationCatalogName,
                lakeFormationProperties.awsCatalogId(),
                lakeFormationProperties.region(), dbName, tblName);
    }

    /**
     * Refuses a path that would read a governed table with the catalog's own credentials. The plain HiveTable
     * half is a backstop: a registered table never produces one.
     */
    private void refuseDataAccess(Table table, String what) {
        if (table instanceof LakeFormationHiveTable lfTable) {
            throw new LakeFormationTableAccessException(
                    "Cannot " + what + " for " + lfTable.getLakeFormationIdentity()
                            + ": this path cannot carry the credentials Lake Formation issued for this"
                            + " query, and falling back to the catalog's own is not allowed.");
        }
        if (table instanceof HiveTable hiveTable) {
            LakeFormationTableResolution resolution = resolutionFor(
                    hiveTable.getCatalogDBName(), hiveTable.getCatalogTableName());
            if (resolution != null && !resolution.isUnregistered()) {
                throw new LakeFormationTableAccessException("Cannot " + what + " for "
                        + hiveTable.getCatalogDBName() + "." + hiveTable.getCatalogTableName()
                        + " on Lake Formation catalog " + lakeFormationCatalogName
                        + ": this table is governed by Lake Formation and this path cannot read it.");
            }
        }
    }

    /**
     * Deny by default: nothing recorded is not "unregistered". An unregistered table nothing authorized first is
     * refused too; avoiding that would need a live registration check or a cross-principal cache.
     */
    private void refuseIfRegistered(String dbName, String tblName, String what) {
        LakeFormationTableResolution resolution = resolutionFor(dbName, tblName);
        if (resolution != null && resolution.isUnregistered()) {
            return;
        }
        throw new LakeFormationTableAccessException("Cannot " + what + " for "
                + identityOf(dbName, tblName) + " without first authorizing the table with Lake Formation."
                + " This version does not authorize partition level access.");
    }

    /** Data access only: a metadata-only authorization permits reading nothing. */
    private LakeFormationTableResolution resolutionFor(String dbName, String tblName) {
        LakeFormationQueryScope scope = LakeFormationQueryScope.current().orElse(null);
        if (scope == null) {
            return null;
        }
        return scope.find(identityOf(dbName, tblName), TableLoadPurpose.DATA_ACCESS);
    }

    private LakeFormationTableAccessException refuseMutation(String what) {
        return new LakeFormationTableAccessException(what + " is not supported on Lake Formation catalog "
                + lakeFormationCatalogName + " in this version.");
    }
}
