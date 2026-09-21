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
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.hive.HiveCacheUpdateProcessor;
import com.starrocks.connector.hive.HiveMetadata;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.HiveMetastoreOperations;
import com.starrocks.connector.hive.HiveStatisticsProvider;
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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * HiveMetadata for a catalog governed by Lake Formation.
 *
 * The instance is already query scoped when a query id exists: MetadataMgr keeps one ConnectorMetadata per
 * (queryId, catalog), so the memo below is per query without any extra plumbing. Note that MetadataMgr picks
 * that instance from the thread local ConnectContext, never from the context passed along the call chain.
 *
 * This version authorizes metadata only. Every data plane entry point is refused for a registered table,
 * because serving it would mean reading with the catalog's own credentials - exactly what deploying Lake
 * Formation is meant to prevent.
 */
public class LakeFormationHiveMetadata extends HiveMetadata {

    private final LakeFormationMetadataGateway gateway;
    private final LakeFormationCatalogProperties lakeFormationProperties;
    private final String lakeFormationCatalogName;
    // No awsCatalogId field: it comes from lakeFormationProperties.awsCatalogId(), the same value the
    // gateway puts on the wire. A second copy could drift from the one the request actually used.

    // Concurrency is not expected during analysis, but PrepareCollectMetaTask does fan out over the same
    // metadata instance, so the map is concurrent and resolution is deduplicated by computeIfAbsent.
    private final Map<LakeFormationTableIdentity, LakeFormationTableResolution> memo = new ConcurrentHashMap<>();

    public LakeFormationHiveMetadata(String catalogName,
                                     HdfsEnvironment hdfsEnvironment,
                                     HiveMetastoreOperations hmsOps,
                                     RemoteFileOperations fileOperations,
                                     HiveStatisticsProvider statisticsProvider,
                                     Optional<HiveCacheUpdateProcessor> cacheUpdateProcessor,
                                     Executor updateExecutor,
                                     ConnectorProperties properties,
                                     LakeFormationMetadataGateway gateway,
                                     LakeFormationCatalogProperties lakeFormationProperties) {
        super(catalogName, hdfsEnvironment, hmsOps, fileOperations, statisticsProvider, cacheUpdateProcessor,
                updateExecutor, properties);
        this.lakeFormationCatalogName = catalogName;
        this.gateway = gateway;
        this.lakeFormationProperties = lakeFormationProperties;
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        LakeFormationTableIdentity identity = identityOf(dbName, tblName);
        // Outside the memo on purpose: "this call path has no query context" is a property of the path, not
        // of the table, and memoizing it would keep the table dead for the rest of the query even on paths
        // that do have one.
        LakeFormationQuerySession session = LakeFormationQuerySessions.of(context);
        LakeFormationTableResolution resolution =
                memo.computeIfAbsent(memoKey(dbName, tblName), key -> resolveOnce(identity, session));
        resolution.rethrowIfFailed();
        if (resolution.isUnregistered()) {
            return super.getTable(context, dbName, tblName);
        }
        return resolution.authorizedTable();
    }

    /**
     * Never throws: computeIfAbsent does not memoize when the mapping function throws, and a table that
     * failed authorization must keep failing for the rest of this query rather than getting a second chance
     * on the next call.
     */
    private LakeFormationTableResolution resolveOnce(LakeFormationTableIdentity identity,
                                                     LakeFormationQuerySession session) {
        try {
            AuthorizedTableMetadata metadata = gateway.getTableMetadata(identity, session);
            // isRegistered throws when the response carried no flag at all, which is what keeps "absent"
            // from being read as "not registered". Only an explicit false takes the ordinary path.
            if (!metadata.isRegistered(identity)) {
                return LakeFormationTableResolution.unregistered();
            }

            // Lake Formation answers with the AWS SDK v2 Glue model; everything below reads the Hive model.
            // One conversion, shared by the guard and the builder, so what was validated is what gets built.
            org.apache.hadoop.hive.metastore.api.Table apiTable =
                    CatalogToHiveConverter.convertTable(metadata.table(), identity.dbName());

            LakeFormationTableGuard.check(metadata, apiTable, identity);

            // Built from the Lake Formation response, never through hmsOps: HiveMetastore.getTable is a bare
            // Glue call with the catalog's own credentials, and its result lands in the cross-query
            // tableCache where the next principal would hit an untrimmed physical schema.
            HiveTable physical = HiveMetastoreApiConverter.toHiveTable(apiTable, lakeFormationCatalogName);
            List<Column> authorized =
                    LakeFormationSchemaProjection.project(physical.getFullSchema(), metadata, identity);
            // After the projection, so the names compared here were already resolved against the physical
            // schema exactly once.
            LakeFormationTableGuard.checkPartitionColumnsAuthorized(authorized, apiTable, identity);
            return LakeFormationTableResolution.authorized(
                    LakeFormationHiveTable.of(physical, authorized, identity));
        } catch (LakeFormationTableAccessException e) {
            return LakeFormationTableResolution.failed(e);
        } catch (Exception e) {
            return LakeFormationTableResolution.failed(new LakeFormationTableAccessException(
                    // toString rather than getMessage: an NPE carries no message at all, and "…: null" is
                    // a dead end for whoever has to work out which table stopped working.
                    "Failed to authorize " + identity + " with Lake Formation: " + e, e));
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // Data plane. Everything here is refused for a registered table.
    // ---------------------------------------------------------------------------------------------------

    /**
     * The catalog's own credentials, refused outright rather than per table.
     *
     * HdfsScanNode calls this from its constructor, before the remote-file refusal further down can fire -
     * that one only runs when the scan range source is lazily set up. Leaving this open would make the
     * boundary depend on the order two unrelated call sites happen to run in, and the whole point of a Lake
     * Formation deployment is that the catalog's own credentials are never what reads the data.
     *
     * It is catalog scoped, not table scoped, so this also refuses unregistered tables in the same catalog.
     * That is the intended trade for this version, which serves no data at all.
     */
    @Override
    public CloudConfiguration getCloudConfiguration() {
        throw new LakeFormationTableAccessException("Lake Formation catalog " + lakeFormationCatalogName
                + " does not hand out data credentials in this version, so its tables cannot be scanned.");
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        refuseDataAccess(table, "enumerate its data files");
        return super.getRemoteFiles(table, params);
    }

    @Override
    public RemoteFileInfoSource getRemoteFilesAsync(Table table, GetRemoteFilesParams params) {
        refuseDataAccess(table, "enumerate its data files");
        return super.getRemoteFilesAsync(table, params);
    }

    @Override
    public Optional<Map<String, Optional<HivePartitionDataInfo>>> getHivePartitionDataInfos(
            HiveTable table, List<String> partitionNames, int partitionLimit) {
        refuseDataAccess(table, "read its partition data layout");
        return super.getHivePartitionDataInfos(table, partitionNames, partitionLimit);
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        refuseDataAccess(table, "read its partitions");
        return super.getPartitions(table, partitionNames);
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate,
                                         long limit,
                                         TvrVersionRange version) {
        // Ahead of super on purpose: HiveMetadata.getTableStatistics swallows every Exception and returns
        // unknown statistics, which would turn a refusal into a silently degraded plan.
        refuseDataAccess(table, "collect statistics for it");
        return super.getTableStatistics(session, table, columns, partitionKeys, predicate, limit, version);
    }

    /**
     * Refreshing ends in an unconditional bare-Glue loadTable whose result is put into the cross-query
     * tableCache, so it has to be refused rather than allowed to repopulate the cache with an untrimmed
     * schema.
     */
    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames,
                             boolean onlyCachedPartitions) {
        refuseDataAccess(table, "refresh it");
        super.refreshTable(srDbName, table, partitionNames, onlyCachedPartitions);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName,
                                           ConnectorMetadataRequestContext requestContext) {
        refuseIfRegistered(dbName, tblName, "list its partitions");
        return super.listPartitionNames(dbName, tblName, requestContext);
    }

    @Override
    public List<String> listPartitionNamesByValue(String dbName, String tblName,
                                                  List<Optional<String>> partitionValues) {
        refuseIfRegistered(dbName, tblName, "list its partitions");
        return super.listPartitionNamesByValue(dbName, tblName, partitionValues);
    }

    // ---------------------------------------------------------------------------------------------------
    // Mutators. Refused wholesale: this version has not implemented or verified writing through
    // Lake Formation, and several of these reach the remote Glue catalog directly.
    // ---------------------------------------------------------------------------------------------------

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

    /**
     * Overridden even though the base class appears to delegate: HiveMetadata does implement it, and it runs
     * HiveAlterTableExecutor against the remote Glue catalog.
     */
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
        memo.clear();
        super.clear();
    }

    /**
     * awsCatalogId is read straight off the catalog properties, which is also where the gateway reads it, so
     * the id named in an error message is always the one the request used. It is nullable, and absent means
     * the caller's own AWS account.
     */
    private LakeFormationTableIdentity identityOf(String dbName, String tblName) {
        return new LakeFormationTableIdentity(lakeFormationCatalogName,
                lakeFormationProperties.awsCatalogId(),
                lakeFormationProperties.region(), dbName, tblName);
    }

    /**
     * The memo key, lower-cased. Hive and Glue treat database and table names case insensitively, so two
     * spellings of the same table in one statement must not resolve twice - that would be a second Lake
     * Formation call and a second Table instance for the same table.
     *
     * The request itself still uses the caller's spelling: only the key is normalized.
     */
    private LakeFormationTableIdentity memoKey(String dbName, String tblName) {
        return identityOf(dbName == null ? null : dbName.toLowerCase(Locale.ROOT),
                tblName == null ? null : tblName.toLowerCase(Locale.ROOT));
    }

    /**
     * This version authorizes metadata only. Reading a registered table's data would mean falling back to
     * the catalog's own credentials, which is precisely what a Lake Formation deployment forbids.
     */
    private void refuseDataAccess(Table table, String what) {
        if (table instanceof LakeFormationHiveTable lfTable) {
            throw new LakeFormationTableAccessException(
                    "Cannot " + what + " for " + lfTable.getLakeFormationIdentity()
                            + ": this version authorizes Lake Formation metadata but does not vend data"
                            + " credentials, and falling back to the catalog's own credentials is not allowed.");
        }
        // A caller can hand back a plain HiveTable it obtained earlier - the statistics collector keeps the
        // Table it was created with, and refreshTable is reached that way. The type check alone would let
        // that through, and refreshTable is the one entry point that ends in a bare Glue load whose result
        // is put into the cross-query table cache, so it gets the same deny-by-default treatment the
        // name-based entry points get.
        if (table instanceof HiveTable hiveTable) {
            LakeFormationTableResolution resolution =
                    memo.get(memoKey(hiveTable.getCatalogDBName(), hiveTable.getCatalogTableName()));
            if (resolution != null && !resolution.isUnregistered()) {
                throw new LakeFormationTableAccessException("Cannot " + what + " for "
                        + hiveTable.getCatalogDBName() + "." + hiveTable.getCatalogTableName()
                        + " on Lake Formation catalog " + lakeFormationCatalogName
                        + ": this version does not read a registered table's data.");
            }
        }
    }

    /**
     * Deny by default. An empty memo does not mean "not registered", it means nobody has resolved this table
     * on this metadata instance yet - which is the normal state on any freshly created instance. Only an
     * explicitly memoized "unregistered" may take the ordinary path.
     *
     * The consequence, which is deliberate: a table that really is unregistered is also refused here if
     * nothing resolved it on this instance first. Deciding otherwise would need either a live registration
     * check on this path - turning partition enumeration into a Lake Formation caller - or a registration
     * cache that is trustworthy across principals. Neither belongs in this version.
     */
    private void refuseIfRegistered(String dbName, String tblName, String what) {
        LakeFormationTableResolution resolution = memo.get(memoKey(dbName, tblName));
        if (resolution != null && resolution.isUnregistered()) {
            return;
        }
        throw new LakeFormationTableAccessException("Cannot " + what + " for "
                + identityOf(dbName, tblName) + " without first authorizing the table with Lake Formation."
                + " This version does not authorize partition level access.");
    }

    private LakeFormationTableAccessException refuseMutation(String what) {
        return new LakeFormationTableAccessException(what + " is not supported on Lake Formation catalog "
                + lakeFormationCatalogName + " in this version.");
    }
}
