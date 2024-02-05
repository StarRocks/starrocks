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


package com.starrocks.server;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalCatalogTableBasicInfo;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.BasicTable;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.ConnectorTableColumnStats;
import com.starrocks.connector.ConnectorTblMetaInfoMgr;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.thrift.TSinkCommitInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MetadataMgr {
    private static final Logger LOG = LogManager.getLogger(MetadataMgr.class);

    public class QueryMetadatas {
        private final Map<String, ConnectorMetadata> metadatas = new HashMap<>();

        public QueryMetadatas() {}

        public synchronized ConnectorMetadata getConnectorMetadata(String catalogName, String queryId) {
            if (metadatas.containsKey(catalogName)) {
                return metadatas.get(catalogName);
            }

            CatalogConnector connector = connectorMgr.getConnector(catalogName);
            if (connector == null) {
                LOG.error("Connector [{}] doesn't exist", catalogName);
                return null;
            }
            ConnectorMetadata connectorMetadata = connector.getMetadata();
            metadatas.put(catalogName, connectorMetadata);
            LOG.info("Succeed to register query level connector metadata [catalog:{}, queryId: {}]", catalogName, queryId);
            return connectorMetadata;
        }
    }

    private final LocalMetastore localMetastore;
    private final ConnectorMgr connectorMgr;
    private final ConnectorTblMetaInfoMgr connectorTblMetaInfoMgr;

    private static final RemovalListener<String, QueryMetadatas> CACHE_REMOVAL_LISTENER = (notification) -> {
        String queryId = notification.getKey();
        QueryMetadatas meta = notification.getValue();
        if (meta != null) {
            meta.metadatas.values().forEach(ConnectorMetadata::clear);
            if (notification.getCause() != RemovalCause.EXPLICIT) {
                LOG.info("Evict cache due to {} and deregister query-level " +
                        "connector metadata on query id: {}", notification.getCause(), queryId);
            }
        }
    };

    private final LoadingCache<String, QueryMetadatas> metadataCacheByQueryId =
            CacheBuilder.newBuilder()
                    .maximumSize(Config.catalog_metadata_cache_size)
                    .expireAfterAccess(300, TimeUnit.SECONDS)
                    .removalListener(CACHE_REMOVAL_LISTENER)
                    .build(new CacheLoader<String, QueryMetadatas>() {
                        @NotNull
                        @Override
                        public QueryMetadatas load(String key) throws Exception {
                            return new QueryMetadatas();
                        }
                    });

    public MetadataMgr(LocalMetastore localMetastore, ConnectorMgr connectorMgr,
                       ConnectorTblMetaInfoMgr connectorTblMetaInfoMgr) {
        Preconditions.checkNotNull(localMetastore, "localMetastore is null");
        this.localMetastore = localMetastore;
        this.connectorMgr = connectorMgr;
        this.connectorTblMetaInfoMgr = connectorTblMetaInfoMgr;
    }

    // get query id from thread local context if possible
    private Optional<String> getOptionalQueryID() {
        if (ConnectContext.get() != null && ConnectContext.get().getQueryId() != null) {
            return Optional.of(ConnectContext.get().getQueryId().toString());
        }
        return Optional.empty();
    }


    /** get ConnectorMetadata by catalog name
     * if catalog is null or empty will return localMetastore
     * @param catalogName catalog's name
     * @return ConnectorMetadata
     */
    public Optional<ConnectorMetadata> getOptionalMetadata(String catalogName) {
        if (Strings.isNullOrEmpty(catalogName) || CatalogMgr.isInternalCatalog(catalogName)) {
            return Optional.of(localMetastore);
        }

        CatalogConnector connector = connectorMgr.getConnector(catalogName);
        if (connector == null) {
            LOG.error("Failed to get {} catalog", catalogName);
            return Optional.empty();
        }

        Optional<String> queryId = getOptionalQueryID();
        if (queryId.isPresent()) { // use query-level cache if from query
            QueryMetadatas queryMetadatas = metadataCacheByQueryId.getUnchecked(queryId.get());
            return Optional.ofNullable(queryMetadatas.getConnectorMetadata(catalogName, queryId.get()));
        }

        return Optional.ofNullable(connector.getMetadata());
    }

    public void removeQueryMetadata() {
        Optional<String> queryId = getOptionalQueryID();
        if (queryId.isPresent()) {
            QueryMetadatas queryMetadatas = metadataCacheByQueryId.getIfPresent(queryId.get());
            if (queryMetadatas != null) {
                queryMetadatas.metadatas.values().forEach(ConnectorMetadata::clear);
                metadataCacheByQueryId.invalidate(queryId.get());
                LOG.info("Succeed to deregister query level connector metadata on query id: {}", queryId);
            }
        }
    }

    public List<String> listDbNames(String catalogName) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        ImmutableSet.Builder<String> dbNames = ImmutableSet.builder();

        if (connectorMetadata.isPresent()) {
            try {
                connectorMetadata.get().listDbNames().forEach(dbNames::add);
            } catch (StarRocksConnectorException e) {
                LOG.error("Failed to listDbNames on catalog {}", catalogName, e);
                throw e;
            }
        }
        return ImmutableList.copyOf(dbNames.build());
    }

    public void createDb(String catalogName, String dbName, Map<String, String> properties)
            throws DdlException, AlreadyExistsException {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        if (connectorMetadata.isPresent()) {
            connectorMetadata.get().createDb(dbName, properties);
        }
    }

    public void dropDb(String catalogName, String dbName, boolean isForce) throws DdlException, MetaNotFoundException {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        if (connectorMetadata.isPresent()) {
            if (getDb(catalogName, dbName) == null) {
                throw new MetaNotFoundException(String.format("Database %s.%s doesn't exists", catalogName, dbName));
            }
            connectorMetadata.get().dropDb(dbName, isForce);
        }
    }

    public Database getDb(String catalogName, String dbName) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        Database db = connectorMetadata.map(metadata -> metadata.getDb(dbName)).orElse(null);
        // set catalog name if external catalog
        if (db != null && CatalogMgr.isExternalCatalog(catalogName)) {
            db.setCatalogName(catalogName);
        }
        return db;
    }

    public Database getDb(Long databaseId) {
        return localMetastore.getDb(databaseId);
    }

    public List<String> listTableNames(String catalogName, String dbName) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
        if (connectorMetadata.isPresent()) {
            try {
                connectorMetadata.get().listTableNames(dbName).forEach(tableNames::add);
            } catch (Exception e) {
                LOG.error("Failed to listTableNames on [{}.{}]", catalogName, dbName, e);
                throw e;
            }
        }
        return ImmutableList.copyOf(tableNames.build());
    }

    public boolean createTable(CreateTableStmt stmt) throws DdlException {
        String catalogName = stmt.getCatalogName();
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);

        if (connectorMetadata.isPresent()) {
            if (!CatalogMgr.isInternalCatalog(catalogName)) {
                String dbName = stmt.getDbName();
                String tableName = stmt.getTableName();
                if (getDb(catalogName, dbName) == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
                }

                if (tableExists(catalogName, dbName, tableName)) {
                    if (stmt.isSetIfNotExists()) {
                        LOG.info("create table[{}] which already exists", tableName);
                        return false;
                    } else {
                        ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
                    }
                }
            }
            return connectorMetadata.get().createTable(stmt);
        } else {
            throw new  DdlException("Invalid catalog " + catalogName + " , ConnectorMetadata doesn't exist");
        }
    }

    public void alterTable(AlterTableStmt stmt) throws UserException {
        String catalogName = stmt.getCatalogName();
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);

        if (connectorMetadata.isPresent()) {
            String dbName = stmt.getDbName();
            String tableName = stmt.getTableName();
            if (getDb(catalogName, dbName) == null) {
                throw new DdlException("Database '" + dbName + "' does not exist in catalog '" + catalogName + "'");
            }

            if (!tableExists(catalogName, dbName, tableName)) {
                throw new DdlException("Table '" + tableName + "' does not exist in database '" + dbName + "'");
            }

            connectorMetadata.get().alterTable(stmt);
        } else {
            throw new  DdlException("Invalid catalog " + catalogName + " , ConnectorMetadata doesn't exist");
        }
    }

    public void dropTable(String catalogName, String dbName, String tblName) {
        TableName tableName = new TableName(catalogName, dbName, tblName);
        DropTableStmt dropTableStmt = new DropTableStmt(false, tableName, false);
        dropTable(dropTableStmt);
    }

    public void dropTable(DropTableStmt stmt) {
        String catalogName = stmt.getCatalogName();
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();

        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        connectorMetadata.ifPresent(metadata -> {
            try {
                metadata.dropTable(stmt);
            } catch (DdlException e) {
                LOG.error("Failed to drop table {}.{}.{}", catalogName, dbName, tableName, e);
                throw new StarRocksConnectorException("Failed to drop table %s.%s.%s. msg: %s",
                        catalogName, dbName, tableName, e.getMessage());
            }
        });
    }

    public Optional<Table> getTable(TableName tableName) {
        return Optional.ofNullable(getTable(tableName.getCatalog(), tableName.getDb(), tableName.getTbl()));
    }

    public Table getTable(String catalogName, String dbName, String tblName) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        Table connectorTable = connectorMetadata.map(metadata -> metadata.getTable(dbName, tblName)).orElse(null);
        if (connectorTable != null) {
            // Load meta information from ConnectorTblMetaInfoMgr for each external table.
            connectorTblMetaInfoMgr.setTableInfoForConnectorTable(catalogName, dbName, connectorTable);
        }
        return connectorTable;
    }

    public Table getTable(Long databaseId, Long tableId) {
        Database database = localMetastore.getDb(databaseId);
        if (database == null) {
            return null;
        }
        return database.getTable(tableId);
    }

    public boolean tableExists(String catalogName, String dbName, String tblName) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        return connectorMetadata.map(metadata -> metadata.tableExists(dbName, tblName)).orElse(false);
    }
        
    /**
     * getTableLocally avoids network interactions with external metadata service when using external catalog(e.g. hive catalog).
     * In this case, only basic information of namespace and table type (derived from the type of its connector) is returned.
     * For default/internal catalog, this method is equivalent to {@link MetadataMgr#getTable(String, String, String)}.
     * Use this method if you are absolutely sure, otherwise use MetadataMgr#getTable.
     */
    public BasicTable getBasicTable(String catalogName, String dbName, String tblName) {
        if (CatalogMgr.isInternalCatalog(catalogName)) {
            return getTable(catalogName, dbName, tblName);
        }

        // for external catalog, do not reach external metadata service
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        return connectorMetadata.map(
                        metadata -> new ExternalCatalogTableBasicInfo(catalogName, dbName, tblName, metadata.getTableType()))
                .orElse(null);
    }

    public Pair<Table, MaterializedIndexMeta> getMaterializedViewIndex(String catalogName, String dbName, String tblName) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        return connectorMetadata.map(metadata -> metadata.getMaterializedViewIndex(dbName, tblName)).orElse(null);
    }

    public List<String> listPartitionNames(String catalogName, String dbName, String tableName) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        ImmutableSet.Builder<String> partitionNames = ImmutableSet.builder();
        if (connectorMetadata.isPresent()) {
            try {
                connectorMetadata.get().listPartitionNames(dbName, tableName).forEach(partitionNames::add);
            } catch (Exception e) {
                LOG.error("Failed to listPartitionNames on [{}.{}]", catalogName, dbName, e);
                throw e;
            }
        }
        return ImmutableList.copyOf(partitionNames.build());
    }

    /**
     * List partition names by partition values, The partition values are in the same order as the partition columns,
     * it used for get partial partition names from hms/glue
     * <p>
     * For example:
     * SQL ： select dt,hh,mm from tbl where hh = '12' and mm = '30';
     * the partition columns are [dt,hh,mm]
     * the partition values should be [empty,'12','30']
     *
     */
    public List<String> listPartitionNamesByValue(String catalogName, String dbName, String tableName,
                                                  List<Optional<String>> partitionValues) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        ImmutableSet.Builder<String> partitionNames = ImmutableSet.builder();
        if (connectorMetadata.isPresent()) {
            try {
                connectorMetadata.get().listPartitionNamesByValue(dbName, tableName, partitionValues).
                        forEach(partitionNames::add);
            } catch (Exception e) {
                LOG.error("Failed to listPartitionNamesByValue on [{}.{}]", catalogName, dbName, e);
                throw e;
            }
        }
        return ImmutableList.copyOf(partitionNames.build());
    }

    public List<PartitionKey> getPrunedPartitions(String catalogName, Table table, ScalarOperator predicate, long limit) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        if (connectorMetadata.isPresent()) {
            try {
                return connectorMetadata.get().getPrunedPartitions(table, predicate, limit);
            } catch (Exception e) {
                LOG.error("Failed to getPrunedPartitions on [{}.{}]", catalogName, table, e);
                throw e;
            }
        }
        return new ArrayList<>();
    }

    public Statistics getTableStatisticsFromInternalStatistics(Table table, Map<ColumnRefOperator, Column> columns) {
        List<ColumnRefOperator> requiredColumnRefs = new ArrayList<>(columns.keySet());
        List<String> columnNames = requiredColumnRefs.stream().map(col -> columns.get(col).getName()).collect(
                Collectors.toList());
        List<ConnectorTableColumnStats> columnStatisticList =
                GlobalStateMgr.getCurrentStatisticStorage().getConnectorTableStatistics(table, columnNames);

        Statistics.Builder statistics = Statistics.builder();
        for (int i = 0; i < requiredColumnRefs.size(); ++i) {
            ColumnRefOperator columnRef = requiredColumnRefs.get(i);
            ConnectorTableColumnStats connectorTableColumnStats = columnStatisticList.get(i);
            if (connectorTableColumnStats != null) {
                statistics.addColumnStatistic(columnRef, connectorTableColumnStats.getColumnStatistic());
                if (!connectorTableColumnStats.isUnknown()) {
                    statistics.setOutputRowCount(connectorTableColumnStats.getRowCount());
                }
            }
        }
        return statistics.build();
    }


    public Statistics getTableStatistics(OptimizerContext session,
                                         String catalogName,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate,
                                         long limit) {
        // FIXME: In testing env, `_statistics_.external_column_statistics` is not created, ignore query columns stats from it.
        Statistics statistics = FeConstants.runningUnitTest ? null :
                getTableStatisticsFromInternalStatistics(table, columns);
        if (statistics == null || statistics.getColumnStatistics().values().stream().allMatch(ColumnStatistic::isUnknown)) {
            session.setObtainedFromInternalStatistics(false);
            Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
            return connectorMetadata.map(metadata -> metadata.getTableStatistics(
                    session, table, columns, partitionKeys, predicate, limit)).orElse(null);
        } else {
            session.setObtainedFromInternalStatistics(true);
            return statistics;
        }
    }

    public Statistics getTableStatistics(OptimizerContext session,
                                         String catalogName,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate) {
        return getTableStatistics(session, catalogName, table, columns, partitionKeys, predicate, -1);
    }

    public List<RemoteFileInfo> getRemoteFileInfos(String catalogName, Table table, List<PartitionKey> partitionKeys) {
        return getRemoteFileInfos(catalogName, table, partitionKeys, -1, null, null, -1);
    }

    public List<RemoteFileInfo> getRemoteFileInfos(String catalogName, Table table, List<PartitionKey> partitionKeys,
                                                   long snapshotId, ScalarOperator predicate, List<String> fieldNames,
                                                   long limit) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        ImmutableSet.Builder<RemoteFileInfo> files = ImmutableSet.builder();
        if (connectorMetadata.isPresent()) {
            try {
                connectorMetadata.get().getRemoteFileInfos(table, partitionKeys, snapshotId, predicate, fieldNames, limit)
                        .forEach(files::add);
            } catch (Exception e) {
                LOG.error("Failed to list remote file's metadata on catalog [{}], table [{}]", catalogName, table, e);
                throw e;
            }
        }
        return ImmutableList.copyOf(files.build());
    }

    public List<PartitionInfo> getPartitions(String catalogName, Table table, List<String> partitionNames) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        ImmutableList.Builder<PartitionInfo> partitions = ImmutableList.builder();
        if (connectorMetadata.isPresent()) {
            try {
                connectorMetadata.get().getPartitions(table, partitionNames).forEach(partitions::add);
            } catch (Exception e) {
                LOG.error("Failed to get partitions on catalog [{}], table [{}]", catalogName, table, e);
                throw e;
            }
        }
        return partitions.build();
    }

    public void refreshTable(String catalogName, String srDbName, Table table,
                             List<String> partitionNames, boolean onlyCachedPartitions) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        connectorMetadata.ifPresent(metadata -> metadata.refreshTable(srDbName, table, partitionNames, onlyCachedPartitions));
    }

    public void finishSink(String catalogName, String dbName, String tableName, List<TSinkCommitInfo> sinkCommitInfos) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        connectorMetadata.ifPresent(metadata -> {
            try {
                metadata.finishSink(dbName, tableName, sinkCommitInfos);
            } catch (StarRocksConnectorException e) {
                LOG.error("table sink commit failed", e);
                throw new StarRocksConnectorException(e.getMessage());
            }
        });
    }
}
