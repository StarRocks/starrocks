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
import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.BasicTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalCatalogTableBasicInfo;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.ConnectorTableVersion;
import com.starrocks.connector.ConnectorTblMetaInfoMgr;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.MetaPreparationItem;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.SerializedMetaSpec;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metadata.MetadataTable;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.connector.statistics.ConnectorTableColumnStats;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.CleanTemporaryTableStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateTemporaryTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.DropTemporaryTableStmt;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Histogram;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TSinkCommitInfo;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MetadataMgr {
    private static final Logger LOG = LogManager.getLogger(MetadataMgr.class);

    public class QueryMetadatas {
        private final Map<String, ConnectorMetadata> metadatas = new HashMap<>();

        public QueryMetadatas() {
        }

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
    private final TemporaryTableMgr temporaryTableMgr;
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

    public MetadataMgr(LocalMetastore localMetastore, TemporaryTableMgr temporaryTableMgr, ConnectorMgr connectorMgr,
                       ConnectorTblMetaInfoMgr connectorTblMetaInfoMgr) {
        Preconditions.checkNotNull(localMetastore, "localMetastore is null");
        Preconditions.checkNotNull(temporaryTableMgr, "temporaryTableMgr is null");
        this.localMetastore = localMetastore;
        this.temporaryTableMgr = temporaryTableMgr;
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

    public Optional<ConnectorMetadata> getOptionalMetadata(String catalogName) {
        Optional<String> queryId = getOptionalQueryID();
        return getOptionalMetadata(queryId, catalogName);
    }

    /**
     * get ConnectorMetadata by catalog name
     * if catalog is null or empty will return localMetastore
     *
     * @param catalogName catalog's name
     * @return ConnectorMetadata
     */
    public Optional<ConnectorMetadata> getOptionalMetadata(Optional<String> queryId, String catalogName) {
        if (Strings.isNullOrEmpty(catalogName) || CatalogMgr.isInternalCatalog(catalogName)) {
            return Optional.of(localMetastore);
        }

        CatalogConnector connector = connectorMgr.getConnector(catalogName);
        if (connector == null) {
            LOG.error("Failed to get {} catalog", catalogName);
            return Optional.empty();
        }

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

    public List<String> listDbNames(ConnectContext context, String catalogName) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        ImmutableSet.Builder<String> dbNames = ImmutableSet.builder();

        if (connectorMetadata.isPresent()) {
            try {
                connectorMetadata.get().listDbNames(context).forEach(dbNames::add);
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

    public void dropDb(ConnectContext context, String catalogName, String dbName, boolean isForce)
            throws DdlException, MetaNotFoundException {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        if (connectorMetadata.isPresent()) {
            if (getDb(context, catalogName, dbName) == null) {
                throw new MetaNotFoundException(String.format("Database %s.%s doesn't exists", catalogName, dbName));
            }
            connectorMetadata.get().dropDb(context, dbName, isForce);
        }
    }

    public Database getDb(ConnectContext context, String catalogName, String dbName) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        Database db = connectorMetadata.map(metadata -> metadata.getDb(context, dbName)).orElse(null);
        // set catalog name if external catalog
        if (db != null && CatalogMgr.isExternalCatalog(catalogName)) {
            db.setCatalogName(catalogName);
        }
        return db;
    }

    public Database getDb(Long databaseId) {
        return localMetastore.getDb(databaseId);
    }

    public List<String> listTableNames(ConnectContext context, String catalogName, String dbName) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
        if (connectorMetadata.isPresent()) {
            try {
                connectorMetadata.get().listTableNames(context, dbName).forEach(tableNames::add);
            } catch (Exception e) {
                LOG.error("Failed to listTableNames on [{}.{}]", catalogName, dbName, e);
                throw e;
            }
        }
        return ImmutableList.copyOf(tableNames.build());
    }

    public boolean createTable(ConnectContext context, CreateTableStmt stmt) throws DdlException {
        String catalogName = stmt.getCatalogName();
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);

        if (connectorMetadata.isPresent()) {
            if (!CatalogMgr.isInternalCatalog(catalogName)) {
                String dbName = stmt.getDbName();
                String tableName = stmt.getTableName();
                if (getDb(context, catalogName, dbName) == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
                }

                if (tableExists(context, catalogName, dbName, tableName)) {
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
            throw new DdlException("Invalid catalog " + catalogName + " , ConnectorMetadata doesn't exist");
        }
    }

    public void createTableLike(ConnectContext context, CreateTableLikeStmt stmt) throws DdlException {
        String catalogName = stmt.getCatalogName();
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);

        if (connectorMetadata.isPresent()) {
            String dbName = stmt.getDbName();
            String tableName = stmt.getTableName();
            if (tableExists(context, catalogName, dbName, tableName)) {
                if (stmt.isSetIfNotExists()) {
                    LOG.info("create table[{}] which already exists", tableName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
                }
            }
            connectorMetadata.get().createTableLike(stmt);
        } else {
            throw new DdlException("Invalid catalog " + catalogName + " , ConnectorMetadata doesn't exist");
        }
    }

    public boolean createTemporaryTable(CreateTemporaryTableStmt stmt) throws DdlException {
        Preconditions.checkArgument(stmt.getSessionId() != null,
                "session id should not be null in CreateTemporaryTableStmt");
        String catalogName = stmt.getCatalogName();

        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        if (!connectorMetadata.isPresent()) {
            throw new DdlException("Invalid catalog " + catalogName + ", ConnectorMetadata doesn't exist");
        } else if (!CatalogMgr.isInternalCatalog(catalogName)) {
            throw new DdlException("temporary table must be created under internal catalog");
        } else {
            String dbName = stmt.getDbName();
            Database db = localMetastore.getDb(dbName);
            if (db == null) {
                throw new DdlException("Database '" + dbName + "' does not exist in catalog '" + catalogName + "'");
            }

            TemporaryTableMgr temporaryTableMgr = GlobalStateMgr.getCurrentState().getTemporaryTableMgr();
            String tableName = stmt.getTableName();
            UUID sessionId = stmt.getSessionId();

            if (temporaryTableMgr.tableExists(sessionId, db.getId(), tableName)) {
                if (stmt.isSetIfNotExists()) {
                    LOG.info("create temporary table[{}.{}] which already exists in session[{}]",
                            dbName, tableName, sessionId);
                    return false;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
                }
            }
            return connectorMetadata.get().createTable(stmt);
        }
    }

    public void createView(CreateViewStmt stmt) throws DdlException {
        String catalogName = stmt.getCatalog();
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);

        if (connectorMetadata.isPresent()) {
            connectorMetadata.get().createView(stmt);
        }
    }

    public void alterTable(ConnectContext context, AlterTableStmt stmt) throws StarRocksException {
        String catalogName = stmt.getCatalogName();
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);

        if (connectorMetadata.isPresent()) {
            String dbName = stmt.getDbName();
            String tableName = stmt.getTableName();
            if (getDb(context, catalogName, dbName) == null) {
                throw new DdlException("Database '" + dbName + "' does not exist in catalog '" + catalogName + "'");
            }

            if (!tableExists(context, catalogName, dbName, tableName)) {
                throw new DdlException("Table '" + tableName + "' does not exist in database '" + dbName + "'");
            }

            connectorMetadata.get().alterTable(context, stmt);
        } else {
            throw new DdlException("Invalid catalog " + catalogName + " , ConnectorMetadata doesn't exist");
        }
    }

    public void alterView(AlterViewStmt stmt) throws StarRocksException {
        String catalogName = stmt.getCatalog();
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);

        if (connectorMetadata.isPresent()) {
            connectorMetadata.get().alterView(stmt);
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

    public void dropTemporaryTable(DropTemporaryTableStmt stmt) {
        Preconditions.checkArgument(stmt.getSessionId() != null,
                "session id should not be null in DropTemporaryTableStmt");
        String catalogName = stmt.getCatalogName();
        if (!CatalogMgr.isInternalCatalog(catalogName)) {
            throw new StarRocksConnectorException("temporary table must be under internal catalog");
        }
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        connectorMetadata.ifPresent(metadata -> {
            String dbName = stmt.getDbName();
            Database db = localMetastore.getDb(dbName);
            if (db == null) {
                throw new StarRocksConnectorException(
                        "Database '" + dbName + "' does not exist in catalog '" + catalogName + "'");
            }

            TemporaryTableMgr temporaryTableMgr = GlobalStateMgr.getCurrentState().getTemporaryTableMgr();
            String tableName = stmt.getTableName();
            UUID sessionId = stmt.getSessionId();

            Long tableId = temporaryTableMgr.getTable(sessionId, db.getId(), tableName);
            if (tableId == null) {
                if (stmt.isSetIfExists()) {
                    LOG.debug("drop temporary table[{}.{}] which doesn't exist in session[{}]",
                            dbName, tableName, sessionId);
                    return;
                } else {
                    throw new StarRocksConnectorException("Temporary table '" + tableName + "' doesn't exist");
                }
            }
            try {
                metadata.dropTemporaryTable(dbName, tableId, tableName, stmt.isSetIfExists(), stmt.isForceDrop());
                temporaryTableMgr.dropTemporaryTable(sessionId, db.getId(), tableName);
            } catch (DdlException e) {
                LOG.error("Failed to drop temporary table {}.{}.{} in session {}, {}",
                        catalogName, dbName, tableName, sessionId, e);
                throw new StarRocksConnectorException("Failed to drop temporary table %s.%s.%s. msg: %s",
                        catalogName, dbName, tableName, e.getMessage());
            }
        });
    }

    public void cleanTemporaryTables(CleanTemporaryTableStmt stmt) {
        Preconditions.checkArgument(stmt.getSessionId() != null,
                "session id should not be null in DropTemporaryTableStmt");
        cleanTemporaryTables(stmt.getSessionId());
    }

    public void cleanTemporaryTables(UUID sessionId) {
        TemporaryTableMgr temporaryTableMgr = GlobalStateMgr.getCurrentState().getTemporaryTableMgr();
        com.google.common.collect.Table<Long, String, Long> allTables = temporaryTableMgr.getTemporaryTables(sessionId);

        for (Long databaseId : allTables.rowKeySet()) {
            Database database = localMetastore.getDb(databaseId);
            if (database == null) {
                // database maybe dropped by force, we should clean temporary tables on it.
                temporaryTableMgr.dropTemporaryTables(sessionId, databaseId);
                continue;
            }
            Map<String, Long> tables = allTables.row(databaseId);
            tables.forEach((tableName, tableId) -> {
                try {
                    database.dropTemporaryTable(tableId, tableName, true, true);
                    temporaryTableMgr.dropTemporaryTable(sessionId, database.getId(), tableName);
                } catch (DdlException e) {
                    LOG.error("Failed to drop temporary table {}.{} in session {}",
                            database.getFullName(), tableName, sessionId, e);
                }
            });
        }
        temporaryTableMgr.removeTemporaryTables(sessionId);
    }

    public Optional<Table> getTable(ConnectContext context, TableName tableName) {
        return Optional.ofNullable(getTable(context, tableName.getCatalog(), tableName.getDb(), tableName.getTbl()));
    }

    public Table getTable(ConnectContext context, String catalogName, String dbName, String tblName) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        Table connectorTable = connectorMetadata.map(metadata -> metadata.getTable(context, dbName, tblName)).orElse(null);
        if (connectorTable != null && !connectorTable.isMetadataTable()) {
            // Load meta information from ConnectorTblMetaInfoMgr for each external table.
            connectorTblMetaInfoMgr.setTableInfoForConnectorTable(catalogName, dbName, connectorTable);
        }

        if (connectorTable != null && connectorTable.isMetadataTable()) {
            MetadataTable metadataTable = (MetadataTable) connectorTable;
            String originDbName = metadataTable.getOriginDb();
            String originTableName = metadataTable.getOriginTable();
            boolean originTableExist = connectorMetadata.map(metadata ->
                    metadata.tableExists(context, originDbName, originTableName)).orElse(false);
            if (!originTableExist) {
                LOG.error("origin table not exists with {}.{}.{}", catalogName, dbName, tblName);
                return null;
            }
        }
        return connectorTable;
    }

    public TableVersionRange getTableVersionRange(String dbName, Table table,
                                                  Optional<ConnectorTableVersion> startVersion,
                                                  Optional<ConnectorTableVersion> endVersion) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(table.getCatalogName());
        return connectorMetadata.map(metadata -> metadata.getTableVersionRange(dbName, table, startVersion, endVersion))
                .orElse(TableVersionRange.empty().empty());
    }

    public Optional<Database> getDatabase(ConnectContext context, BaseTableInfo baseTableInfo) {
        if (baseTableInfo.isInternalCatalog()) {
            return Optional.ofNullable(getDb(baseTableInfo.getDbId()));
        } else {
            return Optional.ofNullable(getDb(context, baseTableInfo.getCatalogName(), baseTableInfo.getDbName()));
        }
    }

    public Optional<Table> getTable(ConnectContext context, BaseTableInfo baseTableInfo) {
        if (baseTableInfo.isInternalCatalog()) {
            return Optional.ofNullable(localMetastore.getTable(baseTableInfo.getDbId(), baseTableInfo.getTableId()));
        } else {
            return Optional.ofNullable(
                    getTable(context, baseTableInfo.getCatalogName(), baseTableInfo.getDbName(), baseTableInfo.getTableName()));
        }
    }

    public Optional<Table> getTableWithIdentifier(ConnectContext context, BaseTableInfo baseTableInfo) {
        Optional<Table> tableOpt = getTable(context, baseTableInfo);
        if (baseTableInfo.isInternalCatalog()) {
            return tableOpt;
        } else if (tableOpt.isPresent()) {
            Table table = tableOpt.get();
            String tableIdentifier = baseTableInfo.getTableIdentifier();
            if (tableIdentifier != null && tableIdentifier.equals(table.getTableIdentifier())) {
                return tableOpt;
            }
        }
        return Optional.empty();
    }

    public Table getTableChecked(ConnectContext context, BaseTableInfo baseTableInfo) {
        Optional<Table> tableOpt = getTableWithIdentifier(context, baseTableInfo);
        if (tableOpt.isPresent()) {
            return tableOpt.get();
        }
        throw MaterializedViewExceptions.reportBaseTableNotExists(baseTableInfo);
    }

    public Table getTemporaryTable(UUID sessionId, String catalogName, Long databaseId, String tblName) {
        if (!CatalogMgr.isInternalCatalog(catalogName)) {
            return null;
        }
        Long tableId = temporaryTableMgr.getTable(sessionId, databaseId, tblName);
        if (tableId == null) {
            return null;
        }
        Database database = localMetastore.getDb(databaseId);
        if (database == null) {
            return null;
        }
        return localMetastore.getTable(database.getId(), tableId);
    }

    public boolean tableExists(ConnectContext context, String catalogName, String dbName, String tblName) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        return connectorMetadata.map(metadata -> metadata.tableExists(context, dbName, tblName)).orElse(false);
    }

    /**
     * getTableLocally avoids network interactions with external metadata service when using external catalog(e.g. hive catalog).
     * In this case, only basic information of namespace and table type (derived from the type of its connector) is returned.
     * For default/internal catalog, this method is equivalent to
     * {@link MetadataMgr#getTable(ConnectContext, String, String, String)}.
     * Use this method if you are absolutely sure, otherwise use MetadataMgr#getTable.
     */
    public BasicTable getBasicTable(ConnectContext context, String catalogName, String dbName, String tblName) {
        if (catalogName == null) {
            return getTable(context, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, dbName, tblName);
        }

        if (CatalogMgr.isInternalCatalog(catalogName)) {
            return getTable(context, catalogName, dbName, tblName);
        }

        // for external catalog, do not reach external metadata service
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        return connectorMetadata.map(
                        metadata -> new ExternalCatalogTableBasicInfo(catalogName, dbName, tblName, metadata.getTableType()))
                .orElse(null);
    }

    public List<String> listPartitionNames(String catalogName, String dbName, String tableName,
                                           ConnectorMetadatRequestContext requestContext) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        ImmutableSet.Builder<String> partitionNames = ImmutableSet.builder();
        if (connectorMetadata.isPresent()) {
            try {
                connectorMetadata.get().listPartitionNames(dbName, tableName, requestContext).forEach(partitionNames::add);
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

    public Statistics getTableStatisticsFromInternalStatistics(Table table, Map<ColumnRefOperator, Column> columns) {
        List<ColumnRefOperator> requiredColumnRefs = new ArrayList<>(columns.keySet());
        List<String> columnNames = requiredColumnRefs.stream().map(col -> columns.get(col).getName()).collect(
                Collectors.toList());
        List<ConnectorTableColumnStats> columnStatisticList =
                GlobalStateMgr.getCurrentState().getStatisticStorage().getConnectorTableStatistics(table, columnNames);

        Map<String, Histogram> histogramStatistics =
                GlobalStateMgr.getCurrentState().getStatisticStorage().getConnectorHistogramStatistics(table, columnNames);

        Statistics.Builder statistics = Statistics.builder();
        for (int i = 0; i < requiredColumnRefs.size(); ++i) {
            ColumnRefOperator columnRef = requiredColumnRefs.get(i);
            ConnectorTableColumnStats connectorTableColumnStats;
            if (histogramStatistics.containsKey(columnRef.getName())) {
                // add histogram to column statistic
                Histogram histogram = histogramStatistics.get(columnRef.getName());
                connectorTableColumnStats = new ConnectorTableColumnStats(
                        ColumnStatistic.buildFrom(columnStatisticList.get(i).getColumnStatistic()).
                                setHistogram(histogram).build(),
                        columnStatisticList.get(i).getRowCount(), columnStatisticList.get(i).getUpdateTime());
            } else {
                connectorTableColumnStats = columnStatisticList.get(i);
            }
            if (connectorTableColumnStats != null) {
                statistics.addColumnStatistic(columnRef, connectorTableColumnStats.getColumnStatistic());
                if (!connectorTableColumnStats.isUnknown()) {
                    double rowCount = Double.isNaN(statistics.getOutputRowCount()) ? connectorTableColumnStats.getRowCount()
                            : Math.max(statistics.getOutputRowCount(), connectorTableColumnStats.getRowCount());
                    statistics.setOutputRowCount(rowCount);
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
                                         long limit,
                                         TableVersionRange versionRange) {
        // FIXME: In testing env, `_statistics_.external_column_statistics` is not created, ignore query columns stats from it.
        // Get basic/histogram stats from internal statistics.
        Statistics internalStatistics = FeConstants.runningUnitTest ? null :
                getTableStatisticsFromInternalStatistics(table, columns);
        if (internalStatistics == null ||
                internalStatistics.getColumnStatistics().values().stream().allMatch(ColumnStatistic::isUnknown)) {
            // Get basic stats from connector metadata.
            session.setObtainedFromInternalStatistics(false);
            // Avoid `analyze table` to collect table statistics from metadata.
            if (StatisticUtils.statisticTableBlackListCheck(table.getId())) {
                return internalStatistics;
            } else {
                Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
                Statistics connectorBasicStats = connectorMetadata.map(metadata -> metadata.getTableStatistics(
                        session, table, columns, partitionKeys, predicate, limit, versionRange)).orElse(null);
                if (connectorBasicStats != null && internalStatistics != null &&
                        internalStatistics.getColumnStatistics().values().stream().anyMatch(
                                columnStatistic -> columnStatistic.getHistogram() != null)) {
                    // combine connector metadata basic stats and histogram from internal stats
                    Map<ColumnRefOperator, ColumnStatistic> internalColumnStatsMap = internalStatistics.getColumnStatistics();
                    Map<ColumnRefOperator, ColumnStatistic> combinedColumnStatsMap = Maps.newHashMap();
                    connectorBasicStats.getColumnStatistics().forEach((key, value) -> {
                        if (internalColumnStatsMap.containsKey(key)) {
                            combinedColumnStatsMap.put(key, ColumnStatistic.buildFrom(value).
                                    setHistogram(internalColumnStatsMap.get(key).getHistogram()).build());
                        } else {
                            combinedColumnStatsMap.put(key, value);
                        }
                    });

                    return Statistics.builder().addColumnStatistics(combinedColumnStatsMap).
                            setOutputRowCount(connectorBasicStats.getOutputRowCount()).build();
                } else {
                    return connectorBasicStats;
                }
            }
        } else {
            session.setObtainedFromInternalStatistics(true);
            return internalStatistics;
        }
    }

    public Statistics getTableStatistics(OptimizerContext session,
                                         String catalogName,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate) {
        return getTableStatistics(session, catalogName, table, columns, partitionKeys, predicate, -1, TableVersionRange.empty());
    }

    public List<PartitionInfo> getRemotePartitions(Table table, List<String> partitionNames) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(table.getCatalogName());
        if (connectorMetadata.isPresent()) {
            try {
                return connectorMetadata.get().getRemotePartitions(table, partitionNames);
            } catch (Exception e) {
                LOG.error("Failed to list partition directory's metadata on catalog [{}], table [{}]",
                        table.getCatalogName(), table, e);
                throw e;
            }
        }
        return new ArrayList<>();
    }

    public Set<DeleteFile> getDeleteFiles(IcebergTable table, Long snapshotId, ScalarOperator predicate, FileContent content) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(table.getCatalogName());
        if (connectorMetadata.isPresent()) {
            try {
                return connectorMetadata.get().getDeleteFiles(table, snapshotId, predicate, content);
            } catch (Exception e) {
                LOG.error("Failed to get delete files on catalog [{}], table [{}]", table.getCatalogName(), table, e);
                throw e;
            }
        }
        return new HashSet<>();
    }

    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(table.getCatalogName());
        if (connectorMetadata.isPresent()) {
            try {
                return connectorMetadata.get().getRemoteFiles(table, params);
            } catch (Exception e) {
                LOG.error("Failed to list remote file's metadata on catalog [{}], table [{}]", table.getCatalogName(), table, e);
                throw e;
            }
        }
        return new ArrayList<>();
    }

    public RemoteFileInfoSource getRemoteFilesAsync(Table table, GetRemoteFilesParams params) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(table.getCatalogName());
        if (connectorMetadata.isPresent()) {
            try {
                return connectorMetadata.get().getRemoteFilesAsync(table, params);
            } catch (Exception e) {
                LOG.error("Failed to list remote file's metadata on catalog [{}], table [{}]", table.getCatalogName(), table, e);
                throw e;
            }
        }
        return RemoteFileInfoDefaultSource.EMPTY;
    }

    public List<PartitionInfo> getPartitions(String catalogName, Table table, List<String> partitionNames) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        if (connectorMetadata.isPresent()) {
            try {
                return connectorMetadata.get().getPartitions(table, partitionNames);
            } catch (Exception e) {
                LOG.error("Failed to get partitions on catalog [{}], table [{}]", catalogName, table, e);
                throw e;
            }
        }
        return new ArrayList<>();
    }

    public SerializedMetaSpec getSerializedMetaSpec(String catalogName, String dbName, String tableName, long snapshotId,
                                                    String serializedPredicate, MetadataTableType type) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        if (connectorMetadata.isPresent()) {
            try {
                return connectorMetadata.get().getSerializedMetaSpec(dbName, tableName, snapshotId, serializedPredicate, type);
            } catch (Exception e) {
                LOG.error("Failed to get remote meta splits on catalog [{}], table [{}.{}]", catalogName, dbName, tableName, e);
                throw e;
            }
        }
        return null;
    }

    public boolean prepareMetadata(String queryId, String catalogName, MetaPreparationItem item,
                                   Tracers tracers, ConnectContext connectContext) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(Optional.of(queryId), catalogName);
        if (connectorMetadata.isPresent()) {
            try {
                return connectorMetadata.get().prepareMetadata(item, tracers, connectContext);
            } catch (Exception e) {
                LOG.error("prepare metadata failed on [{}]", item, e);
                return true;
            }
        }
        return true;
    }

    public void refreshTable(String catalogName, String srDbName, Table table,
                             List<String> partitionNames, boolean onlyCachedPartitions) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        connectorMetadata.ifPresent(metadata -> metadata.refreshTable(srDbName, table, partitionNames, onlyCachedPartitions));
    }

    public void finishSink(String catalogName, String dbName, String tableName,
                           List<TSinkCommitInfo> sinkCommitInfos, String branch) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        connectorMetadata.ifPresent(metadata -> {
            try {
                metadata.finishSink(dbName, tableName, sinkCommitInfos, branch);
            } catch (StarRocksConnectorException e) {
                LOG.error("table sink commit failed", e);
                throw new StarRocksConnectorException(e.getMessage());
            }
        });
    }

    public void abortSink(String catalogName, String dbName, String tableName, List<TSinkCommitInfo> sinkCommitInfos) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        connectorMetadata.ifPresent(metadata -> {
            try {
                metadata.abortSink(dbName, tableName, sinkCommitInfos);
            } catch (StarRocksConnectorException e) {
                LOG.error("table sink abort failed", e);
                throw new StarRocksConnectorException(e.getMessage());
            }
        });
    }
}
