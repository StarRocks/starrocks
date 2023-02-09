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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.ConnectorTblMetaInfoMgr;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class MetadataMgr {
    private static final Logger LOG = LogManager.getLogger(MetadataMgr.class);

    private final LocalMetastore localMetastore;
    private final ConnectorMgr connectorMgr;
    private final ConnectorTblMetaInfoMgr connectorTblMetaInfoMgr;
    private final Map<String, QueryMetadatas> metadataByQueryId = new ConcurrentHashMap<>();

    public MetadataMgr(LocalMetastore localMetastore, ConnectorMgr connectorMgr,
                       ConnectorTblMetaInfoMgr connectorTblMetaInfoMgr) {
        Preconditions.checkNotNull(localMetastore, "localMetastore is null");
        this.localMetastore = localMetastore;
        this.connectorMgr = connectorMgr;
        this.connectorTblMetaInfoMgr = connectorTblMetaInfoMgr;
    }

    protected Optional<ConnectorMetadata> getOptionalMetadata(String catalogName) {
        if (CatalogMgr.isInternalCatalog(catalogName)) {
            return Optional.of(localMetastore);
        } else {
            String queryId = ConnectContext.get() != null && ConnectContext.get().getQueryId() != null ?
                    ConnectContext.get().getQueryId().toString() : null;
            if (queryId != null) {
                QueryMetadatas queryMetadatas = metadataByQueryId.computeIfAbsent(queryId, ignored -> new QueryMetadatas());
                return Optional.ofNullable(queryMetadatas.getConnectorMetadata(catalogName, queryId));
            } else {
                Connector connector = connectorMgr.getConnector(catalogName);
                if (connector == null) {
                    LOG.error("Failed to get {} catalog", catalogName);
                    return Optional.empty();
                } else {
                    return Optional.of(connector.getMetadata());
                }
            }
        }
    }

    public void removeQueryMetadata() {
        String queryId = ConnectContext.get() != null && ConnectContext.get().getQueryId() != null ?
                ConnectContext.get().getQueryId().toString() : null;
        if (queryId != null) {
            QueryMetadatas queryMetadatas = metadataByQueryId.get(queryId);
            if (queryMetadatas != null) {
                queryMetadatas.metadatas.values().forEach(ConnectorMetadata::clear);
                metadataByQueryId.remove(queryId);
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

    public Database getDb(String catalogName, String dbName) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        return connectorMetadata.map(metadata -> metadata.getDb(dbName)).orElse(null);
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

    public Statistics getTableStatistics(OptimizerContext session,
                                         String catalogName,
                                         Table table,
                                         List<ColumnRefOperator> columns,
                                         List<PartitionKey> partitionKeys) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        return connectorMetadata.map(metadata ->
                metadata.getTableStatistics(session, table, columns, partitionKeys)).orElse(null);
    }

    public List<RemoteFileInfo> getRemoteFileInfos(String catalogName, Table table, List<PartitionKey> partitionKeys) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        ImmutableSet.Builder<RemoteFileInfo> files = ImmutableSet.builder();
        if (connectorMetadata.isPresent()) {
            try {
                connectorMetadata.get().getRemoteFileInfos(table, partitionKeys).forEach(files::add);
            } catch (Exception e) {
                LOG.error("Failed to list remote file's metadata on catalog [{}], table [{}]", catalogName, table, e);
                throw e;
            }
        }
        return ImmutableList.copyOf(files.build());
    }

    public void dropTable(String catalogName, String dbName, String tblName) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        connectorMetadata.ifPresent(metadata -> {
            TableName tableName = new TableName(catalogName, dbName, tblName);
            DropTableStmt dropTableStmt = new DropTableStmt(false, tableName, false);
            try {
                metadata.dropTable(dropTableStmt);
            } catch (DdlException e) {
                LOG.error("Failed to drop table {}.{}.{}", catalogName, dbName, tblName, e);
                throw new StarRocksConnectorException("Failed to drop table {}.{}.{}", catalogName, dbName, tblName);
            }
        });
    }

    public void refreshTable(String catalogName, String srDbName, Table table, List<String> partitionNames) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        connectorMetadata.ifPresent(metadata -> metadata.refreshTable(srDbName, table, partitionNames));
    }

    private class QueryMetadatas {
        private final Map<String, ConnectorMetadata> metadatas = new HashMap<>();

        public QueryMetadatas() {
        }

        public synchronized ConnectorMetadata getConnectorMetadata(String catalogName, String queryId) {
            if (metadatas.containsKey(catalogName)) {
                return metadatas.get(catalogName);
            }

            Connector connector = connectorMgr.getConnector(catalogName);
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
}
