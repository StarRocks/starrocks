// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.server;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.external.RemoteFileInfo;
import com.starrocks.qe.ConnectContext;
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
    private final Map<String, QueryMetadatas> metadataByQueryId = new ConcurrentHashMap<>();

    public MetadataMgr(LocalMetastore localMetastore, ConnectorMgr connectorMgr) {
        Preconditions.checkNotNull(localMetastore, "localMetastore is null");
        this.localMetastore = localMetastore;
        this.connectorMgr = connectorMgr;
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
                return Optional.of(connectorMgr.getConnector(catalogName).getMetadata());
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
        return connectorMetadata.map(metadata -> metadata.getTable(dbName, tblName)).orElse(null);
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

    public void refreshTable(String catalogName, String dbName, String tableName, Table table, List<String> partitionNames) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        connectorMetadata.ifPresent(metadata -> metadata.refreshTable(dbName, tableName, table, partitionNames));
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
