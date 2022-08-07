// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.external.hive.RemoteFileInfo;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MetadataMgr {
    private static final Logger LOG = LogManager.getLogger(MetadataMgr.class);

    private final ReadWriteLock metaLock = new ReentrantReadWriteLock();
    private final LocalMetastore localMetastore;
    private final ConnectorMgr connectorMgr;

//    private final Map<String, ConnectorMetadata> connectorMetadatas = new HashMap<>();
    private final Map<UUID, QueryMetadatas> metadataByQueryId = new ConcurrentHashMap<>();

    public MetadataMgr(LocalMetastore localMetastore, ConnectorMgr connectorMgr) {
        Preconditions.checkNotNull(localMetastore, "localMetastore is null");
        this.localMetastore = localMetastore;
        this.connectorMgr = connectorMgr;
    }

//    public void addMetadata(String catalogName, ConnectorMetadata metadata) {
//        writeLock();
//        try {
//            ConnectContext.get().getQueryId();
//            Preconditions.checkState(!connectorMetadatas.containsKey(catalogName),
//                    "ConnectorMetadata of catalog '%s' already exists", catalogName);
//            connectorMetadatas.put(catalogName, metadata);
//        } finally {
//            writeUnLock();
//        }
//    }

//    public void removeMetadata(String catalogName) {
//        writeLock();
//        try {
//            connectorMetadatas.remove(catalogName);
//        } finally {
//            writeUnLock();
//        }
//    }

//    public boolean connectorMetadataExists(String catalogName) {
//        readLock();
//        try {
//            return connectorMetadatas.containsKey(catalogName);
//        } finally {
//            readUnlock();
//        }
//    }

    // get metadata by catalog name
    @VisibleForTesting
    public Optional<ConnectorMetadata> getOptionalMetadata(String catalogName) {
        if (CatalogMgr.isInternalCatalog(catalogName)) {
            return Optional.of(localMetastore);
        } else {
            if (ConnectContext.get() != null && ConnectContext.get().getQueryId() != null) {
                QueryMetadatas queryMetadatas = metadataByQueryId.getOrDefault(ConnectContext.get().getQueryId(), new QueryMetadatas());
                return Optional.of(queryMetadatas.getConnectorMetadata(catalogName));
            } else {
                return Optional.of(connectorMgr.getConnector(catalogName).getMetadata());
            }
        }
    }



    public List<String> listDbNames(String catalogName) throws DdlException {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        ImmutableSet.Builder<String> dbNames = ImmutableSet.builder();
        if (connectorMetadata.isPresent()) {
            try {
                connectorMetadata.get().listDbNames().forEach(dbNames::add);
            } catch (DdlException e) {
                LOG.error("Failed to listDbNames on catalog {}", catalogName, e);
                throw e;
            }
        }
        return ImmutableList.copyOf(dbNames.build());
    }

    public Database getDb(String catalogName, String dbName) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        return connectorMetadata.map(metadata -> metadata.getDb(dbName)).orElse(null);
    }

    public List<String> listTableNames(String catalogName, String dbName) throws DdlException {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
        if (connectorMetadata.isPresent()) {
            try {
                connectorMetadata.get().listTableNames(dbName).forEach(tableNames::add);
            } catch (DdlException e) {
                LOG.error("Failed to listTableNames on [{}.{}]", catalogName, dbName, e);
                throw e;
            }
        }
        return ImmutableList.copyOf(tableNames.build());
    }

    public Table getTable(String catalogName, String dbName, String tblName) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        return connectorMetadata.map(metadata -> metadata.getTable(dbName, tblName)).orElse(null);
    }

    public List<String> getPartitionNames(String catalogName, String dbName, String tblName) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(catalogName);
        return connectorMetadata.map(metadata -> metadata.getPartitionNames(dbName, tblName)).orElse(null);
    }

    public List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(((HiveMetaStoreTable) table).getCatalogName());
        return connectorMetadata.map(metadata -> metadata.getRemoteFileInfos(table, partitionKeys)).orElse(null);
    }

    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         List<ColumnRefOperator> columns,
                                         List<PartitionKey> partitionKeys) {
        Optional<ConnectorMetadata> connectorMetadata = getOptionalMetadata(((HiveMetaStoreTable) table).getCatalogName());
        return connectorMetadata.map(metadata -> metadata.getTableStatistics(session, table, columns, partitionKeys)).orElse(null);
    }

    private void readLock() {
        this.metaLock.readLock().lock();
    }

    private void readUnlock() {
        this.metaLock.readLock().unlock();
    }

    private void writeLock() {
        this.metaLock.writeLock().lock();
    }

    private void writeUnLock() {
        this.metaLock.writeLock().unlock();
    }

    private class QueryMetadatas {
        private final Map<String, ConnectorMetadata> metadatas = new HashMap<>();

        public QueryMetadatas() {
        }

        private synchronized ConnectorMetadata getConnectorMetadata(String catalogName) {
            if (metadatas.containsKey(catalogName)) {
                return metadatas.get(catalogName);
            }
            ConnectorMetadata connectorMetadata = connectorMgr.getConnector(catalogName).getMetadata();
            metadatas.put(catalogName, connectorMetadata);
            return connectorMetadata;
        }

    }
}
