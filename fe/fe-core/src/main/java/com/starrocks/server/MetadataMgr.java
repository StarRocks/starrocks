// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MetadataMgr {
    private static final Logger LOG = LogManager.getLogger(MetadataMgr.class);

    private final ReadWriteLock metaLock = new ReentrantReadWriteLock();
    private final LocalMetastore localMetastore;
    private final Map<String, ConnectorMetadata> connectorMetadatas = new HashMap<>();

    public MetadataMgr(LocalMetastore localMetastore) {
        Preconditions.checkNotNull(localMetastore, "localMetastore is null");
        this.localMetastore = localMetastore;
    }

    public void addMetadata(String catalogName, ConnectorMetadata metadata) {
        writeLock();
        try {
            Preconditions.checkState(!connectorMetadatas.containsKey(catalogName),
                    "ConnectorMetadata of catalog '%s' already exists", catalogName);
            connectorMetadatas.put(catalogName, metadata);
        } finally {
            writeUnLock();
        }
    }

    public void removeMetadata(String catalogName) {
        writeLock();
        try {
            connectorMetadatas.remove(catalogName);
        } finally {
            writeUnLock();
        }
    }

    public boolean connectorMetadataExists(String catalogName) {
        readLock();
        try {
            return connectorMetadatas.containsKey(catalogName);
        } finally {
            readUnlock();
        }
    }

    // get metadata by catalog name
    @VisibleForTesting
    public Optional<ConnectorMetadata> getOptionalMetadata(String catalogName) {
        if (CatalogMgr.isInternalCatalog(catalogName)) {
            return Optional.of(localMetastore);
        } else {
            readLock();
            try {
                return Optional.ofNullable(connectorMetadatas.get(catalogName));
            } finally {
                readUnlock();
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

}
