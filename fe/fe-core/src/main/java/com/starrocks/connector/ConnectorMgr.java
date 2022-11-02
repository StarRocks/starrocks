// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.starrocks.common.DdlException;
import com.starrocks.connector.hive.HiveConnectorFactory;
import com.starrocks.server.MetadataMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// ConnectorMgr is responsible for managing all ConnectorFactory, and for creating Connector
public class ConnectorMgr {
    private static final Logger LOG = LogManager.getLogger(MetadataMgr.class);
    private final ConcurrentHashMap<String, Connector> connectors = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConnectorFactory> connectorFactories = new ConcurrentHashMap<>();
    private final ReadWriteLock connectorLock = new ReentrantReadWriteLock();

    private final MetadataMgr metadataMgr;

    public final List<String> supportConnectorType = Lists.newArrayList();

    public ConnectorMgr(MetadataMgr metadataMgr) {
        this.metadataMgr = metadataMgr;
        init();
    }

    // TODO load jar by plugin
    private void init() {
        addConnectorFactory(new HiveConnectorFactory());
    }

    public void addConnectorFactory(ConnectorFactory connectorFactory) {
        Preconditions.checkNotNull(connectorFactory, "connectorFactory is null");
        supportConnectorType.add(connectorFactory.name());
        ConnectorFactory existingConnectorFactory = connectorFactories.putIfAbsent(
                connectorFactory.name(), connectorFactory);
        Preconditions.checkArgument(existingConnectorFactory == null,
                "ConnectorFactory '$s' is already registered", connectorFactory.name());
    }

    public Connector createConnector(ConnectorContext context) throws DdlException {
        String catalogName = context.getCatalogName();
        String type = context.getType();
        ConnectorFactory connectorFactory = connectorFactories.get(type);
        Preconditions.checkNotNull(connectorFactory, "Cannot load %s connector factory", type);
        readLock();
        try {
            Preconditions.checkState(!connectors.containsKey(catalogName),
                    "Connector of catalog '%s' already exists", catalogName);
        } finally {
            readUnlock();
        }

        Connector connector = connectorFactory.createConnector(context);

        writeLock();
        try {
            connectors.put(catalogName, connector);
        } finally {
            writeUnLock();
        }

        // TODO (stephen): to test behavior that failed to create connector when fe starting.
        try {
            registerConnectorInternal(connector, context);
        } catch (Exception e) {
            writeLock();
            try {
                connectors.remove(catalogName);
            } finally {
                writeUnLock();
            }
            connector.shutdown();
            throw new DdlException(String.format("Failed to create connector on [catalog : %s, type : %s]",
                    catalogName, type), e);
        }
        return connector;
    }

    public void removeConnector(String catalogName) {
        readLock();
        try {
            Preconditions.checkState(connectors.containsKey(catalogName), "Connector of catalog '%s' doesn't exist", catalogName);
        } finally {
            readUnlock();
        }

        removeConnectorInternal(catalogName);
        writeLock();
        try {
            Connector connector = connectors.remove(catalogName);
            connector.shutdown();
        } finally {
            writeUnLock();
        }
    }

    public boolean connectorExists(String catalogName) {
        readLock();
        try {
            return connectors.containsKey(catalogName);
        } finally {
            readUnlock();
        }
    }

    private void registerConnectorInternal(Connector connector, ConnectorContext context) throws Exception {
        metadataMgr.addMetadata(context.getCatalogName(), connector.getMetadata());
    }

    private void removeConnectorInternal(String catalogName) {
        metadataMgr.removeMetadata(catalogName);
    }

    private void readLock() {
        this.connectorLock.readLock().lock();
    }

    private void readUnlock() {
        this.connectorLock.readLock().unlock();
    }

    private void writeLock() {
        this.connectorLock.writeLock().lock();
    }

    private void writeUnLock() {
        this.connectorLock.writeLock().unlock();
    }

}
