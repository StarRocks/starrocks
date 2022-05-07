// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector;

import com.google.common.base.Preconditions;
import com.starrocks.common.DdlException;
import com.starrocks.connector.hive.HiveConnectorFactory;
import com.starrocks.server.MetadataMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

// ConnectorMgr is responsible for managing all ConnectorFactory, and for creating Connector
public class ConnectorMgr {
    private static final Logger LOG = LogManager.getLogger(MetadataMgr.class);
    private final ConcurrentHashMap<String, Connector> connectors = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConnectorFactory> connectorFactories = new ConcurrentHashMap<>();

    private final MetadataMgr metadataMgr;

    public ConnectorMgr(MetadataMgr metadataMgr) {
        this.metadataMgr = metadataMgr;
        init();
    }

    // TODO load jar by plugin
    private void init() {
        addConnectorFactory(new HiveConnectorFactory());
    }

    public synchronized void addConnectorFactory(ConnectorFactory connectorFactory) {
        Preconditions.checkNotNull(connectorFactory, "connectorFactory is null");
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
        Preconditions.checkState(!connectors.containsKey(catalogName), "Connector of catalog '%s' already exists", catalogName);

        Connector connector = connectorFactory.createConnector(context);
        connectors.put(catalogName, connector);

        try {
            registerConnectorInternal(connector, context);
        } catch (Exception e) {
            connectors.remove(catalogName);
            throw new DdlException(String.format("Failed to create connector on [catalog : %s, type : %s]",
                    catalogName, type), e);
        }
        return connector;
    }

    public void removeConnector(String catalogName) {
        Preconditions.checkState(connectors.containsKey(catalogName), "Connector of catalog '%s' doesn't exist", catalogName);
        removeConnectorInternal(catalogName);
        connectors.remove(catalogName);
    }

    public boolean connectorExists(String catalogName) {
        return connectors.containsKey(catalogName);
    }

    private void registerConnectorInternal(Connector connector, ConnectorContext context) throws Exception {
        metadataMgr.addMetadata(context.getCatalogName(), connector.getMetadata());
    }

    private void removeConnectorInternal(String catalogName) {
        metadataMgr.removeMetadata(catalogName);
    }
}
