package com.starrocks.server;

import com.starrocks.spi.Connector;
import com.starrocks.spi.ConnectorFactory;
import com.starrocks.spi.HiveConnectorFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class ConnectorManager {
    private final ConcurrentHashMap<String, ConnectorFactory> connectorFactories = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Connector> connectors = new ConcurrentHashMap<>();
    private final MetadataManager metadataManager;

    public ConnectorManager(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    public void init() {
        addConnectorFactory(new HiveConnectorFactory());
    }

    public synchronized void addConnectorFactory(ConnectorFactory connectorFactory) {
        requireNonNull(connectorFactory, "connectorFactory is null");
        connectorFactories.put(connectorFactory.getName(), connectorFactory);
    }

    public void addConnector(String type, String name, Map<String, String> configs) {
        ConnectorFactory factory = connectorFactories.get(type);
        Connector connector = factory.create(name, configs);

        metadataManager.addMetadata(name, connector.getMetadata());
        connectors.put(name, connector);
    }

}
