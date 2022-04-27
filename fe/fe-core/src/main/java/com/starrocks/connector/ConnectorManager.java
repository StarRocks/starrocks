// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.concurrent.GuardedBy;

// ConnectorManager is responsible for managing all ConnectorFactory, and for creating Connector
public class ConnectorManager {
    @GuardedBy("this")
    private final ConcurrentMap<String, ConnectorFactory> connectorFactories = new ConcurrentHashMap<>();

    /**
     * add connectorFactory
     * @param connectorFactory - a connector factory instance
     */
    public synchronized void addConnectorFactory(ConnectorFactory connectorFactory)
    {
        connectorFactories.putIfAbsent(connectorFactory.name(), connectorFactory);
    }

    /**
     * create a connector provided by connector name
     * @param connectorName - a string specify a kind of connector
     * @param properties - a map of string kv for instantiate a connector
     * @return a connector instance
     */
    public synchronized Connector createConnector(String connectorName, Map<String, String> properties) {
        Connector.Context context = new Connector.Context(properties);

        ConnectorFactory connectorFactory = connectorFactories.get(connectorName);
        return connectorFactory.createConnector(context);
    }
}
