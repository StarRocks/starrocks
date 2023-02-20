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


package com.starrocks.connector;

import com.google.common.base.Preconditions;
import com.starrocks.common.DdlException;
import com.starrocks.connector.delta.DeltaLakeConnectorFactory;
import com.starrocks.connector.hive.HiveConnectorFactory;
import com.starrocks.connector.hudi.HudiConnectorFactory;
import com.starrocks.connector.iceberg.IcebergConnectorFactory;
import com.starrocks.connector.jdbc.JDBCConnectorFactory;
import com.starrocks.server.MetadataMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// ConnectorMgr is responsible for managing all ConnectorFactory, and for creating Connector
public class ConnectorMgr {
    private static final Logger LOG = LogManager.getLogger(MetadataMgr.class);
    private final ConcurrentHashMap<String, Connector> connectors = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConnectorFactory> connectorFactories = new ConcurrentHashMap<>();
    private final ReadWriteLock connectorLock = new ReentrantReadWriteLock();

    public static final Set<String> SUPPORT_CONNECTOR_TYPE = new HashSet<>();

    public ConnectorMgr() {
        init();
    }

    // TODO load jar by plugin
    private void init() {
        addConnectorFactory(new HiveConnectorFactory());
        addConnectorFactory(new IcebergConnectorFactory());
        addConnectorFactory(new HudiConnectorFactory());
        addConnectorFactory(new JDBCConnectorFactory());
        addConnectorFactory(new DeltaLakeConnectorFactory());
    }

    public void addConnectorFactory(ConnectorFactory connectorFactory) {
        Preconditions.checkNotNull(connectorFactory, "connectorFactory is null");
        SUPPORT_CONNECTOR_TYPE.add(connectorFactory.name());
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
            return connector;
        } finally {
            writeUnLock();
        }
    }

    public void removeConnector(String catalogName) {
        readLock();
        try {
            Preconditions.checkState(connectors.containsKey(catalogName), "Connector of catalog '%s' doesn't exist", catalogName);
        } finally {
            readUnlock();
        }

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

    public Connector getConnector(String catalogName) {
        readLock();
        try {
            return connectors.get(catalogName);
        } finally {
            readUnlock();
        }
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
