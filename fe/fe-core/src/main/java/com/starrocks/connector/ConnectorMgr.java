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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// ConnectorMgr is responsible for managing all ConnectorFactory, and for creating Connector
public class ConnectorMgr {
    private final ConcurrentHashMap<String, ConnectorService> connectors = new ConcurrentHashMap<>();
    private final ReadWriteLock connectorLock = new ReentrantReadWriteLock();

    public ConnectorService createConnector(ConnectorContext context) {
        String catalogName = context.getCatalogName();
        ConnectorService connector = null;
        readLock();
        try {
            connector = ConnectorFactory.createConnector(context);
            Preconditions.checkState(!connectors.containsKey(catalogName),
                    "Connector of catalog '%s' already exists", catalogName);
            if (connector == null) {
                return null;
            }
        } finally {
            readUnlock();
        }

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
            ConnectorService connectorService = connectors.remove(catalogName);
            connectorService.shutdown();
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

    public ConnectorService getConnector(String catalogName) {
        readLock();
        try {
            return connectors.get(catalogName);
        } finally {
            readUnlock();
        }
    }

    public List<ConnectorService> listConnectors() {
        return new ArrayList<>(connectors.values());
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
