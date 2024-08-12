// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.collect.Maps;

import java.util.Map;

public class ReplicatedObjectMap {

    private final Map<Long, Long> databaseMap = Maps.newConcurrentMap();

    private final Map<Long, Long> tableMap = Maps.newConcurrentMap();

    public void putDatabaseMap(long remoteDatabaseId, long localDatabaseId) {
        databaseMap.put(remoteDatabaseId, localDatabaseId);
    }

    public Long getLocalDatabaseId(long remoteDatabaseId) {
        return databaseMap.get(remoteDatabaseId);
    }

    public void putTableMap(long remoteTableId, long localTableId) {
        tableMap.put(remoteTableId, localTableId);
    }

    public Long getLocalTableId(long remoteTableId) {
        return tableMap.get(remoteTableId);
    }

    public void clear() {
        databaseMap.clear();
        tableMap.clear();
    }
}
