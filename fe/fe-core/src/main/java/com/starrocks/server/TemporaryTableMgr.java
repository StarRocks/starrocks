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

package com.starrocks.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

// used for temporary tables to store all temporary table related Sessions in the cluster
// maintain the mapping relationship of sessionId -> Session
public class TemporaryTableMgr {
    private Map<UUID, Session> sessionMap = Maps.newConcurrentMap();

    public TemporaryTableMgr() {
    }

    public void addTemporaryTable(UUID sessionId, long databaseId, String tblName, long tableId) {
        if (!sessionMap.containsKey(sessionId)) {
            sessionMap.put(sessionId, new Session(sessionId));
        }
        Session session = sessionMap.get(sessionId);
        session.addTemporaryTable(databaseId, tblName, tableId);
    }

    public Long getTable(UUID sessionId, long databaseId, String tblName) {
        if (!sessionMap.containsKey(sessionId)) {
            return null;
        }
        Session session = sessionMap.get(sessionId);
        return session.getTemporaryTableId(databaseId, tblName);
    }

    public boolean tableExists(UUID sessionId, long databaseId, String tblName) {
        if (!sessionMap.containsKey(sessionId)) {
            return false;
        }
        Session session = sessionMap.get(sessionId);
        return session.getTemporaryTableId(databaseId, tblName) != null;
    }

    public void dropTemporaryTable(UUID sessionId, long databaseId, String tblName) {
        Session session = sessionMap.get(sessionId);
        if (session == null) {
            return;
        }
        session.removeTemporaryTable(databaseId, tblName);
    }

    public Session getSession(UUID sessionId) {
        return sessionMap.getOrDefault(sessionId, null);
    }

    public void removeSession(UUID sessionId) {
        sessionMap.remove(sessionId);
    }

    public List<String> listTemporaryTables(UUID sessionId, long databaseId) {
        if (sessionMap.containsKey(sessionId)) {
            return sessionMap.get(sessionId).listTemporaryTables(databaseId);
        }
        return Lists.newArrayList();
    }

    // db -> session -> table
    public Map<Long, Map<UUID, Long>> getAllTemporaryTables(Set<Long> requiredDbIds) {
        Map<Long, Map<UUID, Long>> result = Maps.newHashMap();
        sessionMap.values().forEach(session -> {
            UUID sessionId = session.getId();
            Map<Long, Map<String, Long>> allTables = session.getAllTemporaryTables();
            allTables.forEach((databaseId, tableMap) -> {
                if (!requiredDbIds.contains(databaseId)) {
                    return;
                }
                if (!result.containsKey(databaseId)) {
                    result.put(databaseId, Maps.newHashMap());
                }
                tableMap.values().stream().forEach(tableId -> {
                    result.get(databaseId).put(sessionId, tableId);
                });
            });
        });
        return result;
    }

    public Set<UUID> listSessions() {
        return sessionMap.keySet();
    }


    @VisibleForTesting
    public void clear() {
        if (sessionMap != null) {
            sessionMap.clear();
        }
        System.gc();
    }
}
