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

import com.google.api.client.util.Lists;
import com.google.common.collect.Maps;
import org.apache.arrow.util.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Session {
    private static final Logger LOG = LogManager.getLogger(Session.class);
    private UUID id;

    private Map<Long, Map<String, Long>> temporaryTables;

    public Session(UUID id) {
        this.id = id;
        temporaryTables = new ConcurrentHashMap<>();
    }

    public UUID getId() {
        return id;
    }

    public Long getTemporaryTableId(Long dbId, String tblName) {
        if (temporaryTables.containsKey(dbId)) {
            Map<String, Long> tables = temporaryTables.get(dbId);
            if (tables.containsKey(tblName)) {
                return tables.get(tblName);
            }
        }
        return null;
    }

    public void addTemporaryTable(Long databaseId, String tblName, Long tableId) {
        if (!temporaryTables.containsKey(databaseId)) {
            temporaryTables.put(databaseId, Maps.newHashMap());
        }
        Map<String, Long> tables = temporaryTables.get(databaseId);
        Preconditions.checkArgument(!tables.containsKey(tblName), tblName + " exists in session " + id);
        tables.put(tblName, tableId);
        LOG.info("add temporary table db id[{}], table name[{}], table id[{}], session[{}]",
                databaseId, tblName, tableId, id.toString());
    }

    public void removeTemporaryTable(Long databaseId, String tblName) {
        Map<String, Long> tables = temporaryTables.get(databaseId);
        Preconditions.checkNotNull(tables, "database doesn't exist " + databaseId);
        tables.remove(tblName);
        LOG.info("remove temporary table, db id[{}], table name[{}], session[{}]", databaseId, tblName, id);
    }

    // get a deep copy of temporaryTables
    public Map<Long, Map<String, Long>> getAllTemporaryTables() {
        Map<Long, Map<String, Long>> deepCopy = Maps.newHashMap();
        temporaryTables.forEach((databaseId, table) -> {
            deepCopy.put(databaseId,
                    table.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        });
        return deepCopy;
    }

    public List<String> listTemporaryTables(long databaseId) {
        List<String> tableNames = Lists.newArrayList();
        if (temporaryTables.containsKey(databaseId)) {
            temporaryTables.get(databaseId).keySet().stream().forEach(tableNames::add);
        }
        return tableNames;
    }
}
