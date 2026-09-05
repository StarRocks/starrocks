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
package com.starrocks.common.util.concurrent.lock;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;

import java.util.Map;
import java.util.Set;

public class LockParams {
    // Map database id -> database
    private final Map<Long, Database> dbs = Maps.newTreeMap(Long::compareTo);
    /**
     * Map database id -> table id set, Use db id as sort key to avoid deadlock,
     * lockTablesWithIntensiveDbLock can internally guarantee the order of locking,
     * so the table ids do not need to be ordered here.
     */
    private final Map<Long, Set<Long>> tables = Maps.newTreeMap(Long::compareTo);

    public LockParams() {
    }

    public void add(Database db, long tableId) {
        dbs.put(db.getId(), db);
        tables.computeIfAbsent(db.getId(), k -> Sets.newHashSet()).add(tableId);
    }

    public Map<Long, Database> getDbs() {
        return dbs;
    }

    public Map<Long, Set<Long>> getTables() {
        return tables;
    }

    @Override
    public String toString() {
        return "LockParams{" +
                "tables=" + tables +
                '}';
    }
}
