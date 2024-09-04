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


<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/connector/jdbc/JDBCTableIdCache.java
package com.starrocks.connector.jdbc;

import java.util.concurrent.ConcurrentHashMap;

public class JDBCTableIdCache {
    private static ConcurrentHashMap<JDBCTableName, Integer> tableIdCache = new ConcurrentHashMap();

    public static Boolean containsTableId(JDBCTableName tableKey) {
        return tableIdCache.containsKey(tableKey);
    }

    public static void putTableId(JDBCTableName tableKey, Integer tableId) {
        tableIdCache.put(tableKey, tableId);
    }

    public static Integer getTableId(JDBCTableName tableKey) {
        return tableIdCache.get(tableKey);
=======
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.qe.QueryDetailQueue;

import java.util.List;
import java.util.Map;

public class QueryTracker implements MemoryTrackable {
    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.of("QueryDetail", QueryDetailQueue.getTotalQueriesCount());
>>>>>>> f0cb5e97c8 ([Enhancement] Optimize memory tracker (#49841)):fe/fe-core/src/main/java/com/starrocks/memory/QueryTracker.java
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        return Lists.newArrayList(Pair.create(QueryDetailQueue.getSamplesForMemoryTracker(),
                QueryDetailQueue.getTotalQueriesCount()));
    }
}
