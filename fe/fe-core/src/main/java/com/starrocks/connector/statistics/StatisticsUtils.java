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

package com.starrocks.connector.statistics;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.ImmutableTriple;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.Triple;

public class StatisticsUtils {
    public static Table getTableByUUID(String tableUUID) {
        String[] splits = tableUUID.split("\\.");

        Preconditions.checkState(splits.length == 4);
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(splits[0], splits[1], splits[2]);
        if (table == null) {
            throw new SemanticException("Table [%s.%s.%s] is not existed", splits[0], splits[1], splits[2]);
        }
        if (table.getUUID().equals(tableUUID)) {
            return table;
        } else {
            throw new SemanticException("Table [%s.%s.%s] is not existed", splits[0], splits[1], splits[2]);
        }
    }

    public static Triple<String, Database, Table> getTableTripleByUUID(String tableUUID) {
        String[] splits = tableUUID.split("\\.");

        Preconditions.checkState(splits.length == 4);
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(splits[0], splits[1]);
        if (db == null) {
            throw new SemanticException("Database [%s.%s] is not existed", splits[0], splits[1]);
        }

        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(splits[0], splits[1], splits[2]);
        if (table == null) {
            throw new SemanticException("Table [%s.%s.%s] is not existed", splits[0], splits[1], splits[2]);
        }
        if (!table.getUUID().equals(tableUUID)) {
            throw new SemanticException("Table [%s.%s.%s] is not existed", splits[0], splits[1], splits[2]);
        }

        return ImmutableTriple.of(splits[0], db, table);
    }
}
