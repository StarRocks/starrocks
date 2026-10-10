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

package com.starrocks.sql.analyzer;

import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Table;

import java.util.Map;

/**
 * What the unlocked pre-pass ({@code QueryAnalyzer#analyzeExternalTablesOnly}) resolved for the statement being
 * planned, for the analysis that runs under the FE meta lock to pick up instead of asking a connector again.
 *
 * <p>One per session, scoped to one statement: {@code StatementPlanner} clears it when planning ends, so nothing
 * the analyzer did not come to collect is offered to the next statement on the connection.
 *
 * <p>Every value is filed under a {@link Slot} and the fully qualified name of the table it is about. A name whose
 * catalog or database is not filled in is never a key: a lookup by it misses and the caller resolves the name the
 * way it always did, so an entry can only stand in for the exact lookup it replaces.
 */
public final class PreResolvedState {
    /**
     * One kind of pre-resolved value. Identity is the slot object, so each kind is declared once as a constant.
     */
    public static final class Slot<V> {
        private final String name;

        public Slot(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * A source table an INSERT ... SELECT already refreshed for this statement, as reloaded after that refresh
     * ({@link InsertSourceRefresher}), filed under the table's own catalog, database and table names. Read, never
     * taken.
     */
    public static final Slot<Table> REFRESHED_SOURCE = new Slot<>("refreshed source");

    private record Key(Slot<?> slot, String catalog, String db, String tbl) {
    }

    private final Map<Key, Object> values = Maps.newHashMap();

    public <V> void put(Slot<V> slot, TableName tableName, V value) {
        Key key = keyOf(slot, tableName);
        if (key != null) {
            values.put(key, value);
        }
    }

    /** @return the value filed for exactly this name, or null; it stays for the next lookup */
    @SuppressWarnings("unchecked")
    public <V> V get(Slot<V> slot, TableName tableName) {
        Key key = keyOf(slot, tableName);
        return key == null ? null : (V) values.get(key);
    }

    public void clear() {
        values.clear();
    }

    private static Key keyOf(Slot<?> slot, TableName tableName) {
        if (tableName == null || tableName.getCatalog() == null || tableName.getDb() == null) {
            return null;
        }
        return new Key(slot, tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
    }
}
