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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.qe.ConnectContext;

import java.util.Map;
import java.util.function.Supplier;

/**
 * What the unlocked pre-pass ({@code QueryAnalyzer#analyzeExternalTablesOnly}) resolved for the statement being
 * planned, for the analysis that runs under the FE meta lock to pick up instead of asking a connector again.
 *
 * <p>One per session, scoped to one statement: {@code StatementPlanner} clears it when planning ends, so nothing
 * the analyzer did not come to collect -- a view behind a branch that was never reached, a target of a statement
 * that failed early -- is offered to the next statement on the connection.
 *
 * <p>Apart from view bodies ({@link PreResolvedViewBodies}, keyed by view and checked for staleness), every value
 * is filed under a {@link Slot} and the fully qualified name of the table it is about. A name whose catalog or
 * database is not filled in is never a key: a lookup by it misses and the caller resolves the name the way it
 * always did, so an entry can only stand in for the exact lookup it replaces.
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
     * The database a CTAS creates its table in, and whether that table already existed when it was asked.
     */
    public record CreateTarget(Database db, boolean tableExists) {
    }

    /**
     * A DML's write target in an external catalog. The pre-pass resolves the tables a statement reads and leaves
     * the target to the locked analyzer, which is right for an internal target, the object the lock protects. An
     * external target is not in {@code PlannerMetaLocker}'s lock set, so the lock never made its metadata stable;
     * and connector metadata is snapshotted per query id ({@code MetadataMgr#getOptionalMetadata}), so a second
     * resolve reads the same answer. Handing the table over therefore needs no "still current" check. Taken once.
     */
    public static final Slot<Table> WRITE_TARGET = new Slot<>("write target");

    /**
     * The same for a CTAS into an external catalog, one step earlier: the table does not exist yet, so what the
     * locked {@code CreateTableAnalyzer} asks the catalog is whether the database exists and whether the table
     * already does. Taken once.
     */
    public static final Slot<CreateTarget> CREATE_TARGET = new Slot<>("create target");

    /**
     * A source table an INSERT ... SELECT already refreshed for this statement, as reloaded after that refresh
     * ({@link InsertSourceRefresher}), filed under the table's own catalog, database and table names. Read, never
     * taken.
     */
    public static final Slot<Table> REFRESHED_SOURCE = new Slot<>("refreshed source");

    private record Key(Slot<?> slot, String catalog, String db, String tbl) {
    }

    private final PreResolvedViewBodies viewBodies = new PreResolvedViewBodies();
    private final Map<Key, Object> values = Maps.newHashMap();

    /**
     * @return the session's state, or null when there is no session -- every lookup on it then misses
     */
    public static PreResolvedState of(ConnectContext context) {
        return context == null ? null : context.getPreResolvedState();
    }

    public PreResolvedViewBodies viewBodies() {
        return viewBodies;
    }

    public <V> void put(Slot<V> slot, TableName tableName, V value) {
        Key key = keyOf(slot, tableName);
        if (key != null) {
            values.put(key, value);
        }
    }

    /** Like {@link #put}, but never replaces a value already filed under this slot and name. */
    public <V> void putIfAbsent(Slot<V> slot, TableName tableName, V value) {
        Key key = keyOf(slot, tableName);
        if (key != null) {
            values.putIfAbsent(key, value);
        }
    }

    /** @return the value filed for exactly this name, or null; it stays for the next lookup */
    @SuppressWarnings("unchecked")
    public <V> V get(Slot<V> slot, TableName tableName) {
        Key key = keyOf(slot, tableName);
        return key == null ? null : (V) values.get(key);
    }

    /** @return the value filed for exactly this name, or null; it is handed out at most once */
    @SuppressWarnings("unchecked")
    public <V> V take(Slot<V> slot, TableName tableName) {
        Key key = keyOf(slot, tableName);
        return key == null ? null : (V) values.remove(key);
    }

    /** {@link #take}, or {@code resolver}'s answer on a miss -- what the caller did before this existed. */
    public <V> V takeOrResolve(Slot<V> slot, TableName tableName, Supplier<V> resolver) {
        V value = take(slot, tableName);
        return value != null ? value : resolver.get();
    }

    public boolean isEmpty() {
        return viewBodies.isEmpty() && values.isEmpty();
    }

    public void clear() {
        viewBodies.clear();
        values.clear();
    }

    private static Key keyOf(Slot<?> slot, TableName tableName) {
        if (tableName == null || tableName.getCatalog() == null || tableName.getDb() == null) {
            return null;
        }
        return new Key(slot, tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
    }
}
