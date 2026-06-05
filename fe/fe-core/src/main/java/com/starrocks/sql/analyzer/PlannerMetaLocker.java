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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Pair;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.ViewRelation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * QueryLocker is used to obtain the lock corresponding to the metadata used in Query
 * Will acquire the read locks of all tables involved in the query
 * and the intention read locks of the database where the tables are located.
 * <p>
 * In terms of compatibility, when Config.use_lock_manager is turned off,
 * it will be consistent with the original db-lock logic
 * and obtain the db-read-lock of all dbs involved in the query.
 */
public class PlannerMetaLocker implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(PlannerMetaLocker.class);

    // Map database id -> database
    private Map<Long, Database> dbs = Maps.newTreeMap(Long::compareTo);

    private UUID queryId;

    /**
     * Map database id -> table id set, Use db id as sort key to avoid deadlock,
     * lockTablesWithIntensiveDbLock can internally guarantee the order of locking,
     * so the table ids do not need to be ordered here.
     */
    private Map<Long, Set<Long>> tables = Maps.newTreeMap(Long::compareTo);

    /**
     * Stack of acquisitions that are currently held but not yet released.
     *
     * <p>Each successful {@link #lock()} / {@link #tryLock} pushes a snapshot of the
     * {@code (db-id, table-ids)} entries it actually acquired; the matching {@link #unlock()}
     * pops the top snapshot and releases exactly those entries in reverse order. The underlying
     * {@code LockManager} is reentrant, so nested acquisitions simply bump the per-rid refCount
     * and each symmetric unlock pops one level and decrements it again.
     *
     * <p>Recording a per-acquisition snapshot (instead of re-deriving the entries from
     * {@link #tables} at release time) keeps {@code unlock()} correct no matter how deeply
     * lock/unlock are nested, and never tries to release an rid that this acquisition did not
     * take — which is exactly the asymmetry that previously surfaced as
     * {@code IllegalMonitorStateException} (see POST-1561). An empty stack means nothing is held,
     * so {@code unlock()} called without a matching {@code lock()} (or after a failed lock() that
     * already rolled itself back) is a safe no-op.
     */
    private final Deque<List<Map.Entry<Long, List<Long>>>> heldStack = new ArrayDeque<>();

    public PlannerMetaLocker(ConnectContext session, StatementBase statementBase) {
        new TableCollector(session, dbs, tables).visit(statementBase);
        session.setCurrentSqlDbIds(dbs.values().stream().map(Database::getId).collect(Collectors.toSet()));
        this.queryId = session.getQueryId();
    }

    /**
     * Try to acquire the lock, return false if the lock cannot be obtained.
     */
    public boolean tryLock(long timeout, TimeUnit unit) {
        Locker locker = new Locker(queryId);

        boolean isLockSuccess = false;
        List<Map.Entry<Long, List<Long>>> acquired = new ArrayList<>(tables.size());
        try {
            for (Map.Entry<Long, Set<Long>> entry : tables.entrySet()) {
                long dbId = entry.getKey();
                List<Long> tableIds = new ArrayList<>(entry.getValue());
                if (!locker.tryLockTablesWithIntensiveDbLock(dbId, tableIds, LockType.READ, timeout, unit)) {
                    return false;
                }
                acquired.add(new AbstractMap.SimpleImmutableEntry<>(dbId, tableIds));
            }
            isLockSuccess = true;
            // Only a fully-successful acquisition is pushed; a partial one is rolled back below
            // and leaves the stack (and any outer acquisition) untouched.
            heldStack.push(acquired);
        } finally {
            if (!isLockSuccess) {
                releaseInReverse(locker, acquired, "tryLock partial rollback");
            }
        }
        return true;
    }

    public boolean isEmpty() {
        return tables.isEmpty();
    }

    public void lock() {
        Locker locker = new Locker(queryId);
        List<Map.Entry<Long, List<Long>>> acquired = new ArrayList<>(tables.size());
        try {
            for (Map.Entry<Long, Set<Long>> entry : tables.entrySet()) {
                long dbId = entry.getKey();
                List<Long> tableIds = new ArrayList<>(entry.getValue());
                locker.lockTablesWithIntensiveDbLock(dbId, tableIds, LockType.READ);
                acquired.add(new AbstractMap.SimpleImmutableEntry<>(dbId, tableIds));
            }
            heldStack.push(acquired);
        } catch (Throwable t) {
            // Partial-acquire rollback at the multi-db level. Inner
            // lockTablesWithIntensiveDbLock is atomic per-db (it rolls itself back on failure),
            // so a throw here means the entries already in `acquired` are fully held while the
            // current and later ones are not. Release the held ones (reverse order) and re-throw.
            // The stack is NOT touched: we never pushed, so any outer acquisition stays intact.
            releaseInReverse(locker, acquired, "lock partial rollback");
            throw t;
        }
    }

    public void unlock() {
        List<Map.Entry<Long, List<Long>>> acquired = heldStack.poll();
        if (acquired == null) {
            // Nothing currently held: lock()/tryLock() was never called successfully on this
            // instance, or every acquisition has already been released. This is what lets callers
            // pair lock() with unlock() in a finally block without a spurious
            // "Attempt to unlock lock" when lock() failed partway and rolled itself back.
            return;
        }
        releaseInReverse(new Locker(), acquired, "unlock");
    }

    @Override
    public void close() {
        // AutoCloseable contract: release every acquisition this instance still holds, regardless
        // of how many lock()/tryLock() calls have not yet been matched by an explicit unlock().
        while (!heldStack.isEmpty()) {
            unlock();
        }
    }

    /**
     * Release the given acquisition snapshot in reverse acquisition order, best-effort: a failure
     * on one entry is logged and the remaining entries are still released. A fully-balanced lock
     * state never triggers a release failure; logging instead of throwing keeps a single bad rid
     * from leaking the rest of the held locks.
     */
    private static void releaseInReverse(Locker locker, List<Map.Entry<Long, List<Long>>> acquired, String context) {
        for (int i = acquired.size() - 1; i >= 0; i--) {
            Map.Entry<Long, List<Long>> entry = acquired.get(i);
            try {
                locker.unLockTablesWithIntensiveDbLock(entry.getKey(), entry.getValue(), LockType.READ);
            } catch (Throwable t) {
                LOG.warn("PlannerMetaLocker.{}: db {} release failed, continuing", context, entry.getKey(), t);
            }
        }
    }

    /**
     * Collect tables that need to be protected by the PlannerMetaLock
     */
    public static void collectTablesNeedLock(StatementBase statement,
                                             ConnectContext session,
                                             Map<Long, Database> dbs,
                                             Map<Long, Set<Long>> tables) {
        new TableCollector(session, dbs, tables).visit(statement);
    }

    private static Pair<Database, Table> resolveTable(ConnectContext session, TableName tableName) {
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();

        String catalogName = tableName.getCatalog();
        String dbName = tableName.getDb();
        String tbName = tableName.getTbl();

        if (Strings.isNullOrEmpty(catalogName)) {
            if (!Strings.isNullOrEmpty(session.getCurrentCatalog())) {
                catalogName = session.getCurrentCatalog();
            }
        }

        if (Strings.isNullOrEmpty(dbName)) {
            if (!Strings.isNullOrEmpty(session.getDatabase())) {
                dbName = session.getDatabase();
            }
        }

        if (catalogName == null || dbName == null || tbName == null) {
            return null;
        }

        if (!CatalogMgr.isInternalCatalog(catalogName)) {
            return null;
        }

        if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalogName)) {
            return null;
        }

        Database db = metadataMgr.getDb(session, catalogName, dbName);
        if (db == null) {
            return null;
        }

        Table table = metadataMgr.getTable(session, catalogName, dbName, tbName);
        if (table == null) {
            return null;
        }

        return new Pair<>(db, table);
    }

    private static class TableCollector extends AstTraverser<Void, Void> {
        private final ConnectContext session;

        private final Map<Long, Database> dbs;
        private final Map<Long, Set<Long>> tables;

        public TableCollector(ConnectContext session, Map<Long, Database> dbs, Map<Long, Set<Long>> tables) {
            this.session = session;
            this.dbs = dbs;
            this.tables = tables;
        }

        @Override
        public Void visitInsertStatement(InsertStmt node, Void context) {
            TableRef tableRef = node.getTableRef();
            TableName tableName = TableName.fromTableRef(tableRef);
            Pair<Database, Table> dbAndTable = resolveTable(session, tableName);
            put(dbAndTable);
            return super.visitInsertStatement(node, context);
        }

        @Override
        public Void visitUpdateStatement(UpdateStmt node, Void context) {
            TableRef tableRef = node.getTableRef();
            TableName tableName = TableName.fromTableRef(tableRef);
            Pair<Database, Table> dbAndTable = resolveTable(session, tableName);
            put(dbAndTable);
            return super.visitUpdateStatement(node, context);
        }

        @Override
        public Void visitDeleteStatement(DeleteStmt node, Void context) {
            TableRef tableRef = node.getTableRef();
            TableName tableName = TableName.fromTableRef(tableRef);
            Pair<Database, Table> dbAndTable = resolveTable(session, tableName);
            put(dbAndTable);
            return super.visitDeleteStatement(node, context);
        }

        @Override
        public Void visitAlterTableStatement(AlterTableStmt statement, Void context) {
            Pair<Database, Table> dbAndTable = resolveTable(session, 
                    com.starrocks.catalog.TableName.fromTableRef(statement.getTableRef()));
            put(dbAndTable);
            return super.visitAlterTableStatement(statement, context);
        }

        @Override
        public Void visitAlterViewStatement(AlterViewStmt statement, Void context) {
            Pair<Database, Table> dbAndTable = resolveTable(session, 
                    com.starrocks.catalog.TableName.fromTableRef(statement.getTableRef()));
            put(dbAndTable);
            return super.visitAlterViewStatement(statement, context);
        }

        @Override
        public Void visitAlterMaterializedViewStatement(AlterMaterializedViewStmt statement, Void context) {
            Pair<Database, Table> dbAndTable = resolveTable(session, 
                    com.starrocks.catalog.TableName.fromTableRef(statement.getMvTableRef()));
            put(dbAndTable);
            return super.visitAlterMaterializedViewStatement(statement, context);
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            Pair<Database, Table> dbAndTable = resolveTable(session, node.getName());
            put(dbAndTable);
            return null;
        }

        @Override
        public Void visitView(ViewRelation node, Void context) {
            Pair<Database, Table> dbAndTable = resolveTable(session, node.getName());
            put(dbAndTable);
            return visit(node.getQueryStatement(), context);
        }

        private void put(Pair<Database, Table> dbAndTable) {
            if (dbAndTable != null) {
                Database database = dbAndTable.first;
                Table table = dbAndTable.second;

                dbs.putIfAbsent(database.getId(), database);
                tables.computeIfAbsent(database.getId(), k -> new HashSet<>()).add(table.getId());
            }
        }
    }
}
