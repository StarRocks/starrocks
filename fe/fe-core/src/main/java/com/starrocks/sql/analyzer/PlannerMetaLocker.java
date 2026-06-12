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
     * Stack of acquisitions still owed a matching {@code unlock()}. Each successful
     * {@link #lock()} / {@link #tryLock} pushes the list of {@code (dbId, tableIds)} entries it
     * acquired; each {@link #unlock()} pops the most recent frame and releases its entries in
     * reverse order. {@link #close()} drains the whole stack.
     *
     * <p>This makes nested lock/unlock symmetric without a separate depth counter. Nesting
     * happens for real on the optimistic-retry path:
     * {@link com.starrocks.sql.InsertPlanner#buildExecPlanWithRetry} unlocks during planning then
     * re-locks before validation, and {@link com.starrocks.sql.StatementPlanner#reAnalyzeStmt}
     * additionally does {@code try { lock(); ... } finally { unlock(); }} on the SAME instance
     * inside the retry iteration. The underlying {@link Locker} is reentrant (LockManager
     * refcounts per rid), so each lock()/unlock() pair only adjusts its own refCount slot and a
     * full unwind leaves the LockManager balanced. An empty stack makes {@code unlock()} a no-op,
     * so callers that always pair lock() with unlock() in a {@code finally} block never hit a
     * spurious "Attempt to unlock lock" when lock() failed partway and rolled itself back.
     */
    private final Deque<List<Map.Entry<Long, Set<Long>>>> heldStack = new ArrayDeque<>();

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
        List<Map.Entry<Long, Set<Long>>> lockedEntries = new ArrayList<>(tables.size());
        try {
            for (Map.Entry<Long, Set<Long>> entry : tables.entrySet()) {
                Database database = dbs.get(entry.getKey());
                if (!locker.tryLockTablesWithIntensiveDbLock(database.getId(), new ArrayList<>(entry.getValue()),
                        LockType.READ, timeout, unit)) {
                    return false;
                }
                lockedEntries.add(entry);
            }
            isLockSuccess = true;
            // Record this acquisition so the matching unlock() releases exactly what it locked.
            heldStack.push(lockedEntries);
        } finally {
            if (!isLockSuccess) {
                // Roll back the entries we did acquire (lockedEntries already records everything
                // needed). Release in reverse order, exception-safe per entry so one failed
                // release doesn't leave the remaining entries leaked.
                for (int i = lockedEntries.size() - 1; i >= 0; i--) {
                    Map.Entry<Long, Set<Long>> entry = lockedEntries.get(i);
                    try {
                        locker.unLockTablesWithIntensiveDbLock(
                                entry.getKey(), new ArrayList<>(entry.getValue()), LockType.READ);
                    } catch (Throwable releaseEx) {
                        LOG.warn("PlannerMetaLocker.tryLock partial rollback: db {} release failed, continuing",
                                entry.getKey(), releaseEx);
                    }
                }
            }
        }
        return true;
    }

    public boolean isEmpty() {
        return tables.isEmpty();
    }

    public void lock() {
        Locker locker = new Locker(queryId);
        List<Map.Entry<Long, Set<Long>>> lockedEntries = new ArrayList<>(tables.size());
        try {
            for (Map.Entry<Long, Set<Long>> entry : tables.entrySet()) {
                Database database = dbs.get(entry.getKey());
                List<Long> tableIds = new ArrayList<>(entry.getValue());
                locker.lockTablesWithIntensiveDbLock(database.getId(), tableIds, LockType.READ);
                lockedEntries.add(entry);
            }
            // Record this acquisition so the matching unlock() releases exactly what it locked.
            // Each lock() pushes its own frame, so nested calls stack independently and a later
            // unlock() pops them one at a time in reverse acquisition order.
            heldStack.push(lockedEntries);
        } catch (Throwable t) {
            // Partial-acquire rollback at the multi-db level. Inner
            // Locker.lockTablesWithIntensiveDbLock is atomic per-db (it rolls itself back on
            // failure), so a throw here means earlier (db, tableIds) entries are fully held
            // while the current and later ones are not. Release the earlier ones in reverse
            // order before re-throwing. The stack is NOT pushed — we never reached the push, so
            // any outer frame (if this was a nested call) stays exactly as it was.
            for (int i = lockedEntries.size() - 1; i >= 0; i--) {
                Map.Entry<Long, Set<Long>> entry = lockedEntries.get(i);
                try {
                    locker.unLockTablesWithIntensiveDbLock(
                            entry.getKey(), new ArrayList<>(entry.getValue()), LockType.READ);
                } catch (Throwable releaseEx) {
                    LOG.warn("PlannerMetaLocker.lock partial rollback: db {} release failed, continuing",
                            entry.getKey(), releaseEx);
                }
            }
            throw t;
        }
    }

    public void unlock() {
        if (heldStack.isEmpty()) {
            // No acquisition currently in scope. Either lock()/tryLock() was never called
            // successfully on this instance, or all prior acquisitions have already been
            // released. This is the safety net that lets callers pair lock() with unlock() in
            // a finally block without spurious "Attempt to unlock lock" when lock() failed
            // partway and rolled back.
            return;
        }
        Locker locker = new Locker();
        // Pop the most recent acquisition and release its entries in reverse order — the
        // mirror image of how lock() acquired them. Each unlock() drops exactly one frame, so
        // nested lock()/unlock() pairs stay symmetric and the underlying reentrant LockManager
        // refCount is decremented once per rid per pop.
        List<Map.Entry<Long, Set<Long>>> entriesToRelease = heldStack.pop();
        for (int i = entriesToRelease.size() - 1; i >= 0; i--) {
            Map.Entry<Long, Set<Long>> entry = entriesToRelease.get(i);
            Database database = dbs.get(entry.getKey());
            List<Long> tableIds = new ArrayList<>(entry.getValue());
            try {
                locker.unLockTablesWithIntensiveDbLock(database.getId(), tableIds, LockType.READ);
            } catch (Throwable t) {
                // A fully-balanced lock state should never trigger this; if it does, log and
                // continue so the remaining entries still get released.
                LOG.warn("PlannerMetaLocker.unlock: db {} release failed, continuing", database.getId(), t);
            }
        }
    }

    @Override
    public void close() {
        // AutoCloseable contract: release everything this instance still holds, regardless of
        // how many lock()/tryLock() calls have not yet been matched by an explicit unlock().
        // Drain the whole stack so try-with-resources cleanup is total even if the body did
        // multiple lock()s. (No current caller does that, but the contract should hold.)
        while (!heldStack.isEmpty()) {
            unlock();
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
