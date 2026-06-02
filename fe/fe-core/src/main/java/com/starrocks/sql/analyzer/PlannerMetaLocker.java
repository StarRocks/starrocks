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

import com.google.api.client.util.Lists;
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

import java.util.ArrayList;
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
     * Entries successfully locked by the outermost {@link #lock()} / {@link #tryLock} that
     * is currently active on this instance. {@code unlock()} uses this snapshot to release
     * the right rids, regardless of how deeply nested the lock/unlock calls are.
     *
     * <p>Null when {@link #lockDepth} is 0 (no acquisition currently in scope, or already
     * fully released). See {@code lockDepth} for the depth semantics.
     */
    private List<Map.Entry<Long, Set<Long>>> heldEntries;

    /**
     * Number of currently-pending {@code lock()}/{@code tryLock()} acquisitions on this
     * instance — i.e., how many matching {@code unlock()} calls are still owed.
     *
     * <p>Nested acquisitions happen for real on the optimistic-retry path:
     * {@link com.starrocks.sql.InsertPlanner#buildExecPlanWithRetry} unlocks during planning
     * then re-locks before validation, and {@link com.starrocks.sql.StatementPlanner#reAnalyzeStmt}
     * additionally calls {@code lock()}/{@code unlock()} on the SAME instance inside the retry
     * iteration. The inner pair must not release the outer acquisition's LockManager refCount,
     * and the inner {@code unlock()} must not clear {@link #heldEntries} (otherwise the outer
     * {@code unlock()} would become a no-op and leak the locks).
     *
     * <p>{@code lock()} increments after a successful acquire; {@code unlock()} decrements
     * unconditionally when in-scope and clears {@code heldEntries} once it hits zero. A failed
     * {@code lock()} (partial-acquire that rolled itself back) does NOT increment, so the
     * outer state — if any — is preserved untouched.
     */
    private int lockDepth = 0;

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
        List<Database> lockedDbs = Lists.newArrayList();
        List<Map.Entry<Long, Set<Long>>> lockedEntries = new ArrayList<>(tables.size());
        try {
            for (Map.Entry<Long, Set<Long>> entry : tables.entrySet()) {
                Database database = dbs.get(entry.getKey());
                if (!locker.tryLockTablesWithIntensiveDbLock(database.getId(), new ArrayList<>(entry.getValue()),
                        LockType.READ, timeout, unit)) {
                    return false;
                }
                lockedDbs.add(database);
                lockedEntries.add(entry);
            }
            isLockSuccess = true;
            lockDepth++;
            // Only the outermost successful acquisition records the snapshot. Nested calls
            // re-enter the same rids in LockManager (refCount++) but the entry list is
            // unchanged because `tables` is fixed at construction time.
            if (lockDepth == 1) {
                heldEntries = lockedEntries;
            }
        } finally {
            if (!isLockSuccess) {
                for (Database database : lockedDbs) {
                    locker.unLockTablesWithIntensiveDbLock(database.getId(), new ArrayList<>(tables.get(database.getId())),
                            LockType.READ);
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
            lockDepth++;
            // Only the outermost successful acquisition records the snapshot. Nested lock()
            // calls re-enter the same rids in LockManager (refCount goes up by 1 per call) but
            // the snapshot is unchanged because `tables` is fixed at construction. Overwriting
            // here would let an inner unlock() clear the outer's record and turn the outer
            // unlock() into a no-op, leaking the locks. See POST-1561 review feedback.
            if (lockDepth == 1) {
                heldEntries = lockedEntries;
            }
        } catch (Throwable t) {
            // Partial-acquire rollback at the multi-db level. Inner
            // Locker.lockTablesWithIntensiveDbLock is atomic per-db (it rolls itself back on
            // failure), so a throw here means earlier (db, tableIds) entries are fully held
            // while the current and later ones are not. Release the earlier ones in reverse
            // order before re-throwing. {@code lockDepth} / {@code heldEntries} are NOT
            // modified — we never reached the increment, so outer state (if this was a nested
            // call) stays exactly as it was.
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
        if (lockDepth == 0) {
            // No acquisition currently in scope. Either lock()/tryLock() was never called
            // successfully on this instance, or all prior acquisitions have already been
            // released. This is the safety net that lets callers pair lock() with unlock() in
            // a finally block without spurious "Attempt to unlock lock" when lock() failed
            // partway and rolled back.
            return;
        }
        Locker locker = new Locker();
        // Each unlock() decrements LockManager refCount by 1 per rid. The snapshot
        // (heldEntries) is the same across nested unlocks because `tables` is fixed; we use
        // it for every release and only clear it on the outermost unlock.
        List<Map.Entry<Long, Set<Long>>> entriesToRelease = heldEntries;
        lockDepth--;
        if (lockDepth == 0) {
            heldEntries = null;
        }
        for (Map.Entry<Long, Set<Long>> entry : entriesToRelease) {
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
        unlock();
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
