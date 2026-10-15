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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.View;
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
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.MergeIntoStmt;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
     * Bound on every later acquisition through {@link #lockForPlanning()}, or negative for none. A caller that
     * takes the lock with {@link #tryLock} sets it, so that a planner which releases the lock mid-way and takes
     * it back -- the optimistic INSERT path -- waits no longer on the way back than the caller did going in.
     */
    private long acquireTimeoutMs = -1;

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

    /** A view whose body was walked for the lock set, as it was when walked. */
    private record WalkedView(TableName name, View view, String inlineViewDef, long sqlMode) {
    }

    // The session the views were resolved in; null for a locker built by forAllTablesOf, which walks no view body.
    private final ConnectContext session;
    private final List<WalkedView> walkedViews;
    private boolean walkedViewsChecked = false;
    // Whether the statement reads an external table a policy may apply to; see mayReachExternalTablePolicy.
    private boolean mayReachExternalTablePolicy = false;

    public PlannerMetaLocker(ConnectContext session, StatementBase statementBase) {
        TableCollector collector = new TableCollector(session, dbs, tables);
        collector.visit(statementBase);
        // The databases the statement names, not those of the tables behind a view: they classify the query
        // (resource groups and the like) and stay what the user wrote.
        session.setCurrentSqlDbIds(new HashSet<>(collector.statementDbIds));
        this.queryId = session.getQueryId();
        this.session = session;
        this.walkedViews = collector.walkedViews;
        this.mayReachExternalTablePolicy = collector.mayReachExternalTablePolicy;
    }

    /**
     * Whether the statement reads, directly or through a view, an external table a row access or column masking
     * policy may apply to. Such a policy may read internal tables, and only resolving the external table -- the
     * unlocked pre-pass does that -- tells which: see {@link #addTablesReadByExternalTablePolicies}. So the pre-pass
     * has to run for it even when the statement has no internal table of its own to lock.
     */
    public boolean mayReachExternalTablePolicy() {
        return mayReachExternalTablePolicy;
    }

    private PlannerMetaLocker(UUID queryId, long acquireTimeoutMs) {
        this.queryId = queryId;
        this.acquireTimeoutMs = acquireTimeoutMs;
        this.session = null;
        this.walkedViews = List.of();
    }

    /**
     * Fail when a view whose body was walked for the lock set has changed since: its name now stands for another
     * view, or it is defined differently. The bodies are walked before the lock is taken, so a view redefined in
     * between would be expanded by the analyzer to tables this lock does not hold, and read without it.
     *
     * <p>Run once, right after the first acquisition: the views are locked from then on and cannot change any more.
     * A later acquisition -- {@code InsertPlanner} taking the lock back, {@code StatementPlanner#reAnalyzeStmt} --
     * happens after analysis, and whatever it finds is the planner's to validate. Here it fails the statement, which
     * is the caller's to retry: for every user of this class, not only {@code StatementPlanner}, the alternative is
     * planning off tables nobody locked.
     */
    private void checkWalkedViewsOnFirstLock() {
        if (walkedViewsChecked) {
            return;
        }
        walkedViewsChecked = true;
        for (WalkedView walked : walkedViews) {
            Pair<Database, Table> current = resolveTable(session, walked.name());
            View view = walked.view();
            if (current == null || current.second != view || view.getSqlMode() != walked.sqlMode()
                    || !Objects.equals(view.getInlineViewDef(), walked.inlineViewDef())) {
                unlock();
                throw new StarRocksPlannerException(String.format("View %s was changed while the statement was "
                        + "being planned, please retry", walked.name()), ErrorType.INTERNAL_ERROR);
            }
        }
    }

    /**
     * A locker for every table {@code analyzedStatement} reads, including what analysis brought in: the tables
     * behind a view, the ones a row access or column masking policy reads. The constructor, run on the statement
     * as parsed, sees a view as a name and nothing more.
     *
     * <p>Unlike the constructor, this leaves the session's statement databases alone: they classify the query
     * (resource groups and the like) and stay what the user wrote.
     */
    public PlannerMetaLocker forAllTablesOf(ConnectContext session, StatementBase analyzedStatement) {
        PlannerMetaLocker all = new PlannerMetaLocker(queryId, acquireTimeoutMs);
        new TableCollector(session, all.dbs, all.tables).visit(analyzedStatement);
        return all;
    }

    /** {@link #forAllTablesOf}, less the tables this locker already locks. */
    public PlannerMetaLocker forTablesNotLockedOf(ConnectContext session, StatementBase analyzedStatement) {
        PlannerMetaLocker missing = forAllTablesOf(session, analyzedStatement);
        for (Map.Entry<Long, Set<Long>> entry : tables.entrySet()) {
            Set<Long> tableIds = missing.tables.get(entry.getKey());
            if (tableIds == null) {
                continue;
            }
            tableIds.removeAll(entry.getValue());
            if (tableIds.isEmpty()) {
                missing.tables.remove(entry.getKey());
                missing.dbs.remove(entry.getKey());
            }
        }
        return missing;
    }

    /**
     * Add the tables read by the policies of the external tables the unlocked analysis pre-pass resolved: the
     * constructor cannot see those, since finding an external table's policies takes the table, i.e. the connector,
     * which it keeps off. The pre-pass has resolved them by now -- outside the lock, on the relations themselves --
     * and stashed what the policy lookups need, so this asks the catalog nothing. Call it before the first
     * acquisition.
     */
    public void addTablesReadByExternalTablePolicies(ConnectContext session, StatementBase statement) {
        // Only walks policy bodies: no context table function of the statement is reached, nor authorized, here.
        TableCollector collector = new TableCollector(session, dbs, tables, false);
        collector.externalPoliciesOnly = true;
        collector.visit(statement);
    }

    /** Whether every one of {@code boundTables} is among the tables this locker locks, by id. */
    public boolean locksAll(Collection<? extends Table> boundTables) {
        return boundTables.stream().allMatch(table -> locksTable(table.getId()));
    }

    /** Whether every table {@code other} locks is among the tables this locker locks. */
    public boolean locksAll(PlannerMetaLocker other) {
        return other.tables.values().stream().flatMap(Set::stream).allMatch(this::locksTable);
    }

    private boolean locksTable(long tableId) {
        return tables.values().stream().anyMatch(tableIds -> tableIds.contains(tableId));
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
        checkWalkedViewsOnFirstLock();
        return true;
    }

    public boolean isEmpty() {
        return tables.isEmpty();
    }

    public void setAcquireTimeoutMs(long acquireTimeoutMs) {
        this.acquireTimeoutMs = acquireTimeoutMs;
    }

    /**
     * {@link #lock()}, unless {@link #setAcquireTimeoutMs} bounded the wait: then {@link #tryLock} with that bound,
     * throwing {@link AcquireTimeoutException} when it runs out. Nothing is held after the throw.
     */
    public void lockForPlanning() {
        if (acquireTimeoutMs < 0) {
            lock();
        } else if (!tryLock(acquireTimeoutMs, TimeUnit.MILLISECONDS)) {
            throw new AcquireTimeoutException(acquireTimeoutMs);
        }
    }

    public static class AcquireTimeoutException extends RuntimeException {
        public AcquireTimeoutException(long timeoutMs) {
            super("failed to re-acquire the planner meta lock within " + timeoutMs + " ms");
        }
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
        // Outside the rollback above: a failed check releases what this acquisition took itself.
        checkWalkedViewsOnFirstLock();
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

    /** {@code tableName} with the session's catalog and database filled in, or null when it stays partial. */
    private static TableName qualify(ConnectContext session, TableName tableName) {
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
        return new TableName(catalogName, dbName, tbName);
    }

    private static Pair<Database, Table> resolveTable(ConnectContext session, TableName tableName) {
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();

        TableName qualified = qualify(session, tableName);
        if (qualified == null) {
            return null;
        }
        String catalogName = qualified.getCatalog();
        String dbName = qualified.getDb();
        String tbName = qualified.getTbl();

        // Judged on the catalog *name*, because that is what the database is about to be resolved through:
        // any other catalog, resource-mapping included, routes getDb to the connector and yields a
        // connector-minted id that must never be locked. Returning early also keeps lock collection off the
        // network. The check below re-asks the same question of the resolved table object, where the answer
        // for resource-mapping is the opposite one -- see Table#isMetaLockTarget.
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

        // Only lock what the lock can protect. Same predicate AnalyzerUtils.CopyUnsafeTablesCollector uses to
        // decide who may extend the lock's lifetime, so who gets locked and who decides for how long cannot
        // drift apart. This is what leaves out the internal-database tables whose data lives elsewhere and
        // whose definition is never written in place (resource-mapping HIVE/ICEBERG/HUDI, FILE, MYSQL, JDBC,
        // ELASTICSEARCH, ExternalOlapTable), and it is the anchor for asserting the invariant inside Locker later.
        if (!table.isMetaLockTarget()) {
            return null;
        }

        return new Pair<>(db, table);
    }

    private static class TableCollector extends AstTraverser<Void, Void> {
        private final ConnectContext session;

        private final Map<Long, Database> dbs;
        private final Map<Long, Set<Long>> tables;
        private final Set<Long> statementDbIds = new HashSet<>();
        // > 0 while inside the body of a view
        private int resolvedDepth = 0;
        // The views whose bodies are being walked, so that a definition rewritten into a cycle cannot recurse forever
        private final Set<Long> expandingViews = new HashSet<>();
        // The CTE names of the with clauses in scope, innermost first. A table of the same name stays unlocked only
        // where the analyzer would read the CTE instead, never more: that would leave a table it reads unlocked.
        private final Deque<Set<String>> cteNames = new ArrayDeque<>();
<<<<<<< HEAD
=======
        private final List<WalkedView> walkedViews = new ArrayList<>();
        // Whether the relations being walked get a policy injected by the analyzer, besides the ones the statement
        // flags itself: everything in the body of a view so flagged (see QueryAnalyzer#inheritPolicyRewriteFlag).
        private boolean inBodyRewrittenByPolicy = false;
        // Collect nothing but what the policies of already resolved external tables read.
        private boolean externalPoliciesOnly = false;
        private boolean mayReachExternalTablePolicy = false;
        // Whether a context table function the session itself is authorized for is authorized before it runs: one the
        // statement calls directly, or one in the body of a SECURITY INVOKER view it reads. One in any other view's
        // body is not, since a view's reader reads it with the view's privilege (see AgentBaseRelations#resolve)
        private final boolean authorizeStatementCalls;
>>>>>>> 3d28e7f5daf... [BugFix][POST-1966] Close the remaining planner and MV metadata lock gaps (#65384)

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
            // Same reason as the MERGE INTO override below: this runs before Analyzer.analyze, so
            // getQueryStatement() is null and super's traversal reaches nothing. Until the analyzer folds
            // them into one query, the tables an UPDATE reads live in the raw FROM clause, the CTEs, the
            // where predicate's subqueries and the assignment expressions.
            visitRawDmlClauses(node.getCommonTableExpressions(), node.getFromRelations(), node.getWherePredicate());
            if (node.getAssignments() != null) {
                node.getAssignments().forEach(assignment -> visitIfPresent(assignment.getExpr()));
            }
            return super.visitUpdateStatement(node, context);
        }

        @Override
        public Void visitDeleteStatement(DeleteStmt node, Void context) {
            TableRef tableRef = node.getTableRef();
            TableName tableName = TableName.fromTableRef(tableRef);
            Pair<Database, Table> dbAndTable = resolveTable(session, tableName);
            put(dbAndTable);
            // See the UPDATE override: the raw USING clause, the CTEs and the where predicate's subqueries
            // are where a DELETE's other tables are before the analyzer builds its query statement.
            visitRawDmlClauses(node.getCommonTableExpressions(), node.getUsingRelations(),
                    node.getWherePredicate());
            return super.visitDeleteStatement(node, context);
        }

        /**
         * Lock what planning will read, and therefore what it may snapshot: {@code StatementPlanner} copies
         * every OlapTable of the analyzed statement while this lock is held, and a table that never made it
         * into the lock set would be copied without one.
         */
        private void visitRawDmlClauses(List<CTERelation> cteRelations, List<Relation> relations,
                                        Expr wherePredicate) {
            withCtes(cteRelations, () -> {
                if (cteRelations != null) {
                    cteRelations.forEach(this::visitIfPresent);
                }
                if (relations != null) {
                    relations.forEach(this::visitIfPresent);
                }
                visitIfPresent(wherePredicate);
            });
        }

        private void visitIfPresent(ParseNode node) {
            if (node != null) {
                visit(node);
            }
        }

        @Override
        public Void visitMergeIntoStatement(MergeIntoStmt node, Void context) {
            TableRef tableRef = node.getTableRef();
            TableName tableName = TableName.fromTableRef(tableRef);
            Pair<Database, Table> dbAndTable = resolveTable(session, tableName);
            put(dbAndTable);
            // PlannerMetaLocker is constructed in StatementPlanner BEFORE
            // Analyzer.analyze runs, so node.getQueryStatement() is null at
            // this point. super's default traversal walks the analyzed
            // QueryStatement and would skip the raw USING source — visit the
            // source relation explicitly so its tables are locked alongside
            // the target before the analyzer resolves the join.
            if (node.getSourceRelation() != null) {
                visit(node.getSourceRelation());
            }
            return super.visitMergeIntoStatement(node, context);
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
        public Void visitSelect(SelectRelation node, Void context) {
            withCtes(node.getCteRelations(), () -> super.visitSelect(node, context));
            return null;
        }

        @Override
        public Void visitSetOp(SetOperationRelation node, Void context) {
            withCtes(node.getCteRelations(), () -> super.visitSetOp(node, context));
            return null;
        }

        /** Run {@code body} in a new with-clause scope, which {@link #visitCTE} fills as the CTEs are walked. */
        private void withCtes(List<CTERelation> cteRelations, Runnable body) {
            if (cteRelations == null || cteRelations.isEmpty()) {
                body.run();
                return;
            }
            cteNames.push(new HashSet<>());
            try {
                body.run();
            } finally {
                cteNames.pop();
            }
        }

        /**
         * A CTE's name is in scope only after its own definition, as the analyzer has it: in
         * {@code WITH t0 AS (SELECT * FROM t0)} the inner {@code t0} is the table.
         */
        @Override
        public Void visitCTE(CTERelation node, Void context) {
            super.visitCTE(node, context);
            if (!cteNames.isEmpty() && node.getName() != null) {
                cteNames.peek().add(node.getName());
            }
            return null;
        }

        /** An unqualified name a with clause in scope declares: the analyzer reads the CTE, not a table. */
        private boolean isCteReference(TableName name) {
            return name != null && Strings.isNullOrEmpty(name.getCatalog()) && Strings.isNullOrEmpty(name.getDb())
                    && cteNames.stream().anyMatch(names -> names.contains(name.getTbl()));
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
<<<<<<< HEAD
            if (isCteReference(node.getName())) {
=======
            if (externalPoliciesOnly) {
                visitExternalTablePolicies(node);
                return null;
            }
            if (isCteReference(node.getName()) || visitResolved(node)) {
>>>>>>> 3d28e7f5daf... [BugFix][POST-1966] Close the remaining planner and MV metadata lock gaps (#65384)
                return null;
            }
            noteExternalTablePolicy(node);
            Pair<Database, Table> dbAndTable = resolveTable(session, node.getName());
            put(dbAndTable);
<<<<<<< HEAD
            if (dbAndTable != null && dbAndTable.second instanceof View view) {
                visitViewBody(view);
=======
            if (dbAndTable == null) {
                return null;
            }
            boolean rewrittenByPolicy = (inBodyRewrittenByPolicy || node.isNeedRewrittenByPolicy())
                    && !node.isSyncMVQuery() && !node.isCacheStatsQuery();
            if (rewrittenByPolicy) {
                visitPolicyBodies(new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        dbAndTable.first.getFullName(), dbAndTable.second.getName()), dbAndTable.second);
            }
            if (dbAndTable.second instanceof View view) {
                walkedViews.add(new WalkedView(node.getName(), view, view.getInlineViewDef(), view.getSqlMode()));
                visitViewBody(node, view, rewrittenByPolicy);
>>>>>>> 3d28e7f5daf... [BugFix][POST-1966] Close the remaining planner and MV metadata lock gaps (#65384)
            }
            return null;
        }

        /**
         * Before analysis a view is a name and nothing more, yet planning reads the tables behind it -- and copies
         * them, when it runs off copies. Lock those too, by walking the body here.
         */
<<<<<<< HEAD
        private void visitViewBody(View view) {
            if (!expandingViews.add(view.getId())) {
                return;
            }
            // A view body is its own name scope: a CTE of the enclosing statement does not shadow a table in it.
            Deque<Set<String>> enclosingCtes = new ArrayDeque<>(cteNames);
            cteNames.clear();
            resolvedDepth++;
            try {
                visit(view.getQueryStatement());
            } catch (StarRocksPlannerException e) {
                // A definition that does not parse is the analyzer's to report.
            } finally {
                resolvedDepth--;
=======
        private void visitViewBody(TableRelation reference, View view, boolean rewrittenByPolicy) {
            if (!expandingViews.add(view.getId())) {
                return;
            }
            // ColumnPrivilege.checkViewPrivilege authorizes a SECURITY INVOKER view's body as the reader. The name
            // scope and resolvedDepth are visitOutsideTheStatement's.
            boolean invoker = view.isSecurity();
            if (invoker) {
                invokerViewDepth++;
            }
            try {
                QueryStatement body = parseViewBody(reference, view);
                if (body != null) {
                    visitOutsideTheStatement(rewrittenByPolicy, () -> visit(body));
                }
            } finally {
                if (invoker) {
                    invokerViewDepth--;
                }
>>>>>>> 3d28e7f5daf... [BugFix][POST-1966] Close the remaining planner and MV metadata lock gaps (#65384)
                expandingViews.remove(view.getId());
            }
        }

        /**
         * The analyzer applies a row access or column masking policy by injecting it as a subquery over the relation
         * it guards (SecurityPolicyRewriteRule), and a policy may read other tables -- a mapping table, say. Before
         * analysis nothing of that is in the statement; lock those tables too, by walking the policy expressions here.
         * The analyzer does not apply policies to what a policy reads, so neither does this.
         */
        private void visitPolicyBodies(TableName name, Table table) {
            List<Expr> bodies = new ArrayList<>();
            try {
                Expr rowAccess = Authorizer.getRowAccessPolicy(session, name);
                if (rowAccess != null) {
                    bodies.add(rowAccess);
                }
                // The columns SecurityPolicyRewriteRule#buildView masks.
                List<Column> columns = table.getBaseSchema().stream()
                        .filter(c -> !c.getType().isUnknown()).filter(Column::isVisible).toList();
                Map<String, Expr> masking = Authorizer.getColumnMaskingPolicy(session, name, columns);
                if (masking != null) {
                    bodies.addAll(masking.values());
                }
            } catch (RuntimeException e) {
                // The analyzer looks the policies up again, and reports what fails.
                return;
            }
            if (!bodies.isEmpty()) {
                boolean enclosingExternalPoliciesOnly = externalPoliciesOnly;
                externalPoliciesOnly = false;
                try {
                    visitOutsideTheStatement(false, () -> bodies.forEach(this::visit));
                } finally {
                    externalPoliciesOnly = enclosingExternalPoliciesOnly;
                }
            }
        }

        /** Asks the access controller only, by name: resolving the external table is the pre-pass's to do. */
        private void noteExternalTablePolicy(TableRelation node) {
            if (mayReachExternalTablePolicy || node.getName() == null
                    || !(inBodyRewrittenByPolicy || node.isNeedRewrittenByPolicy())) {
                return;
            }
            TableName name = qualify(session, node.getName());
            if (name != null && !CatalogMgr.isInternalCatalog(name.getCatalog())) {
                try {
                    mayReachExternalTablePolicy = Authorizer.mayHavePolicy(session, name);
                } catch (RuntimeException e) {
                    // A catalog the access controllers do not know: the analyzer reports it.
                }
            }
        }

        /**
         * An external table the pre-pass resolved carries its table, and is named the way the pre-pass keyed what it
         * stashed for the policy lookups (see Authorizer#preResolvePolicyTarget). An internal view is followed into
         * the bodies the pre-pass resolved for it (PreResolvedViewBodies), where its external tables are resolved the
         * same way; the view inherits the policy rewrite to its body, as in QueryAnalyzer#inheritPolicyRewriteFlag.
         * What the pre-pass did not resolve is left to the analyzer, as before.
         */
        private void visitExternalTablePolicies(TableRelation node) {
            if (isCteReference(node.getName()) || node.isSyncMVQuery() || node.isCacheStatsQuery()) {
                return;
            }
            TableName name = node.getName() == null ? null : qualify(session, node.getName());
            if (name == null) {
                return;
            }
            boolean rewrittenByPolicy = inBodyRewrittenByPolicy || node.isNeedRewrittenByPolicy();
            if (!CatalogMgr.isInternalCatalog(name.getCatalog())) {
                if (rewrittenByPolicy && node.getTable() != null) {
                    visitPolicyBodies(name, node.getTable());
                }
                return;
            }
            Pair<Database, Table> dbAndTable = resolveTable(session, name);
            if (node.isCreateByPolicyRewritten()) {
                // Already analyzed -- the deferred-lock INSERT analyzes its SELECT before taking the lock -- so a
                // policy is in the statement as the subquery it was rewritten to, and this table is what it reads.
                resolvedDepth++;
                try {
                    put(dbAndTable);
                } finally {
                    resolvedDepth--;
                }
                return;
            }
            if (dbAndTable == null || !(dbAndTable.second instanceof View view) || !expandingViews.add(view.getId())) {
                return;
            }
            try {
                // The body parsed for this reference when the lock set was built, which the pre-pass walked in place
                // (see #parseViewBody); else the ones the pre-pass parsed itself.
                PreResolvedState state = session.getPreResolvedState();
                QueryStatement parsed = state.parsedViewBody(node, view);
                List<QueryStatement> bodies = parsed != null ? List.of(parsed) : state.viewBodies().peek(view);
                visitOutsideTheStatement(rewrittenByPolicy, () -> bodies.forEach(this::visit));
            } finally {
                expandingViews.remove(view.getId());
            }
        }

        /**
         * Walk what the analyzer will bring into the statement: tables locked, but not counted among the databases
         * the statement names. It is its own name scope -- a CTE of the enclosing statement does not shadow a table
         * in it.
         */
        private void visitOutsideTheStatement(boolean rewrittenByPolicy, Runnable walk) {
            Deque<Set<String>> enclosingCtes = new ArrayDeque<>(cteNames);
            boolean enclosingRewrittenByPolicy = inBodyRewrittenByPolicy;
            cteNames.clear();
            inBodyRewrittenByPolicy = rewrittenByPolicy;
            resolvedDepth++;
            try {
                walk.run();
            } catch (StarRocksPlannerException e) {
                // A definition that does not parse is the analyzer's to report.
            } finally {
                resolvedDepth--;
                inBodyRewrittenByPolicy = enclosingRewrittenByPolicy;
                cteNames.clear();
                cteNames.addAll(enclosingCtes);
            }
        }

<<<<<<< HEAD
=======
        /**
         * The body the analyzer will expand this reference from: the calls resolved while walking it must be the ones
         * it meets, or it resolves them again, under the lock. So the body is filed under the reference, with a copy of
         * the definition it was parsed from -- the view is read without its lock, and ALTER VIEW rewrites the
         * definition in place -- and the unlocked pre-pass and the analyzer take it (see
         * {@link PreResolvedState#parsedViewBody}). The definition is parsed once, and walking the statement again
         * walks the same body.
         *
         * @return null for a definition that will not parse, which is the analyzer's to report
         */
        private QueryStatement parseViewBody(TableRelation reference, View view) {
            PreResolvedState state = PreResolvedState.of(session);
            QueryStatement body = state == null ? null : state.parsedViewBody(reference, view);
            if (body != null) {
                return body;
            }
            View parsed = new View(view.getId(), view.getName(), view.getFullSchema());
            parsed.setInlineViewDefWithSqlMode(view.getInlineViewDef(), view.getSqlMode());
            try {
                body = parsed.getQueryStatement();
            } catch (StarRocksPlannerException e) {
                return null;
            }
            if (state != null) {
                state.putParsedViewBody(reference, parsed, body);
            }
            return body;
        }

        @Override
        public Void visitTableFunction(TableFunctionRelation node, Void context) {
            if (externalPoliciesOnly) {
                return null;
            }
            visitResolved(node);
            return null;
        }

        @Override
        public Void visitNormalizedTableFunction(NormalizedTableFunctionRelation node, Void context) {
            // The call on the right, or once analyzed the relation it became; the left side is the dual row.
            return visit(node.getRight(), context);
        }

        /**
         * An analyzed statement (SPM analyzes before planning does; re-planning analyzes again) carries what an
         * AgentBase relation resolved to in place of the call. It is still not something the statement named.
         */
        @Override
        public Void visitSubqueryRelation(SubqueryRelation node, Void context) {
            PreResolvedState state = PreResolvedState.of(session);
            if (state == null || !state.isAgentBaseResolution(node)) {
                return super.visitSubqueryRelation(node, context);
            }
            resolvedDepth++;
            try {
                return super.visitSubqueryRelation(node, context);
            } finally {
                resolvedDepth--;
            }
        }

        /**
         * Resolve an AgentBase relation here, before the lock is taken (see {@link AgentBaseRelations}), and
         * collect the tables it reads.
         *
         * <p>Inside a view body too: the analyzer expands the view from the body walked here (see
         * {@link #parseViewBody}), so it finds the call answered and does not run it again under the lock.
         *
         * @return whether {@code node} was one
         */
        private boolean visitResolved(Relation node) {
            Relation resolved = AgentBaseRelations.resolve(session, node, authorizeStatementCalls && isSessionCall());
            if (resolved == null) {
                return false;
            }
            resolvedDepth++;
            try {
                visit(resolved);
            } finally {
                resolvedDepth--;
            }
            return true;
        }

        /**
         * Whether a call met here is one the statement's authorization checks as the session: every level that
         * encloses it is the body of a SECURITY INVOKER view, or there is none.
         */
        private boolean isSessionCall() {
            return resolvedDepth == invokerViewDepth;
        }

        /**
         * CREATE VIEW's body is the statement's own query: it is analyzed with the statement, and authorized as the
         * session that creates the view (AuthorizerStmtVisitor checks it), so it is walked like a query.
         */
        @Override
        public Void visitCreateViewStatement(CreateViewStmt statement, Void context) {
            if (statement.getQueryStatement() != null) {
                visit(statement.getQueryStatement(), context);
            }
            return null;
        }

        /** The new body of an ALTER VIEW, which is analyzed and authorized like CREATE VIEW's. */
        @Override
        public Void visitAlterViewClause(AlterViewClause clause, Void context) {
            if (clause.getQueryStatement() != null) {
                visit(clause.getQueryStatement(), context);
            }
            return null;
        }

>>>>>>> 3d28e7f5daf... [BugFix][POST-1966] Close the remaining planner and MV metadata lock gaps (#65384)
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
                if (resolvedDepth == 0) {
                    statementDbIds.add(database.getId());
                }
            }
        }
    }
}
