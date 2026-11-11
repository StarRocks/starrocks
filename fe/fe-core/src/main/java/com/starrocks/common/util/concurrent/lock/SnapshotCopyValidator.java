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

import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.util.concurrent.lock.LockInvariantViolations.Mode;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

/**
 * Enforces the read half of the metadata-lock contract for the one read that matters most to
 * planning:
 *
 * <blockquote>a planning snapshot of a published table is taken under that table's lock</blockquote>
 *
 * {@link OlapTable#copyOnlyForQuery} walks the live partition, index and column maps of the table it
 * copies. Those maps are mutated in place by DDL, by INSERT OVERWRITE swapping partitions and by a
 * follower's replayer, each under the table's (or its database's) WRITE lock. A copy taken without a
 * READ lock races them: at best the statement fails with a ConcurrentModificationException, at worst it
 * plans off metadata that was never published as a whole. POST-1966 was this bug -- a view's base
 * tables were copied without their lock -- and nothing in the unit tests could see it, because a copy
 * taken without a lock works perfectly on a single thread.
 *
 * <p>This check makes it visible on a single thread: it asks whether the copier holds the lock, not
 * whether a race happened.
 *
 * <h3>What counts as holding the lock</h3>
 *
 * READ or WRITE on the table, or READ or WRITE on its database. An intention lock (IS/IX) on the
 * database does not count: it is what the intensive path takes <em>alongside</em> a table lock, and on
 * its own it does not exclude a writer of this table.
 *
 * <h3>What is not checked</h3>
 * <ul>
 *     <li>A table that is not a lock target ({@link OlapTable#isMetaLockTarget()} false, e.g.
 *         {@code ExternalOlapTable}): no lock protects it, so there is nothing to have held.</li>
 *     <li>A copy of a copy ({@link OlapTable#isPlanningSnapshot()}): the source is private to the
 *         statement. Copies carry an explicit mark rather than being recognised by the metastore no longer
 *         returning them, because a published table that was just dropped or replaced also stops being
 *         returned, and is exactly the object a racing copier would be reading.</li>
 *     <li>A table whose id no database knows: one built by a test, or by a code path that never
 *         publishes it. There is no lock id to have held.</li>
 *     <li>A copy inside {@link #exemptKnownViolation}: an existing unlocked site whose fix is tracked on its
 *         own, named at the call site with the reason.</li>
 * </ul>
 *
 * <p>Off the violation path the cost is one thread-local read plus up to two sharded lock-table
 * lookups, none of which copies the lock's owner set. Only a copier holding no table lock pays for finding
 * the table's database: a map lookup by the database id the analyzer recorded on the table, and a scan of
 * the database map only when that id is missing or stale.
 *
 * @see LockTargetValidator the same channel, enforcing who may be locked at all
 */
public class SnapshotCopyValidator {
    private static final Logger LOG = LogManager.getLogger(SnapshotCopyValidator.class);

    /** Grep token, the report's {@code kind=} field. See {@link LockInvariantViolations#LOG_TAG}. */
    static final String KIND_SNAPSHOT_COPY_WITHOUT_LOCK = "snapshot_copy_without_lock";

    private static final String REMEDY =
            "A planning copy of a published table reads its live partition and index maps, which DDL, "
                    + "INSERT OVERWRITE and the replayer mutate in place under the WRITE lock. Take the table's "
                    + "READ lock (or the database's) around the copy -- for a statement, make sure "
                    + "PlannerMetaLocker collects the table. A test that copies a published table on purpose "
                    + "without its lock should call LockTestUtils.disableSnapshotCopyValidation() and say why";

    /**
     * The copy plumbing between the guard and whoever decided to copy. Reporting one of these would
     * name the same frame every time, and it is never the code that has to change.
     */
    private static final Set<String> COPY_METHODS =
            ImmutableSet.of("copyOnlyForQuery", "buildBookmarkScopedTable", "getShadowCopyTable");

    /**
     * The known violation the current thread is inside, if any. Read first, so a copy inside an exempted
     * scope pays for neither the lock lookups nor the database search.
     */
    private static final ThreadLocal<String> KNOWN_VIOLATION = new ThreadLocal<>();

    private SnapshotCopyValidator() {
    }

    /**
     * Scope in which an unlocked copy is a known, recorded violation rather than a new one. For an existing
     * call site whose fix is tracked elsewhere, so that the check can be enforced everywhere else in the meantime.
     * Not for silencing a site nobody has looked at: {@code reason} has to say why the copy is unlocked and where
     * the fix is tracked.
     */
    public static KnownViolation exemptKnownViolation(String reason) {
        return new KnownViolation(reason);
    }

    public static final class KnownViolation implements AutoCloseable {
        private final String previous;

        private KnownViolation(String reason) {
            this.previous = KNOWN_VIOLATION.get();
            KNOWN_VIOLATION.set(reason);
        }

        @Override
        public void close() {
            if (previous == null) {
                KNOWN_VIOLATION.remove();
            } else {
                KNOWN_VIOLATION.set(previous);
            }
        }
    }

    /**
     * Report if the current thread is about to copy {@code source} for planning without holding its
     * lock. Mounted at the entry of {@link OlapTable#copyOnlyForQuery}, which every subclass reaches
     * through {@code super}.
     */
    public static void validateCopySource(OlapTable source) {
        Mode mode = LockInvariantViolations.currentSnapshotCopyMode();
        if (mode == Mode.OFF || KNOWN_VIOLATION.get() != null) {
            return;
        }

        // Verdict inside the catch-all, report outside it, as in LockTargetValidator: the exception ERROR
        // mode raises must not be swallowed by the catch that absorbs faults in the inspected state.
        String detail;
        try {
            detail = describeUnlockedCopy(source);
        } catch (Exception e) {
            LOG.debug("snapshot copy validation skipped, table could not be inspected", e);
            return;
        }

        if (detail != null) {
            LockInvariantViolations.reportAtSite(KIND_SNAPSHOT_COPY_WITHOUT_LOCK, detail, REMEDY, mode,
                    callerOfTheCopy());
        }
    }

    private static String describeUnlockedCopy(OlapTable source) {
        if (source.isPlanningSnapshot() || !source.isMetaLockTarget()) {
            return null;
        }

        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        LockManager lockManager = state.getLockManager();
        long tableId = source.getId();
        boolean underLock = LockHoldDepth.isUnderLock();
        if (underLock && holdsReadOrWrite(lockManager, tableId)) {
            return null;
        }

        Database db = owningDatabase(state, source);
        if (db == null) {
            return null;
        }
        if (underLock && holdsReadOrWrite(lockManager, db.getId())) {
            return null;
        }
        return "copying table " + db.getFullName() + "." + source.getName() + " (id=" + tableId
                + ") for planning while holding neither its READ/WRITE lock nor its database's (id="
                + db.getId() + "); this thread holds " + LockHoldDepth.current() + " FE metadata lock(s)";
    }

    private static boolean holdsReadOrWrite(LockManager lockManager, long rid) {
        return lockManager.isReadOrWriteOwnedByCurrentThread(rid);
    }

    /**
     * The database that publishes a table with this id, or null if none does. Matched by id, not by object:
     * a copier still holding an object that was replaced under the same id is reading what the lock on that
     * id protects. Only an olap table counts as the publisher: the system databases mint their own small ids, which
     * a table that was never published can share.
     *
     * <p>The database id the analyzer records on the table is tried first, so a copier holding only its
     * database's lock does not scan every database on every copy. It is a hint, verified the same way.
     */
    private static Database owningDatabase(GlobalStateMgr state, OlapTable source) {
        long tableId = source.getId();
        Long dbIdHint = source.dbId;
        if (dbIdHint != null) {
            Database db = state.getLocalMetastore().getDb(dbIdHint);
            if (db != null && db.getTable(tableId) instanceof OlapTable) {
                return db;
            }
        }
        for (Database db : state.getLocalMetastore().getIdToDb().values()) {
            if (db.getTable(tableId) instanceof OlapTable) {
                return db;
            }
        }
        return null;
    }

    /**
     * The innermost frame outside the lock layer and the copy plumbing -- the code that decided to copy.
     * Walks the stack (lazily, see {@link LockInvariantViolations#STACK_WALKER}), so it runs only once a
     * violation has already been established.
     */
    private static String callerOfTheCopy() {
        return LockInvariantViolations.STACK_WALKER.walk(frames -> frames
                .filter(frame -> !LockInvariantViolations.isJdkOrLockLayer(frame.getClassName())
                        && !COPY_METHODS.contains(frame.getMethodName()))
                .findFirst()
                .map(LockInvariantViolations::siteOf)
                .orElse(LockInvariantViolations.UNKNOWN_SITE));
    }
}
