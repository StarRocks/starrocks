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

package com.starrocks.load;

import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.load.batchwrite.MergeCommitTask;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.streamload.AbstractStreamLoadTask;
import com.starrocks.load.streamload.StreamLoadMultiStmtTask;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.thrift.TGetLoadsParams;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Decides how much of a load job a user may see in information_schema (loads, load_tracking_logs, routine_load_jobs,
 * stream_loads), in SHOW LOAD / SHOW STREAM LOAD / SHOW ROUTINE LOAD, in SHOW PROC and in the load info API. A job's
 * error log holds rejected rows of the tables it loads into, and its error-log URL, tracking SQL, error messages and
 * properties can quote such rows or lead to them. So the caller gets:
 * <ul>
 *   <li>{@link Access#FULL} if it is root or holds SELECT on every table the job loads into;</li>
 *   <li>{@link Access#METADATA}, the job without those fields ({@link LoadJobRedaction}), if it holds some privilege
 *       on every such table, or OPERATE;</li>
 *   <li>{@link Access#NONE} otherwise.</li>
 * </ul>
 *
 * <p>The privilege check looks a target table up by name and lets a missing one through, so it counts only for a
 * table its name still leads to. A job on a dropped table, on a temporary table (registered by id alone), or one
 * whose target tables are unknown, is therefore shown in full to root alone, and without those fields to OPERATE.
 * A caller without an identity sees no job. Privileges granted to the caller's groups are not seen here, because the
 * schema scan request carries the user and its roles only.
 */
public final class LoadJobVisibility {

    /** How much of a load job the caller may see. */
    public enum Access {
        /** The job is not shown. */
        NONE,
        /** The job is shown without the fields {@link LoadJobRedaction} removes. */
        METADATA,
        /** The job is shown in full. */
        FULL
    }

    /** A stream load task to show and how much of it. */
    public record VisibleTask(AbstractStreamLoadTask task, Access access) {
    }

    private LoadJobVisibility() {
    }

    /**
     * Whether the load job views follow this rule, the default. With {@code enable_new_load_job_privilege_check} off,
     * each view authorizes as it did before the rule, and shows every field: SHOW STREAM LOAD and SHOW ROUTINE LOAD
     * check INSERT on the target table (SHOW ROUTINE LOAD also any privilege on it), the load info API checks INSERT
     * in the database, and the information_schema views, SHOW LOAD and the SHOW PROC nodes check nothing.
     */
    public static boolean isEnabled() {
        return Config.enable_new_load_job_privilege_check;
    }

    /** Every statement of {@code task} in full, as a view without a check of its own shows it with the rule off. */
    public static List<VisibleTask> allTasksInFull(AbstractStreamLoadTask task) {
        List<VisibleTask> tasks = new ArrayList<>();
        if (task instanceof StreamLoadMultiStmtTask multiStmtTask) {
            for (StreamLoadTask child : multiStmtTask.getTasks()) {
                tasks.add(new VisibleTask(child, Access.FULL));
            }
        } else {
            tasks.add(new VisibleTask(task, Access.FULL));
        }
        return tasks;
    }

    /**
     * The caller carried by a schema scan request. Without an identity the context has no user, so every
     * visibility check on it fails.
     */
    public static ConnectContext callerOf(TGetLoadsParams request) {
        ConnectContext context = new ConnectContext();
        if (!request.isSetCurrent_user_ident()) {
            return context;
        }
        UserIdentityUtils.setAuthInfoFromThrift(context, request.getCurrent_user_ident());
        return context;
    }

    public static Access accessOf(ConnectContext context, LoadJob job) {
        return accessOf(context, job.getDbId(), job.getTableIds());
    }

    public static Access accessOf(ConnectContext context, RoutineLoadJob job) {
        return accessOf(context, job.getDbId(), List.of(job.getTableId()));
    }

    /**
     * The tasks to show for {@code task}, with how much of each: the task itself unless the caller may not see it. A
     * multi-statement transaction is split into its statements, and only those the caller may see are kept. Each
     * statement loads one table into its own error log, but a failed transaction gives every statement its error,
     * which can name the error-log URL of another statement; so a statement is shown in full only if every statement
     * is. The result is one snapshot, so build the output from it: a statement the transaction adds later is neither
     * in it nor authorized. Pass the transaction, not one of its statements.
     */
    public static List<VisibleTask> visibleTasks(ConnectContext context, AbstractStreamLoadTask task) {
        List<VisibleTask> visible = new ArrayList<>();
        if (task instanceof StreamLoadMultiStmtTask multiStmtTask) {
            List<StreamLoadTask> children = new ArrayList<>(multiStmtTask.getTasks());
            List<Access> accesses = new ArrayList<>();
            for (StreamLoadTask child : children) {
                accesses.add(accessOf(context, child.getDBId(), List.of(child.getTableId())));
            }
            boolean allFull = accesses.stream().allMatch(access -> access == Access.FULL);
            for (int i = 0; i < children.size(); i++) {
                Access access = accesses.get(i);
                addIfVisible(visible, children.get(i), access == Access.FULL && !allFull ? Access.METADATA : access);
            }
        } else {
            addIfVisible(visible, task, accessOf(context, task.getDBId(), List.of(targetTableId(task))));
        }
        return visible;
    }

    private static void addIfVisible(List<VisibleTask> visible, AbstractStreamLoadTask task, Access access) {
        if (access != Access.NONE) {
            visible.add(new VisibleTask(task, access));
        }
    }

    private static long targetTableId(AbstractStreamLoadTask task) {
        if (task instanceof StreamLoadTask streamLoadTask) {
            return streamLoadTask.getTableId();
        }
        if (task instanceof MergeCommitTask mergeCommitTask) {
            return mergeCommitTask.getTableId();
        }
        return -1;
    }

    private static Access accessOf(ConnectContext context, long dbId, Collection<Long> tableIds) {
        if (!hasUser(context)) {
            return Access.NONE;
        }
        if (isRoot(context)) {
            return Access.FULL;
        }
        Access access = tableAccess(context, dbId, tableIds);
        if (access == Access.NONE && Authorizer.hasSystemAction(context, PrivilegeType.OPERATE)) {
            return Access.METADATA;
        }
        return access;
    }

    /** The least access the caller has on any of the tables. */
    private static Access tableAccess(ConnectContext context, long dbId, Collection<Long> tableIds) {
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = metastore.getDb(dbId);
        if (db == null || tableIds.isEmpty()) {
            return Access.NONE;
        }
        Access access = Access.FULL;
        for (Long tableId : tableIds) {
            if (metastore.getTable(dbId, tableId) == null) {
                return Access.NONE;
            }
            Access tableAccess = tableAccess(context, metastore, db, tableId);
            if (tableAccess == Access.NONE) {
                return Access.NONE;
            }
            if (tableAccess.compareTo(access) < 0) {
                access = tableAccess;
            }
        }
        return access;
    }

    private static Access tableAccess(ConnectContext context, LocalMetastore metastore, Database db, long tableId) {
        Locker locker = new Locker();
        if (!locker.lockTableAndCheckDbExist(db, tableId, LockType.READ)) {
            return Access.NONE;
        }
        try {
            // The check resolves the table again by name and lets a missing one through, so it counts only if that
            // name leads to the table the job loads into. The lock keeps DROP and RENAME, which hold the database
            // write lock, from changing what the name leads to before the check runs.
            Table table = metastore.getTable(db.getId(), tableId);
            if (table == null || metastore.getDb(db.getId()) != db
                    || metastore.getTable(db.getFullName(), table.getName()) != table) {
                return Access.NONE;
            }
            try {
                Authorizer.checkActionOnTableLikeObject(context, db.getFullName(), table, PrivilegeType.SELECT);
                return Access.FULL;
            } catch (AccessDeniedException e) {
                // Without SELECT, any other privilege shows the job without its rows.
            }
            try {
                Authorizer.checkAnyActionOnTableLikeObject(context, db.getFullName(), table);
                return Access.METADATA;
            } catch (AccessDeniedException e) {
                return Access.NONE;
            }
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.READ);
        }
    }

    private static boolean hasUser(ConnectContext context) {
        return context != null && context.getCurrentUserIdentity() != null;
    }

    private static boolean isRoot(ConnectContext context) {
        return UserIdentity.ROOT.equals(context.getCurrentUserIdentity());
    }
}
