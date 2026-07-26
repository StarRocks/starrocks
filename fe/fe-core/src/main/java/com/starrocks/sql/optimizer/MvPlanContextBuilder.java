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

package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class MvPlanContextBuilder {
    private static final Logger LOG = LogManager.getLogger(MvPlanContextBuilder.class);

    /**
     * Get plan context for the given materialized view.
     *
     * @param mv
     * @param isThrowException: whether to throw exception when failed to build plan context:
     *                          - when in altering table, we want to throw exception to fail the alter table operation
     *                          - when in generating mv plan, we want to ignore the exception and continue the query
     */
    public static List<MvPlanContext> getPlanContext(MaterializedView mv,
                                                     boolean isThrowException) {
        // build mv query logical plan
        MaterializedViewOptimizer mvOptimizer = new MaterializedViewOptimizer();

        // If the caller is not from query (eg. background schema change thread), set thread local info to avoid
        // NPE in the planning.
        List<MvPlanContext> results = Lists.newArrayList();
        ConnectContext.ContextScope scope = ConnectContext.enterOnlyReadIcebergCacheScope(ConnectContext.get());
        ConnectContext connectContext = scope.getContext();
        if (connectContext.getCurrentUserIdentity() == null) {
            switchMvMaintenanceUser(connectContext, mv);
        }

        try (scope; var guard = connectContext.bindScope()) {
            Optional.ofNullable(doGetOptimizePlan(() -> mvOptimizer.optimize(mv, connectContext), isThrowException))
                    .map(results::add);

            if (mv.getBaseTables().stream().anyMatch(table -> table.isView())) {
                Optional.ofNullable(doGetOptimizePlan(() -> mvOptimizer.optimize(mv, connectContext, false, true),
                                isThrowException))
                        .map(results::add);
            }
        }
        return results;
    }

    private static MvPlanContext doGetOptimizePlan(Supplier<MvPlanContext> supplier,
                                                   boolean isThrowException) {
        try {
            return supplier.get();
        } catch (Exception e) {
            // ignore
            LOG.warn("Failed to build mv plan context", e);
            if (isThrowException) {
                throw e;
            }
        }
        return null;
    }

    /**
     * When MV plan building runs on a background maintenance thread (the mv-plan-cache pool), its
     * ConnectContext has no current user, so analyzing a UDF-referencing MV would fail its
     * authorization check with an NPE. Give the context a valid identity, mirroring
     * {@code TaskRun.switchUser} so plan-cache building authorizes the MV the same way MV refresh
     * does, honoring {@link Config#mv_use_creator_based_authorization}.
     */
    private static void switchMvMaintenanceUser(ConnectContext connectContext, MaterializedView mv) {
        UserIdentity user = resolveMvMaintenanceUser(mv);
        connectContext.setQualifiedUser(user.getUser());
        connectContext.setCurrentUserIdentity(user);
        if (UserIdentity.ROOT.equals(user)) {
            connectContext.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        } else {
            try {
                // Activate all the creator's roles (not just default roles), mirroring TaskRun.switchUser:
                // unattended MV maintenance can't SET ROLE, so a privilege granted via a non-default role would
                // otherwise be missing and the UDF authorization would fail (rewrite silently broken).
                connectContext.setCurrentRoleIds(
                        GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdsByUser(user));
            } catch (PrivilegeException e) {
                LOG.warn("MV {} plan build: failed to resolve creator roles; using default roles",
                        mv.getName(), e);
                connectContext.setCurrentRoleIds(user);
            }
        }
    }

    private static UserIdentity resolveMvMaintenanceUser(MaterializedView mv) {
        if (!Config.mv_use_creator_based_authorization) {
            return UserIdentity.ROOT;
        }
        UserIdentity creator = creatorOfRefreshTask(mv);
        if (creator != null) {
            return creator;
        }
        LOG.warn("MV {} plan build: creator-based authorization is enabled but the refresh task/creator " +
                "could not be resolved (task not registered yet?); falling back to ROOT", mv.getName());
        return UserIdentity.ROOT;
    }

    private static UserIdentity creatorOfRefreshTask(MaterializedView mv) {
        Task task = GlobalStateMgr.getCurrentState().getTaskManager()
                .getTask(TaskBuilder.getMvTaskName(mv.getId()));
        if (task == null) {
            return null;
        }
        if (task.getUserIdentity() != null) {
            return task.getUserIdentity();
        }
        if (task.getCreateUser() != null) {
            return UserIdentity.createAnalyzedUserIdentWithIp(task.getCreateUser(), "%");
        }
        return null;
    }
}
