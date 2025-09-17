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

package com.starrocks.qe;

import com.google.common.base.Preconditions;
import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.authentication.UserProperty;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

public class ExecuteAsExecutor {
    private static final Logger LOG = LogManager.getLogger(ExecuteAsExecutor.class);

    /**
     * Only set current user, won't reset any other context, for example, current database.
     * Because mysql client still think that this session is using old databases and will show such hint,
     * which will only confuse the user
     * <p>
     * MySQL [test_priv]> execute as test1 with no revert;
     * Query OK, 0 rows affected (0.00 sec)
     * MySQL [test_priv]> select * from test_table2;
     * ERROR 1064 (HY000): No database selected
     */
    public static void execute(ExecuteAsStmt stmt, ConnectContext ctx) throws DdlException {
        // only support WITH NO REVERT for now
        Preconditions.checkArgument(!stmt.isAllowRevert());
        LOG.info("{} EXEC AS {} from now on", ctx.getCurrentUserIdentity(), stmt.getToUser());

        UserIdentity userIdentity = stmt.getToUser();
        ctx.setCurrentUserIdentity(userIdentity);

        // Refresh groups and roles for all users based on security integration
        refreshGroupsAndRoles(ctx, userIdentity);

        if (!userIdentity.isEphemeral()) {
            UserProperty userProperty = ctx.getGlobalStateMgr().getAuthenticationMgr()
                    .getUserProperty(userIdentity.getUser());
            ctx.updateByUserProperty(userProperty);

            //Execute As not affect session variables, so we need to reset the session variables
            SetStmt setStmt = ctx.getModifiedSessionVariables();
            if (setStmt != null) {
                SetExecutor executor = new SetExecutor(ctx, setStmt);
                executor.execute();
            }
        }
    }

    /**
     * Refresh groups and roles for user based on security integration
     * This applies to all users (both external and native) to ensure proper permission refresh
     */
    private static void refreshGroupsAndRoles(ConnectContext ctx, UserIdentity userIdentity) {
        try {
            // Get group provider list based on security integration
            List<String> groupProviderList = getGroupProviderList(ctx);

            // Query groups for the user
            Set<String> groups = AuthenticationHandler.getGroups(userIdentity, userIdentity.getUser(), groupProviderList);

            // Set groups to context
            ctx.setGroups(groups);

            // Refresh current role IDs based on user + groups
            ctx.setCurrentRoleIds(userIdentity);

            LOG.info("Refreshed groups {} and roles for user {}", groups, userIdentity);
        } catch (Exception e) {
            LOG.warn("Failed to refresh groups and roles for user {}: {}", userIdentity, e.getMessage());
            // Continue execution even if group refresh fails
        }
    }

    /**
     * Get group provider list based on security integration
     */
    private static List<String> getGroupProviderList(ConnectContext ctx) {
        String securityIntegration = ctx.getSecurityIntegration();

        // If no security integration is set, use default group provider
        if (securityIntegration == null || securityIntegration.isEmpty() ||
                securityIntegration.equals("native")) {
            return List.of(Config.group_provider);
        }

        // Try to get group provider from security integration
        try {
            var authMgr = ctx.getGlobalStateMgr().getAuthenticationMgr();
            var si = authMgr.getSecurityIntegration(securityIntegration);
            if (si != null && si.getGroupProviderName() != null && !si.getGroupProviderName().isEmpty()) {
                return si.getGroupProviderName();
            }
        } catch (Exception e) {
            LOG.warn("Failed to get group provider from security integration {}: {}",
                    securityIntegration, e.getMessage());
        }

        // Fallback to default group provider
        return List.of(Config.group_provider);
    }
}
