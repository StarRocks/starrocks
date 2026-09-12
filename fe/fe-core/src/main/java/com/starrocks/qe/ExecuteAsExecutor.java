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
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.UserRef;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.HashSet;
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
        UserRef user = stmt.getToUser();
        UserIdentity registeredIdentity = new UserIdentity(user.getUser(), user.getHost(), user.isDomain());

        if (ctx.getGlobalStateMgr().getAuthenticationMgr().doesUserExist(registeredIdentity)) {
            // A registered user keeps the existing behaviour verbatim, including the fact that a failed group
            // refresh is swallowed: those users own privileges of their own, and making a flaky group
            // provider start failing existing EXECUTE AS would be a regression.
            LOG.info("{} EXEC AS {} from now on", ctx.getCurrentUserIdentity(), user);
            ctx.setCurrentUserIdentity(registeredIdentity);
            refreshGroupsAndRoles(ctx, registeredIdentity);

            UserProperty userProperty = ctx.getGlobalStateMgr().getAuthenticationMgr()
                    .getUserProperty(user.getUser());
            ctx.updateByUserProperty(userProperty);

            //Execute As not affect session variables, so we need to reset the session variables
            SetStmt setStmt = ctx.getModifiedSessionVariables();
            if (setStmt != null) {
                SetExecutor executor = new SetExecutor(ctx, setStmt);
                executor.execute();
            }
            return;
        }

        // The target has no account. Everything is resolved into locals and applied in one go below, so a
        // refusal or a failure anywhere leaves the session exactly as it was - rather than running as the
        // target while still carrying the impersonator's groups and roles.
        UserIdentity userIdentity = UserIdentity.createEphemeralUserIdent(user.getUser(), user.getHost());
        Set<String> groups = admitExternalUserOrFail(user, ctx);
        requireImpersonateOnEphemeralOrFail(userIdentity, user, ctx);
        // Mutable on purpose: AuthorizationMgr.loadPrivilegeCollection's ephemeral branch aliases the
        // context's role id set (validRoleIds = roleIdsSpecified) and then addAll()s group roles into it.
        Set<Long> roleIds = new HashSet<>();
        for (String group : groups) {
            roleIds.addAll(ctx.getGlobalStateMgr().getAuthorizationMgr().getRoleIdListByGroup(group));
        }

        // Worth naming "external" in the log: it is the one case where EXEC AS grants privileges nobody
        // granted to a user nobody created, so an auditor needs to be able to find these lines.
        //
        // The groups are named here, unlike in the refusal above, because here they ARE the privileges:
        // every one of them is fed to getRoleIdListByGroup() just below and to Ranger through
        // ctx.setGroups(), so none of them is an idle piece of directory trivia - and a group that maps to
        // nothing today maps to something the day a policy is added. AuditEvent carries only User and
        // AuthorizedUser (no group field), so this line is the only record of what that session's
        // privileges rested on. Reducing it to a count would leave that question unanswerable.
        LOG.info("{} EXEC AS {} (external) from now on, groups {}", ctx.getCurrentUserIdentity(), user, groups);
        ctx.setCurrentUserIdentity(userIdentity);
        ctx.setGroups(groups);
        ctx.setCurrentRoleIds(roleIds);
    }

    /**
     * Re-asks the IMPERSONATE question against the identity this branch is actually going to run as.
     *
     * <p>AuthorizerStmtVisitor.visitExecuteAsStatement chose which identity to check by asking
     * AuthenticationMgr whether the target is registered, and the branch above asked the same question
     * again. A `DROP USER` can land between those two questions, and then the check that passed was made
     * against a NAMED identity while the session is about to become an EPHEMERAL one - so a grant naming
     * the user would end up authorizing an accountless namesake. UserPEntryObject.match() refuses exactly
     * that for a grant which outlives its user; this is the same rule for a user dropped mid-statement.
     *
     * <p>Only the external branch needs it. The registered branch is unchanged, and the opposite
     * race is harmless on its own terms: a check that passed against an ephemeral identity means the caller
     * holds IMPERSONATE ON ALL USERS, which covers a named identity too.
     *
     * <p>When nothing changed - every real call - this is the identical call the authorizer just made, same
     * context and same identity, so it re-reads the merged privilege collection the first one populated.
     * Under Ranger it is inert either way: RangerStarRocksAccessController builds its resource from
     * impersonateUser.getUser(), and that string is the same for both identities.
     *
     * <p>Runs after admission rather than before it, so that a refusal stays attributable to the thing that
     * refused: every other rejection on this path is an admission decision, and this one is not.
     */
    private static void requireImpersonateOnEphemeralOrFail(UserIdentity ephemeral, UserRef user, ConnectContext ctx)
            throws DdlException {
        try {
            Authorizer.checkUserAction(ctx, ephemeral, PrivilegeType.IMPERSONATE);
        } catch (AccessDeniedException e) {
            // `cannot find user` is not a euphemism here - the account really did just disappear.
            LOG.warn("EXECUTE AS denied for {}: {} lost its account between the IMPERSONATE check and the " +
                            "switch, and impersonating a name with no account needs IMPERSONATE ON ALL USERS",
                    ctx.getCurrentUserIdentity(), user);
            throw cannotFindUser(user);
        }
    }

    /**
     * Decides whether a target that has no StarRocks account may be impersonated, and returns the groups
     * its session will be authorized with.
     *
     * <p>This runs here rather than in the analyzer, and that placement is the point. The caller has
     * already been shown to hold IMPERSONATE by the time the executor is reached, so answering "is this
     * person in an allowed group?" no longer tells an unprivileged caller anything: to them every target
     * looks the same. And the groups are resolved immediately before they are applied, so the set that
     * admitted the target is the very set the session runs with.
     *
     * @return the target's groups, never null
     * @throws DdlException with the analyzer's own wording, so a refusal here is indistinguishable from
     *                      analyzer's `cannot find user` when the feature is off
     */
    private static Set<String> admitExternalUserOrFail(UserRef user, ConnectContext ctx) throws DdlException {
        String[] allowedGroups = Config.execute_as_external_user_allowed_groups;
        if (allowedGroups == null || allowedGroups.length == 0) {
            // Feature off. Unreachable in practice - the analyzer already rejected - but the executor must
            // not depend on that to stay closed.
            throw cannotFindUser(user);
        }

        if (user.isDomain() || !"%".equals(user.getHost())) {
            // 'u'@['domain'] only means something for a registered user. An explicit host is refused for a
            // different reason: RangerStarRocksAccessRequest uses the identity's host as the request's
            // client IP, so allowing one here would let the text of a SQL statement choose the value that
            // Ranger policies match on and audit records show.
            LOG.warn("EXECUTE AS denied for {}: {} has no account, and an unregistered target may only be " +
                    "named as 'user' or 'user'@'%'", ctx.getCurrentUserIdentity(), user);
            throw cannotFindUser(user);
        }

        List<String> groupProviderList = getGroupProviderListWithoutFallback(ctx);
        if (groupProviderList == null) {
            // Could not consult the directory this session is supposed to trust. Falling back to the
            // default provider would admit the target on the word of a directory nobody pointed at.
            LOG.warn("EXECUTE AS denied for {}: security integration '{}' names group providers that could " +
                            "not be resolved, and admission must not fall back to Config.group_provider - " +
                            "it is a different directory",
                    ctx.getCurrentUserIdentity(), ctx.getSecurityIntegration());
            throw cannotFindUser(user);
        }

        UserIdentity ephemeral = UserIdentity.createEphemeralUserIdent(user.getUser(), user.getHost());
        Set<String> groups = AuthenticationHandler.getGroups(ephemeral, user.getUser(), groupProviderList);
        if (Arrays.stream(allowedGroups).noneMatch(groups::contains)) {
            // The FE log is the sole diagnostic channel here - the client is told nothing beyond the usual
            // wording - but it gets the group COUNT, not the membership. The count carries what a diagnosis
            // needs: zero says the directory answered with nothing (a provider or DN problem), non-zero says
            // the target is known but in none of the allowed groups (a policy problem). The names themselves
            // would only be a record of someone's directory profile, kept for the log's whole retention, on
            // an occasion where nothing was authorized on their basis. Contrast the success path, which does
            // log the groups: there they become the session's privileges, so the line is the only record of
            // why that session could do what it did.
            LOG.warn("EXECUTE AS denied for {}: {} is not a registered user, and none of its {} group(s) " +
                            "intersect execute_as_external_user_allowed_groups {}",
                    ctx.getCurrentUserIdentity(), user, groups.size(), Arrays.toString(allowedGroups));
            throw cannotFindUser(user);
        }
        return groups;
    }

    private static DdlException cannotFindUser(UserRef user) {
        return new DdlException("cannot find user " +
                new UserIdentity(user.getUser(), user.getHost(), user.isDomain()) + "!");
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
            ctx.setCurrentRoleIds(userIdentity, groups);

            LOG.info("Refreshed groups {} and roles for user {}", groups, userIdentity);
        } catch (Exception e) {
            LOG.warn("Failed to refresh groups and roles for user {}: {}", userIdentity, e.getMessage());
            // Continue execution even if group refresh fails
        }
    }

    /**
     * Get group provider list based on security integration
     * <p>Falls back to the default provider when the session's security integration cannot be resolved,
     * which is the pre-existing behaviour and is only reached from the registered-user refresh above. Admission of
     * an unregistered user must not fall back - see {@link #getGroupProviderListWithoutFallback}.
     */
    private static List<String> getGroupProviderList(ConnectContext ctx) {
        List<String> groupProviderList = getGroupProviderListWithoutFallback(ctx);
        // Fallback to default group provider
        return groupProviderList != null ? groupProviderList : List.of(Config.group_provider);
    }

    /**
     * The group providers the session's security integration names.
     *
     * <p>An unset or native integration is not a failure: {@code Config.group_provider} IS its
     * configuration. Returning null is reserved for a session that names some other integration which then
     * could not be resolved - deleted, erroring, or naming no provider.
     *
     * <p>The caller decides what that means. The registered-user refresh accepts the default provider,
     * because it always did and because those users own privileges of their own either way. The
     * admission gate for an unregistered user must not: the default provider is a different directory
     * from the one the session was told to trust, and letting it vouch would admit a user on the strength
     * of a directory nobody pointed at.
     *
     * @return null when the named security integration could not be resolved
     */
    private static List<String> getGroupProviderListWithoutFallback(ConnectContext ctx) {
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
            LOG.warn("Security integration {} names no group provider", securityIntegration);
        } catch (Exception e) {
            LOG.warn("Failed to get group provider from security integration {}: {}",
                    securityIntegration, e.getMessage());
        }

        return null;
    }
}
