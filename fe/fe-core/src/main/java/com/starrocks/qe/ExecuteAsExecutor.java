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
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.LDAPAuthProvider;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authentication.SimpleLDAPSecurityIntegration;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.authentication.UserProperty;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.UserRef;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        // The session's SI/DN describe the authenticated principal, not necessarily the impersonated principal.
        // Preserve synchronous EXECUTE AS semantics, but do not allow that metadata to own a new AI task.
        ctx.setAuthenticatedTaskIdentity(null);
        LOG.info("{} EXEC AS {} from now on", ctx.getCurrentUserIdentity(), stmt.getToUser());

        UserRef user = stmt.getToUser();
        // Create UserIdentity with ephemeral flag for external users
        UserIdentity userIdentity;
        if (user.isExternal()) {
            userIdentity = UserIdentity.createEphemeralUserIdent(user.getUser(), user.getHost());
        } else {
            userIdentity = new UserIdentity(user.getUser(), user.getHost(), user.isDomain());
        }
        ctx.setCurrentUserIdentity(userIdentity);

        // Refresh groups and roles for all users based on security integration
        refreshGroupsAndRoles(ctx, userIdentity);

        if (!userIdentity.isEphemeral()) {
            UserProperty userProperty = ctx.getGlobalStateMgr().getAuthenticationMgr()
                    .getUserProperty(user.getUser());
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
     * Recompute the group set and the roles for the identity we just switched to.
     * <p>
     * Impersonation never authenticates the target - there is no credential for it, the gate is the
     * IMPERSONATE privilege on the impersonator - so this is the authorization half of a login only:
     * resolve the target's groups, then derive its roles. Resolving groups needs no credential, so
     * everything a login does here can be done for a target as well.
     * <p>
     * The sources are decided by the **target**, not by the session: the impersonator may have
     * logged in through a different security integration, or natively, and its configuration says
     * nothing about how the target's groups are resolved.
     */
    private static void refreshGroupsAndRoles(ConnectContext ctx, UserIdentity userIdentity) {
        try {
            // Drop the memberOf groups of the original user before anything else. They were read from
            // that user's own LDAP entry at login and say nothing about the target identity. The
            // replacement below would already hide them, but clearing keeps the guarantee from
            // resting on "no one happens to read the field".
            ctx.getAccessControlContext().setMemberOfGroups(Set.of());

            ResolvedTarget target = resolveTarget(ctx, userIdentity);

            Set<String> groups = new HashSet<>();
            // `memberof` means the group providers are ignored - the same rule as on the login path.
            if (target.ldapProvider == null || target.ldapProvider.isGroupProviderUsed()) {
                // Hand the group providers the real DN when resolving the target produced one: a
                // provider without `ldap_user_search_attr` keys its cache by DN, and a login name
                // would simply never match there.
                String distinguishedName = target.distinguishedName == null
                        ? userIdentity.getUser() : target.distinguishedName;
                groups.addAll(AuthenticationHandler.resolveGroupsFromProviders(
                        userIdentity, distinguishedName, target.groupProviderList));
            }
            groups.addAll(target.memberOfGroups);

            // Set groups to context
            ctx.setGroups(groups);

            // Refresh current role IDs based on user + groups
            ctx.setCurrentRoleIds(userIdentity, groups);

            LOG.info("Refreshed groups {} and roles for user {}", groups, userIdentity);
        } catch (Exception e) {
            // The identity was already switched by the caller, so retaining the previous group set
            // would let the session keep the impersonator's groups and roles under the target's
            // name. Clear instead: a resolution failure may only ever lose privileges.
            LOG.warn("Failed to refresh groups and roles for user {}, dropping groups and roles: {}",
                    userIdentity, e.getMessage());
            ctx.setGroups(Set.of());
            ctx.setCurrentRoleIds(userIdentity, Set.of());
        }
    }

    /**
     * Everything that was worked out about the target: which group providers to ask, the LDAP
     * provider that answered (null when the target has no LDAP identity), and what its own entry
     * said - its DN and its groups.
     */
    private static class ResolvedTarget {
        private final List<String> groupProviderList;
        private final LDAPAuthProvider ldapProvider;
        private final String distinguishedName;
        private final Set<String> memberOfGroups;

        private ResolvedTarget(List<String> groupProviderList, LDAPAuthProvider ldapProvider,
                               LDAPAuthProvider.TargetUserGroups fromOwnEntry) {
            this.groupProviderList = groupProviderList;
            this.ldapProvider = ldapProvider;
            this.distinguishedName = fromOwnEntry == null ? null : fromOwnEntry.distinguishedName();
            this.memberOfGroups = fromOwnEntry == null ? Set.of() : fromOwnEntry.memberOfGroups();
        }
    }

    /**
     * Work out where the target's groups come from. `memberOf` is read using the **target's** own
     * configuration, not the session's: the impersonator may have logged in through a different
     * security integration, or natively, and its configuration says nothing about the target's own
     * LDAP entry. The group providers keep their pre-existing default, the session's - see
     * {@link #getGroupProviderList}.
     * <ul>
     *     <li>A user that exists in the catalog carries its own authentication method, so the choice
     *     is exact: a native-password user has no LDAP identity and only gets the group providers;
     *     an `authentication_ldap_simple` user gets a provider built from the FE configuration, the
     *     same one its own login would use.</li>
     *     <li>An external user (`EXECUTE AS EXTERNAL USER`) has no catalog entry, so there is nothing
     *     to look its method up in - which is why its groups already came from a security integration
     *     before this change. See {@link #resolveExternalTarget}.</li>
     * </ul>
     */
    private static ResolvedTarget resolveTarget(ConnectContext ctx, UserIdentity userIdentity) {
        AuthenticationMgr authMgr = ctx.getGlobalStateMgr().getAuthenticationMgr();
        List<String> defaultGroupProviders = getGroupProviderList(ctx);

        if (userIdentity.isEphemeral()) {
            return resolveExternalTarget(authMgr, userIdentity, defaultGroupProviders);
        }

        UserAuthenticationInfo info = authMgr.getUserAuthenticationInfoByUserIdentity(userIdentity);
        String authPlugin = info == null ? null : info.getAuthPlugin();
        if (authPlugin == null
                || !authPlugin.equalsIgnoreCase(AuthPlugin.Server.AUTHENTICATION_LDAP_SIMPLE.name())) {
            // Native or any other authentication method: no LDAP identity to read an attribute from.
            return new ResolvedTarget(defaultGroupProviders, null, null);
        }
        // A pre-created LDAP user reads the FE configuration, exactly as at login. When it was
        // created with AS '<dn>' the provider refuses memberOf on its own (legacy form).
        LDAPAuthProvider provider =
                asLdapProvider(AuthPlugin.Server.AUTHENTICATION_LDAP_SIMPLE.getProvider(info.getAuthString()));
        return new ResolvedTarget(defaultGroupProviders, provider,
                provider == null ? null : provider.resolveMemberOfGroupsForUser(userIdentity.getUser()));
    }

    /**
     * For an external target, walk `authentication_chain` in its configured order and take the first
     * integration that can actually resolve the user's own entry.
     * <p>
     * "The first LDAP integration in the chain" is not good enough: a chain often carries more than
     * one - a leftover from an earlier setup, or two directories side by side - and the first one may
     * be configured not to read memberOf at all, or may not know this user. Trying them in order is
     * also what the login path does with the same chain, so the two agree.
     * <p>
     * The loop is cheap: an integration whose group source does not read memberOf answers without
     * sending anything to a directory, so only integrations that could actually answer cost a
     * request. If none resolves the user, only the group providers are left, and they stay the
     * session's - exactly what happened before this method existed.
     */
    private static ResolvedTarget resolveExternalTarget(AuthenticationMgr authMgr, UserIdentity userIdentity,
                                                        List<String> defaultGroupProviders) {
        for (String name : Config.authentication_chain) {
            if (name == null || name.isEmpty()
                    || name.equals(SecurityIntegration.AUTHENTICATION_CHAIN_MECHANISM_NATIVE)) {
                continue;
            }
            SecurityIntegration si = authMgr.getSecurityIntegration(name);
            if (si == null) {
                continue;
            }
            // Only the open-source LDAP integration can answer for memberOf: the enterprise `ldap`
            // type resolves groups through its own role mapping and is out of scope here.
            if (!(si instanceof SimpleLDAPSecurityIntegration)) {
                continue;
            }
            List<String> groupProviders =
                    si.getGroupProviderName().isEmpty() ? defaultGroupProviders : si.getGroupProviderName();
            LDAPAuthProvider provider;
            try {
                provider = asLdapProvider(si.getAuthenticationProvider());
            } catch (Exception e) {
                LOG.warn("Failed to build the authentication provider of security integration {}: {}",
                        name, e.getMessage());
                continue;
            }
            if (provider == null) {
                continue;
            }

            LDAPAuthProvider.TargetUserGroups fromOwnEntry =
                    provider.resolveMemberOfGroupsForUser(userIdentity.getUser());
            if (fromOwnEntry.distinguishedName() != null) {
                // This integration found the entry - that is the one the user would authenticate
                // against, so it decides both sources.
                return new ResolvedTarget(groupProviders, provider, fromOwnEntry);
            }
        }
        return new ResolvedTarget(defaultGroupProviders, null, null);
    }

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

    private static LDAPAuthProvider asLdapProvider(AuthenticationProvider provider) {
        return provider instanceof LDAPAuthProvider ? (LDAPAuthProvider) provider : null;
    }

}
