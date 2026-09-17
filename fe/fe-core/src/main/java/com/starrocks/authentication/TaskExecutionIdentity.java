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

package com.starrocks.authentication;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TTaskExecutionIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;

/**
 * An authenticated principal that can be reconstructed for a user-submitted AI task.
 * This is deliberately not an authorization snapshot: groups, roles, grants and credentials are never persisted.
 */
public final class TaskExecutionIdentity {
    private static final Logger LOG = LogManager.getLogger(TaskExecutionIdentity.class);
    private static final int CURRENT_VERSION = 1;

    @SerializedName("version")
    private final int version;
    @SerializedName("user")
    private final String user;
    @SerializedName("host")
    private final String host;
    @SerializedName("domain")
    private final Boolean domain;
    // UserIdentity's ephemeral field is not persisted by its existing Gson representation.
    @SerializedName("ephemeral")
    private final Boolean ephemeral;
    @SerializedName("securityIntegration")
    private final String securityIntegration;
    @SerializedName("distinguishedName")
    private final String distinguishedName;

    private TaskExecutionIdentity(UserIdentity identity, String securityIntegration, String distinguishedName) {
        this.version = CURRENT_VERSION;
        this.user = identity.getUser();
        this.host = identity.getHost();
        this.domain = identity.isDomain();
        this.ephemeral = identity.isEphemeral();
        this.securityIntegration = securityIntegration;
        this.distinguishedName = distinguishedName;
    }

    static void recordAuthentication(ConnectContext context) {
        context.setAuthenticatedTaskIdentity(new TaskExecutionIdentity(context.getCurrentUserIdentity(),
                context.getSecurityIntegration(), context.getDistinguishedName()));
    }

    public static TaskExecutionIdentity capture(ConnectContext context) {
        TaskExecutionIdentity identity = context.getAuthenticatedTaskIdentity();
        if (identity == null || !identity.matches(context)) {
            throw new SemanticException("AI tasks require an authenticated creator identity; "
                    + "reconnect before submitting the task (EXECUTE AS is not supported); "
                    + "for forwarded requests, verify that all Frontends are upgraded");
        }
        identity.validate();
        return identity;
    }

    public boolean matches(ConnectContext context) {
        return matchesPrincipal(context)
                && Objects.equals(securityIntegration, context.getSecurityIntegration())
                && Objects.equals(distinguishedName, context.getDistinguishedName());
    }

    private boolean matchesPrincipal(ConnectContext context) {
        UserIdentity current = context.getCurrentUserIdentity();
        return current != null && domain != null && ephemeral != null
                && Objects.equals(user, current.getUser()) && Objects.equals(host, current.getHost())
                && domain == current.isDomain() && ephemeral == current.isEphemeral()
                && Objects.equals(user, context.getQualifiedUser());
    }

    public TTaskExecutionIdentity toThrift() {
        validate();
        return new TTaskExecutionIdentity().setVersion(version).setUser(user).setHost(host)
                .setIs_domain(domain).setIs_ephemeral(ephemeral).setSecurity_integration(securityIntegration)
                .setDistinguished_name(distinguishedName);
    }

    /** Install provenance only for a complete, matching identity received from another trusted FE. */
    public static void restoreForwarded(ConnectContext context, TTaskExecutionIdentity forwarded) {
        context.setAuthenticatedTaskIdentity(null);
        if (forwarded == null) {
            return;
        }
        if (!forwarded.isSetVersion() || !forwarded.isSetUser() || !forwarded.isSetHost() || !forwarded.isSetIs_domain()
                || !forwarded.isSetIs_ephemeral() || !forwarded.isSetSecurity_integration()
                || !forwarded.isSetDistinguished_name()) {
            LOG.warn("Ignoring forwarded AI task identity: incomplete envelope");
            return;
        }
        if (forwarded.getVersion() != CURRENT_VERSION) {
            LOG.warn("Ignoring forwarded AI task identity: unsupported version");
            return;
        }
        TaskExecutionIdentity identity = new TaskExecutionIdentity(
                new UserIdentity(forwarded.getUser(), forwarded.getHost(), forwarded.isIs_domain(),
                        forwarded.isIs_ephemeral()), forwarded.getSecurity_integration(), forwarded.getDistinguished_name());
        try {
            identity.validate();
        } catch (SemanticException e) {
            LOG.warn("Ignoring forwarded AI task identity: invalid attributes");
            return;
        }
        if (!identity.matchesPrincipal(context)) {
            LOG.warn("Ignoring forwarded AI task identity: principal mismatch");
            return;
        }
        context.setSecurityIntegration(identity.securityIntegration);
        context.setDistinguishedName(identity.distinguishedName);
        context.setAuthenticatedTaskIdentity(identity);
    }

    private void validate() {
        if (version != CURRENT_VERSION) {
            throw new SemanticException("Unsupported task execution identity version; recreate the AI task");
        }
        if (user == null || user.isEmpty() || host == null || host.isEmpty() || domain == null || ephemeral == null
                || securityIntegration == null || securityIntegration.isEmpty()
                || distinguishedName == null || distinguishedName.isEmpty()) {
            throw new SemanticException("Task execution identity is incomplete; recreate the AI task");
        }
        // The native authentication mechanism applies if and only if the principal is non-ephemeral.
        if (ephemeral == SecurityIntegration.AUTHENTICATION_CHAIN_MECHANISM_NATIVE.equals(securityIntegration)) {
            throw new SemanticException("Task execution identity has an inconsistent authentication mechanism");
        }
    }

    public void restore(ConnectContext context) {
        validate();
        UserIdentity identity = new UserIdentity(user, host, domain, ephemeral);
        AuthenticationMgr authenticationMgr = context.getGlobalStateMgr().getAuthenticationMgr();
        List<String> groupProviders = List.of(Config.group_provider);
        List<String> permittedGroups = List.of();
        AuthenticationProvider authenticationProvider = null;
        boolean legacyPerUserDN = false;
        if (identity.isEphemeral()) {
            SecurityIntegration integration = authenticationMgr.getSecurityIntegration(securityIntegration);
            if (integration == null || !Arrays.asList(Config.authentication_chain).contains(securityIntegration)) {
                throw new SemanticException("AI task security integration no longer exists or is disabled");
            }
            if (!integration.getGroupProviderName().isEmpty()) {
                groupProviders = integration.getGroupProviderName();
            }
            permittedGroups = integration.getGroupAllowedLoginList();
            try {
                authenticationProvider = integration.getAuthenticationProvider();
            } catch (AuthenticationException | RuntimeException e) {
                throw new SemanticException("Cannot resolve AI task authentication provider");
            }
            if (authenticationProvider == null) {
                throw new SemanticException("AI task authentication provider no longer exists");
            }
        } else {
            UserAuthenticationInfo info = authenticationMgr.getUserAuthenticationInfoByUserIdentity(identity);
            if (info == null) {
                throw new SemanticException("AI task creator no longer exists; recreate the task with an authenticated user");
            }
            if (info.isLdapAuthPlugin()) {
                authenticationProvider = AuthPlugin.Server.AUTHENTICATION_LDAP_SIMPLE.getProvider(info.getAuthString());
                // Login deliberately excludes memberOf for the legacy CREATE USER ... AS '<dn>' form.
                legacyPerUserDN = !Strings.isNullOrEmpty(info.getAuthString());
            }
            // An ALTER USER must not leave a task resolving groups for a different directory principal.
            boolean samePrincipal = info.isLdapAuthPlugin()
                    ? !legacyPerUserDN || isSameLDAPPrincipal(info.getAuthString())
                    : distinguishedName.equals(user);
            if (!samePrincipal) {
                throw new SemanticException("AI task creator authentication identity has changed; recreate the task");
            }
        }
        LDAPAuthProvider ldapProvider = authenticationProvider instanceof LDAPAuthProvider
                ? (LDAPAuthProvider) authenticationProvider : null;
        Set<String> groups = new HashSet<>();
        if (ldapProvider != null && ldapProvider.isMemberOfUsed() && !legacyPerUserDN) {
            groups.addAll(resolveMemberOfGroups(ldapProvider));
        }
        if (ldapProvider == null || ldapProvider.isGroupProviderUsed()) {
            for (String providerName : groupProviders) {
                GroupProvider provider = authenticationMgr.getGroupProvider(providerName);
                if (provider == null) {
                    throw new SemanticException("AI task group provider no longer exists");
                }
                try {
                    groups.addAll(provider.getGroup(identity, distinguishedName));
                } catch (RuntimeException e) {
                    // A provider exception may contain the DN; do not expose it through task errors or logs.
                    throw new SemanticException("Cannot resolve AI task creator groups");
                }
            }
        }
        if (!permittedGroups.isEmpty()) {
            Set<String> allowedLowerCase = LDAPMemberOfExtractor.toLowerCaseSet(permittedGroups);
            if (groups.stream().noneMatch(group -> group != null && allowedLowerCase.contains(group.toLowerCase(Locale.ROOT)))) {
                throw new SemanticException("AI task creator no longer belongs to a permitted security integration group");
            }
        }
        context.setCurrentUserIdentity(identity);
        context.setQualifiedUser(user);
        context.setSecurityIntegration(securityIntegration);
        context.setDistinguishedName(distinguishedName);
        context.setGroups(groups);
        try {
            Set<Long> roles = identity.isEphemeral() ? new HashSet<>() :
                    new HashSet<>(context.getGlobalStateMgr().getAuthorizationMgr().getRoleIdsByUser(identity));
            for (String group : groups) {
                roles.addAll(context.getGlobalStateMgr().getAuthorizationMgr().getRoleIdListByGroup(group));
            }
            context.setCurrentRoleIds(roles);
        } catch (PrivilegeException e) {
            throw new SemanticException("Cannot restore AI task creator roles; recreate the task with an authenticated user");
        }
        context.setAuthenticatedTaskIdentity(this);
    }

    private Set<String> resolveMemberOfGroups(LDAPAuthProvider provider) {
        LDAPAuthProvider.TargetUserGroups current;
        boolean samePrincipal;
        try {
            current = provider.resolveMemberOfGroupsForUser(user);
            samePrincipal = isSameLDAPPrincipal(current.distinguishedName());
        } catch (RuntimeException e) {
            throw new SemanticException("Cannot refresh AI task creator LDAP groups");
        }
        // Unlike EXECUTE AS, a task cannot silently discard a required source when the service
        // account cannot resolve it, or switch to a different directory entry with the same username.
        if (!samePrincipal) {
            throw new SemanticException("Cannot refresh AI task creator LDAP groups; "
                    + "verify the LDAP service account and the creator's current identity");
        }
        return current.memberOfGroups();
    }

    private boolean isSameLDAPPrincipal(String currentDN) {
        if (currentDN == null) {
            return false;
        }
        try {
            return new LdapName(distinguishedName).equals(new LdapName(currentDN));
        } catch (InvalidNameException e) {
            return false;
        }
    }
}
