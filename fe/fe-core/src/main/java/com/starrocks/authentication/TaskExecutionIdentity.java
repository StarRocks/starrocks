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

import com.google.gson.annotations.SerializedName;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TTaskExecutionIdentity;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * An authenticated principal that can be reconstructed for a user-submitted AI task.
 * This is deliberately not an authorization snapshot: groups, roles, grants and credentials are never persisted.
 */
public final class TaskExecutionIdentity {
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
                    + "reconnect before submitting the task (EXECUTE AS is not supported)");
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
        if (forwarded == null || !forwarded.isSetVersion() || forwarded.getVersion() != CURRENT_VERSION
                || !forwarded.isSetUser() || !forwarded.isSetHost() || !forwarded.isSetIs_domain()
                || !forwarded.isSetIs_ephemeral() || !forwarded.isSetSecurity_integration()
                || !forwarded.isSetDistinguished_name()) {
            return;
        }
        TaskExecutionIdentity identity = new TaskExecutionIdentity(
                new UserIdentity(forwarded.getUser(), forwarded.getHost(), forwarded.isIs_domain(),
                        forwarded.isIs_ephemeral()), forwarded.getSecurity_integration(), forwarded.getDistinguished_name());
        try {
            identity.validate();
        } catch (SemanticException e) {
            return;
        }
        if (!identity.matchesPrincipal(context)) {
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
        if (ephemeral == SecurityIntegration.AUTHENTICATION_CHAIN_MECHANISM_NATIVE.equals(securityIntegration)) {
            throw new SemanticException("Task execution identity has an inconsistent authentication mechanism");
        }
    }

    public void restore(ConnectContext context) {
        validate();
        UserIdentity identity = new UserIdentity(user, host, domain, ephemeral);
        AuthenticationMgr authenticationMgr = context.getGlobalStateMgr().getAuthenticationMgr();
        if (!identity.isEphemeral() && !authenticationMgr.doesUserExist(identity)) {
            throw new SemanticException("AI task creator no longer exists; recreate the task with an authenticated user");
        }
        List<String> groupProviders;
        List<String> permittedGroups = List.of();
        if (identity.isEphemeral()) {
            SecurityIntegration integration = authenticationMgr.getSecurityIntegration(securityIntegration);
            if (integration == null || !Arrays.asList(Config.authentication_chain).contains(securityIntegration)) {
                throw new SemanticException("AI task security integration no longer exists or is disabled");
            }
            groupProviders = integration.getGroupProviderName();
            permittedGroups = integration.getGroupAllowedLoginList();
        } else {
            groupProviders = List.of(Config.group_provider);
        }
        Set<String> groups = new HashSet<>();
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
        if (!permittedGroups.isEmpty() && permittedGroups.stream().noneMatch(groups::contains)) {
            throw new SemanticException("AI task creator no longer belongs to a permitted security integration group");
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
}
