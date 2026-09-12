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
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TUserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class UserIdentityUtils {
    private static final Logger LOG = LogManager.getLogger(UserIdentityUtils.class);

    public static UserIdentity fromString(String userIdentStr) {
        if (Strings.isNullOrEmpty(userIdentStr)) {
            return null;
        }

        String[] parts = userIdentStr.split("@");
        if (parts.length != 2) {
            return null;
        }

        String user = parts[0];
        if (!user.startsWith("'") || !user.endsWith("'")) {
            return null;
        }

        String host = parts[1];
        if (host.startsWith("['") && host.endsWith("']")) {
            return new UserIdentity(user.substring(1, user.length() - 1),
                    host.substring(2, host.length() - 2), true);
        } else if (host.startsWith("'") && host.endsWith("'")) {
            return new UserIdentity(user.substring(1, user.length() - 1),
                    host.substring(1, host.length() - 1));
        }

        return null;
    }

    public static TUserIdentity toThrift(UserIdentity userIdentity) {
        TUserIdentity tUserIdent = new TUserIdentity();
        tUserIdent.setHost(userIdentity.getHost());
        tUserIdent.setUsername(userIdentity.getUser());
        tUserIdent.setIs_domain(userIdentity.isDomain());
        tUserIdent.setIs_ephemeral(userIdentity.isEphemeral());
        return tUserIdent;
    }

    public static UserIdentity fromThrift(TUserIdentity tUserIdent) {
        return new UserIdentity(tUserIdent.getUsername(), tUserIdent.getHost(), tUserIdent.is_domain,
                tUserIdent.is_ephemeral);
    }

    public static void setAuthInfoFromThrift(ConnectContext context, TAuthInfo authInfo) {
        if (authInfo.isSetCurrent_user_ident()) {
            setAuthInfoFromThrift(context, authInfo.getCurrent_user_ident());
        } else {
            setAuthInfoFromThrift(context, authInfo.user, authInfo.user_ip);
        }
    }

    /**
     * Fallback for Thrift requests that carry a bare user name and IP instead of a full identity.
     *
     * <p>Several request types (TGetTablesParams, TGetTasksParams, ...) declare the same
     * {@code user}/{@code user_ip}/{@code current_user_ident} triple without sharing a Thrift struct, so
     * each of their handlers inlined this block. Sharing it here keeps them from drifting apart - notably
     * over the group resolution below, which every one of them needs.
     */
    public static void setAuthInfoFromThrift(ConnectContext context, String user, String userIp) {
        UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp(user, userIp);
        context.setCurrentUserIdentity(userIdentity);
        context.setCurrentRoleIds(userIdentity);
        setGroupsIfAbsent(context, userIdentity);
    }

    public static void setAuthInfoFromThrift(ConnectContext context, TUserIdentity tUserIdent) {
        UserIdentity userIdentity = UserIdentityUtils.fromThrift(tUserIdent);
        context.setCurrentUserIdentity(userIdentity);
        if (tUserIdent.isSetCurrent_role_ids()) {
            List<Long> roleIdList = tUserIdent.current_role_ids.getRole_id_list();
            Set<Long> requestedRoleIds = roleIdList != null ? new HashSet<>(roleIdList) : new HashSet<>();
            try {
                Set<Long> actualRoleIds =
                        GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdsByUser(userIdentity);
                requestedRoleIds.retainAll(actualRoleIds);
            } catch (PrivilegeException e) {
                LOG.warn("Failed to validate role IDs for user {}: {}", userIdentity, e.getMessage());
            }
            context.setCurrentRoleIds(requestedRoleIds);
        } else {
            context.setCurrentRoleIds(userIdentity);
        }
        setGroupsIfAbsent(context, userIdentity);
    }

    /**
     * Resolve the caller's groups into a context that was rebuilt from a Thrift request.
     *
     * <p>TAuthInfo/TUserIdentity carry the identity and the role ids but no group membership, so a context
     * built from them starts with an empty group set. The handlers that serve {@code information_schema}
     * and the {@code sys} tables construct one per request and hand it to
     * {@link com.starrocks.sql.analyzer.Authorizer}, which passes {@code context.getGroups()} straight to
     * the external access controller - Ranger then receives an empty group list and no policy item whose
     * subject is a group can match. The visible effect is that a user who holds access purely through a
     * group sees those tables as empty while {@code SHOW TABLES} and {@code SELECT} on the very same
     * objects succeed, because those run on the session context where the groups were resolved at login.
     *
     * <p>The groups are re-resolved here rather than carried over Thrift on purpose. Role ids can be
     * shipped because the receiver can check them against the ones the user actually holds - which is what
     * the {@code retainAll} above does. Group membership has no such check available: the only authority
     * on it is the group provider, so a group list arriving over Thrift would have to be trusted as sent,
     * and anything able to shape that request could claim membership and pick up whatever a policy grants
     * it. Asking the providers keeps the answer coming from the same place the login path got it from, and
     * it is a cache read there rather than directory traffic.
     *
     * <p>Only fills an empty set. Some callers pass a live session context that already carries the groups
     * resolved at login - possibly through a security integration's own provider, which this path cannot
     * see - and those must not be overwritten with a global-provider answer.
     *
     * <p>A failure here leaves the context with no groups, which denies rather than grants, so it is
     * logged and swallowed instead of failing the metadata scan outright. This mirrors
     * {@code ExecuteAsExecutor.refreshGroupsAndRoles()}.
     */
    private static void setGroupsIfAbsent(ConnectContext context, UserIdentity userIdentity) {
        if (!context.getGroups().isEmpty()) {
            return;
        }

        try {
            Set<String> groups = AuthenticationHandler.getGroups(
                    userIdentity, groupProviderLookupKey(userIdentity), List.of(Config.group_provider));
            if (!groups.isEmpty()) {
                context.setGroups(groups);
            }
        } catch (Exception e) {
            LOG.warn("Failed to resolve groups for user {}: {}", userIdentity, e.getMessage());
        }
    }

    /**
     * The key the group providers look a user up by. {@link AuthenticationHandler} passes the session's
     * distinguished name in this position at login; this path has none to pass.
     *
     * <p>The DN is produced by the LDAP bind and only the authenticating session holds it - neither
     * TAuthInfo nor TUserIdentity carries it, and re-deriving it would mean a directory search per
     * metadata request, which is exactly the cost this fix sets out to avoid. The user name is what
     * remains, and it is the right key for every provider that caches under it: {@code file} and
     * {@code unix} always do, and {@code ldap} does when {@code ldap_user_search_attr} is set, which
     * makes {@link LDAPGroupProvider} cache each member under the value that attribute pulls out of the
     * member DN - the same string the user logs in with.
     *
     * <p>An {@code ldap} provider without that property caches under the full member DN instead and will
     * not match a user name. Such a configuration resolves to no groups here, which denies rather than
     * grants, and leaves those tables looking the way they do today.
     */
    private static String groupProviderLookupKey(UserIdentity userIdentity) {
        return userIdentity.getUser();
    }
}
