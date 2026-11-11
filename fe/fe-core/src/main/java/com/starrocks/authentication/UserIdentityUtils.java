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
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TUserIdentity;
import com.starrocks.thrift.TUserRoles;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
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

    /**
     * The caller of a session as an RPC sends it: its identity, the roles it activated and the groups it belongs to.
     * The FE serving the RPC rebuilds the caller with {@link #setAuthInfoFromThrift(ConnectContext, TUserIdentity)}.
     */
    public static TUserIdentity callerToThrift(ConnectContext context) {
        TUserIdentity tUserIdent = toThrift(context.getCurrentUserIdentity());
        if (context.getCurrentRoleIds() != null) {
            TUserRoles userRoles = new TUserRoles();
            userRoles.setRole_id_list(new ArrayList<>(context.getCurrentRoleIds()));
            tUserIdent.setCurrent_role_ids(userRoles);
        }
        if (context.getGroups() != null && !context.getGroups().isEmpty()) {
            tUserIdent.setGroups(new ArrayList<>(context.getGroups()));
        }
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
            UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp(authInfo.user, authInfo.user_ip);
            context.setCurrentUserIdentity(userIdentity);
            context.setCurrentRoleIds(userIdentity);
        }
    }

    public static void setAuthInfoFromThrift(ConnectContext context, TUserIdentity tUserIdent) {
        UserIdentity userIdentity = UserIdentityUtils.fromThrift(tUserIdent);
        context.setCurrentUserIdentity(userIdentity);
        // The groups were resolved when the session logged in, from the directory, the token claims or a group
        // provider, and cannot be resolved again here: a token is gone after login, and a directory lookup per RPC
        // is too slow. They are taken as sent, like the user name. The privilege collection grants the roles mapped
        // to them, as it does for the session itself.
        Set<String> groups = tUserIdent.isSetGroups() ? new HashSet<>(tUserIdent.getGroups()) : new HashSet<>();
        context.setGroups(groups);
        if (tUserIdent.isSetCurrent_role_ids()) {
            List<Long> roleIdList = tUserIdent.current_role_ids.getRole_id_list();
            Set<Long> requestedRoleIds = roleIdList != null ? new HashSet<>(roleIdList) : new HashSet<>();
            if (userIdentity.isEphemeral()) {
                // An ephemeral user, one authenticated through a security integration, owns no roles to check
                // against: its privileges are the roles its session activated, as in the privilege collection.
                // The roles must not be looked up either, as that reads the session of the current thread, and
                // the RPC threads calling this have none.
                context.setCurrentRoleIds(requestedRoleIds);
                return;
            }
            try {
                // A role is kept if it is granted to the user or to one of its groups, as the session got its roles.
                AuthorizationMgr authorizationMgr = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
                Set<Long> actualRoleIds = new HashSet<>(authorizationMgr.getRoleIdsByUser(userIdentity));
                for (String group : groups) {
                    actualRoleIds.addAll(authorizationMgr.getRoleIdListByGroup(group));
                }
                requestedRoleIds.retainAll(actualRoleIds);
            } catch (PrivilegeException e) {
                LOG.warn("Failed to validate role IDs for user {}: {}", userIdentity, e.getMessage());
            }
            context.setCurrentRoleIds(requestedRoleIds);
        } else {
            context.setCurrentRoleIds(userIdentity, groups);
        }
    }

}
