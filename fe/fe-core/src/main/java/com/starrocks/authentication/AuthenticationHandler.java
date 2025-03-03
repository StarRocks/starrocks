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

import com.starrocks.common.Config;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.ErrorCode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthenticationHandler {
    private static final Logger LOG = LogManager.getLogger(AuthenticationHandler.class);

    public static UserIdentity authenticate(ConnectContext context, String user, String remoteHost,
                                            byte[] authResponse, byte[] randomString) throws AuthenticationException {
        String usePasswd = authResponse.length == 0 ? "NO" : "YES";
        if (user == null || user.isEmpty()) {
            throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, "", usePasswd);
        }

        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();

        UserIdentity authenticatedUser = null;
        String groupProviderName = null;
        List<String> authenticatedGroupList = null;

        if (Config.enable_auth_check) {
            String[] authChain = Config.authentication_chain;

            for (String authMechanism : authChain) {
                if (authenticatedUser != null) {
                    break;
                }

                if (authMechanism.equals(ConfigBase.AUTHENTICATION_CHAIN_MECHANISM_NATIVE)) {
                    Map.Entry<UserIdentity, UserAuthenticationInfo> matchedUserIdentity =
                            authenticationMgr.getBestMatchedUserIdentity(user, remoteHost);

                    if (matchedUserIdentity == null) {
                        LOG.debug("cannot find user {}@{}", user, remoteHost);
                    } else {
                        try {
                            AuthenticationProvider provider =
                                    AuthenticationProviderFactory.create(matchedUserIdentity.getValue().getAuthPlugin());
                            provider.authenticate(user, remoteHost, authResponse, randomString, matchedUserIdentity.getValue());
                            authenticatedUser = matchedUserIdentity.getKey();

                            groupProviderName = Config.group_provider;
                            authenticatedGroupList = List.of(Config.authenticated_group_list);
                        } catch (AuthenticationException e) {
                            LOG.debug("failed to authenticate for native, user: {}@{}, error: {}",
                                    user, remoteHost, e.getMessage());
                        }
                    }
                } else {
                    SecurityIntegration securityIntegration = authenticationMgr.getSecurityIntegration(authMechanism);
                    if (securityIntegration == null) {
                        continue;
                    }

                    try {
                        AuthenticationProvider provider = securityIntegration.getAuthenticationProvider();
                        UserAuthenticationInfo userAuthenticationInfo = new UserAuthenticationInfo();
                        provider.authenticate(user, remoteHost, authResponse, randomString, userAuthenticationInfo);
                        // the ephemeral user is identified as 'username'@'auth_mechanism'
                        authenticatedUser = UserIdentity.createEphemeralUserIdent(user, securityIntegration.getName());

                        groupProviderName = securityIntegration.getGroupProviderName();
                        if (groupProviderName == null) {
                            groupProviderName = Config.group_provider;
                        }

                        authenticatedGroupList = securityIntegration.getAuthenticatedGroupList();
                    } catch (AuthenticationException e) {
                        LOG.debug("failed to authenticate, user: {}@{}, security integration: {}, error: {}",
                                user, remoteHost, securityIntegration, e.getMessage());
                    }
                }
            }
        } else {
            Map.Entry<UserIdentity, UserAuthenticationInfo> matchedUserIdentity =
                    authenticationMgr.getBestMatchedUserIdentity(user, remoteHost);
            if (matchedUserIdentity == null) {
                LOG.info("enable_auth_check is false, but cannot find user '{}'@'{}'", user, remoteHost);
                throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, user, usePasswd);
            } else {
                authenticatedUser = matchedUserIdentity.getKey();
                groupProviderName = Config.group_provider;
                authenticatedGroupList = List.of(Config.authenticated_group_list);
            }
        }

        if (authenticatedUser == null) {
            throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, user, usePasswd);
        }

        context.setCurrentUserIdentity(authenticatedUser);
        if (!authenticatedUser.isEphemeral()) {
            context.setCurrentRoleIds(authenticatedUser);
            context.setAuthDataSalt(randomString);
        }
        context.setQualifiedUser(user);

        Set<String> groups = getGroups(authenticatedUser, groupProviderName);
        context.setGroups(groups);

        if (!authenticatedGroupList.isEmpty()) {
            Set<String> intersection = new HashSet<>(groups);
            intersection.retainAll(authenticatedGroupList);
            if (intersection.isEmpty()) {
                throw new AuthenticationException(ErrorCode.ERR_GROUP_ACCESS_DENY);
            }
        }

        return authenticatedUser;
    }

    public static Set<String> getGroups(UserIdentity userIdentity, String groupProviderName) {
        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        GroupProvider groupProvider = authenticationMgr.getGroupProvider(groupProviderName);
        if (groupProvider == null) {
            return new HashSet<>();
        }
        return groupProvider.getGroup(userIdentity);
    }
}
