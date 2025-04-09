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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.Pair;
import com.starrocks.epack.authentication.AuthenticationMgrEPack;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUserSecurityPolicyRequest;
import com.starrocks.thrift.TUserSecurityPolicyResponse;
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

        AuthenticationMgrEPack authenticationMgr =
                (AuthenticationMgrEPack) GlobalStateMgr.getCurrentState().getAuthenticationMgr();

        UserIdentity authenticatedUser = null;
        List<String> groupProviderName = null;
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
                            AuthenticationProvider provider = AuthenticationProviderFactory.create(
                                    matchedUserIdentity.getValue().getAuthPlugin(),
                                    matchedUserIdentity.getValue().getAuthString());
                            Preconditions.checkState(provider != null);
                            provider.authenticate(context, user, remoteHost, authResponse, randomString,
                                    matchedUserIdentity.getValue());
                            authenticatedUser = matchedUserIdentity.getKey();

                            if (authenticatedUser != null) {
                                if (authenticationMgr.checkUserPasswordExpired(authenticatedUser)) {
                                    context.setPasswordExpired(true);
                                }
                            }
                            groupProviderName = List.of(Config.group_provider);
                        } catch (AuthenticationException e) {
                            LOG.debug("failed to authenticate for native, user: {}@{}, error: {}",
                                    user, remoteHost, e.getMessage());

                            try {
                                if (GlobalStateMgr.getCurrentState().isLeader()) {
                                    authenticationMgr.increasePasswordErrorTimes(matchedUserIdentity.getKey());
                                } else {
                                    TAuthInfo tAuthInfo = new TAuthInfo();
                                    tAuthInfo.current_user_ident = matchedUserIdentity.getKey().toThrift();

                                    TUserSecurityPolicyRequest tUserSecurityPolicyRequest = new TUserSecurityPolicyRequest();
                                    tUserSecurityPolicyRequest.setAuthInfo(tAuthInfo);

                                    Pair<String, Integer> ipAndPort =
                                            GlobalStateMgr.getCurrentState().getNodeMgr().getLeaderIpAndRpcPort();
                                    TNetworkAddress thriftAddress = new TNetworkAddress(ipAndPort.first, ipAndPort.second);
                                    TUserSecurityPolicyResponse response = ThriftRPCRequestExecutor.call(
                                            ThriftConnectionPool.frontendPool,
                                            thriftAddress,
                                            client -> client.increasePasswordErrorTimes(tUserSecurityPolicyRequest));
                                }
                            } catch (Exception ex) {
                                LOG.error(ex);
                            }
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
                        provider.authenticate(context, user, remoteHost, authResponse, randomString, userAuthenticationInfo);

                        authenticatedUser = UserIdentity.createEphemeralUserIdent(user, securityIntegration.getName());
                        groupProviderName = securityIntegration.getGroupProviderName();
                        if (groupProviderName == null) {
                            groupProviderName = List.of(Config.group_provider);
                        }

                        authenticatedGroupList = securityIntegration.getGroupAllowedLoginList();
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
                groupProviderName = List.of(Config.group_provider);
            }
        }

        if (authenticatedUser == null) {
            throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, user, usePasswd);
        }

        context.setCurrentUserIdentity(authenticatedUser);
        if (!authenticatedUser.isEphemeral()) {
            context.setCurrentRoleIds(authenticatedUser);
            context.setAuthDataSalt(randomString);

            UserProperty userProperty =
                    GlobalStateMgr.getCurrentState().getAuthenticationMgr().getUserProperty(authenticatedUser.getUser());
            context.updateByUserProperty(userProperty);
        }
        context.setQualifiedUser(user);

        Set<String> groups = getGroups(authenticatedUser, groupProviderName);
        context.setGroups(groups);

        if (authenticatedGroupList != null && !authenticatedGroupList.isEmpty()) {
            Set<String> intersection = new HashSet<>(groups);
            intersection.retainAll(authenticatedGroupList);
            if (intersection.isEmpty()) {
                throw new AuthenticationException(ErrorCode.ERR_GROUP_ACCESS_DENY, user, Joiner.on(",").join(groups));
            }
        }

        return authenticatedUser;
    }

    public static Set<String> getGroups(UserIdentity userIdentity, List<String> groupProviderList) {
        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();

        HashSet<String> groups = new HashSet<>();
        for (String groupProviderName : groupProviderList) {
            GroupProvider groupProvider = authenticationMgr.getGroupProvider(groupProviderName);
            if (groupProvider == null) {
                continue;
            }
            groups.addAll(groupProvider.getGroup(userIdentity));
        }

        return groups;
    }
}
