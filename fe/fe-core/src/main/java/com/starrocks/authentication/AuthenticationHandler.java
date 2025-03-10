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

import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.Pair;
import com.starrocks.epack.authentication.AuthenticationMgrEPack;
import com.starrocks.mysql.privilege.AuthPlugin;
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

                            if (authenticatedUser != null) {
                                if (authenticationMgr.checkUserPasswordExpired(authenticatedUser)) {
                                    context.setPasswordExpired(true);
                                }
                            }
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
                        if (securityIntegration.getType().equalsIgnoreCase(SecurityIntegration.SECURITY_INTEGRATION_TYPE_LDAP)) {
                            userAuthenticationInfo.extraInfo.put(AuthPlugin.AUTHENTICATION_LDAP_SIMPLE_FOR_EXTERNAL.name(),
                                    securityIntegration);

                            provider.authenticate(user, remoteHost, authResponse, randomString, userAuthenticationInfo);

                            AuthorizationMgr authorizationMgr = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
                            Set<Long> roleIds = authorizationMgr.getRoleMappingMetaMgr()
                                    .getMappedRoleIdsForLdapUser(securityIntegration.getName(), user);
                            if (roleIds.isEmpty()) {
                                LOG.info("authenticate '{}' with security integration '{}' successfully," +
                                                " but cannot map any role, will try other auth mechanisms",
                                        user, securityIntegration.getName());
                            } else {
                                authenticatedUser = UserIdentity.createEphemeralUserIdent(user, authMechanism);
                                context.setCurrentRoleIds(roleIds);
                            }
                        } else {
                            provider.authenticate(user, remoteHost, authResponse, randomString, userAuthenticationInfo);
                            authenticatedUser = UserIdentity.createEphemeralUserIdent(user, securityIntegration.getName());
                        }
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

        return authenticatedUser;
    }
}
