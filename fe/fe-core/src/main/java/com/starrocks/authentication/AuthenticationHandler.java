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
import com.google.common.collect.Lists;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.catalog.UserIdentityWithDnName;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.Pair;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class AuthenticationHandler {
    private static final Logger LOG = LogManager.getLogger(AuthenticationHandler.class);

    /**
     * Authenticate user
     *
     * @param context      the connection context
     * @param user         username
     * @param remoteHost   remote host address
     * @param authResponse authentication response from client
     * @return authenticated user identity
     * @throws AuthenticationException when authentication fails
     */
    public static UserIdentity authenticate(ConnectContext context, String user, String remoteHost,
                                            byte[] authResponse)
            throws AuthenticationException {
        if (user == null || user.isEmpty()) {
            throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, "", authResponse.length == 0 ? "NO" : "YES");
        }

        /*
         * authentication in Native first, and then check Security Integration if it does not exist internally.
         * If you check Security Integration first, it may cause internal users to wait too long.
         * For example, a meaningless authentication of OAuth2 may cause a long wait.
         */
        AuthenticationResult authenticationResult;
        authenticationResult = authenticateWithNative(context.getAuthenticationContext(), user, remoteHost, authResponse);

        // If the user does not exist in the native authentication method, authentication is performed in Security Integration
        if (authenticationResult == null) {
            authenticationResult =
                    authenticateWithSecurityIntegration(context.getAuthenticationContext(), user, remoteHost, authResponse);
        }

        if (authenticationResult == null) {
            throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, user, authResponse.length == 0 ? "NO" : "YES");
        }

        setAuthenticationResultToContext(context, authenticationResult);
        return authenticationResult.authenticatedUser;
    }

    private static AuthenticationResult authenticateWithNative(AuthenticationContext authContext, String user, String remoteHost,
                                                               byte[] authResponse)
            throws AuthenticationException {
        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();

        Map.Entry<UserIdentity, UserAuthenticationInfo> matchedUserIdentity =
                authenticationMgr.getBestMatchedUserIdentity(user, remoteHost);
        if (matchedUserIdentity == null) {
            if (Config.enable_auth_check) {
                LOG.debug("cannot find user {}@{}", user, remoteHost);
                return null;
            } else {
                LOG.info("enable_auth_check is false, but cannot find user '{}'@'{}'", user, remoteHost);
                throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, user,
                        authResponse.length == 0 ? "NO" : "YES");
            }
        } else {
            AuthenticationProvider provider;
            if (matchedUserIdentity.getValue().getAuthPlugin().equalsIgnoreCase(AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.name())) {
                provider = AuthenticationProviderFactory.create(matchedUserIdentity.getValue().getAuthPlugin(),
                        new String(matchedUserIdentity.getValue().getPassword(), StandardCharsets.UTF_8));
            } else {
                provider = AuthenticationProviderFactory.create(
                        matchedUserIdentity.getValue().getAuthPlugin(), matchedUserIdentity.getValue().getAuthString());
            }

            if (provider == null) {
                LOG.warn("authentication provider is null for user {}@{}, auth plugin: {}", user, remoteHost,
                        matchedUserIdentity.getValue().getAuthPlugin());
                throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, user,
                        authResponse.length == 0 ? "NO" : "YES");
            }
            authContext.setAuthenticationProvider(provider);

            if (Config.enable_auth_check) {
                //Throw an exception directly and feedback to the client
                provider.authenticate(authContext, matchedUserIdentity.getKey(), authResponse);
            }

            return new AuthenticationResult(matchedUserIdentity.getKey(), List.of(Config.group_provider), null);
        }
    }

    private static AuthenticationResult authenticateWithSecurityIntegration(AuthenticationContext authContext,
                                                                            String user,
                                                                            String remoteHost,
                                                                            byte[] authResponse) throws AuthenticationException {
        List<Pair<String, AuthenticationException>> exceptions = Lists.newArrayList();
        AuthenticationResult authenticationResult = null;
        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();

        String[] authChain = Config.authentication_chain;
        for (String authMechanism : authChain) {
            if (authenticationResult != null) {
                break;
            }

            SecurityIntegration securityIntegration = authenticationMgr.getSecurityIntegration(authMechanism);
            if (securityIntegration == null) {
                continue;
            }

            if (!Objects.requireNonNull(AuthPlugin.covertFromServerToClient(securityIntegration.getType()))
                    .equalsIgnoreCase(authContext.getAuthPlugin())) {
                continue;
            }

            AuthenticationProvider provider = securityIntegration.getAuthenticationProvider();
            if (provider == null) {
                LOG.warn("authentication provider is null for security integration: {}", authMechanism);
                continue;
            }

            UserIdentity userIdentity = UserIdentityWithDnName.createEphemeralUserIdent(user, remoteHost);

            try {
                authContext.setAuthenticationProvider(provider);
                provider.authenticate(authContext, userIdentity, authResponse);
            } catch (AuthenticationException e) {
                exceptions.add(new Pair<>(authMechanism, e));
                continue;
            }

            authenticationResult = new AuthenticationResult(userIdentity,
                    securityIntegration.getGroupProviderName() == null ?
                            List.of(Config.group_provider) : securityIntegration.getGroupProviderName(),
                    securityIntegration.getGroupAllowedLoginList());
        }

        if (authenticationResult == null && !exceptions.isEmpty()) {
            throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL_IN_AUTH_CHAIN,
                    Joiner.on(", ").join(exceptions.stream().map(e -> e.first + ": " + e.second.getMessage())
                            .collect(Collectors.toList())));
        }

        return authenticationResult;
    }

    private static void setAuthenticationResultToContext(ConnectContext context,
                                                         AuthenticationResult authenticationResult)
            throws AuthenticationException {
        String user = authenticationResult.authenticatedUser.getUser();

        context.setCurrentUserIdentity(authenticationResult.authenticatedUser);
        if (!authenticationResult.authenticatedUser.isEphemeral()) {
            context.setCurrentRoleIds(authenticationResult.authenticatedUser);

            UserProperty userProperty = GlobalStateMgr.getCurrentState().getAuthenticationMgr()
                    .getUserProperty(authenticationResult.authenticatedUser.getUser());
            context.updateByUserProperty(userProperty);
        }
        context.setQualifiedUser(user);

        Set<String> groups = getGroups(authenticationResult.authenticatedUser, authenticationResult.groupProviderName);
        context.setGroups(groups);

        if (authenticationResult.authenticatedGroupList != null && !authenticationResult.authenticatedGroupList.isEmpty()) {
            Set<String> intersection = new HashSet<>(groups);
            intersection.retainAll(authenticationResult.authenticatedGroupList);
            if (intersection.isEmpty()) {
                throw new AuthenticationException(ErrorCode.ERR_GROUP_ACCESS_DENY, user, Joiner.on(",").join(groups));
            }
        }
    }

    private static class AuthenticationResult {
        private UserIdentity authenticatedUser = null;
        private List<String> groupProviderName = null;
        private List<String> authenticatedGroupList = null;

        public AuthenticationResult(UserIdentity authenticatedUser,
                                    List<String> groupProviderName,
                                    List<String> authenticatedGroupList) {
            this.authenticatedUser = authenticatedUser;
            this.groupProviderName = groupProviderName;
            this.authenticatedGroupList = authenticatedGroupList;
        }
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
