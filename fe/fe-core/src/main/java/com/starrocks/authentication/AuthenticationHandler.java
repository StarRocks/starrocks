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
        authenticationResult = authenticateWithNative(context.getAccessControlContext(), user, remoteHost, authResponse);

        // If the user does not exist in the native authentication method, authentication is performed in Security Integration
        if (authenticationResult == null) {
            authenticationResult =
                    authenticateWithSecurityIntegration(context.getAccessControlContext(), user, remoteHost, authResponse);
        }

        if (authenticationResult == null) {
            throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, user, authResponse.length == 0 ? "NO" : "YES");
        }

        setAuthenticationResultToContext(context, authenticationResult);
        return authenticationResult.authenticatedUser;
    }

    private static AuthenticationResult authenticateWithNative(AccessControlContext authContext, String user, String remoteHost,
                                                               byte[] authResponse)
            throws AuthenticationException {
        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();

        Map.Entry<UserIdentity, UserAuthenticationInfo> matchedUserIdentity =
                authenticationMgr.getBestMatchedUserIdentityForLogin(user, remoteHost);
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

            return new AuthenticationResult(matchedUserIdentity.getKey(), List.of(Config.group_provider), null, "native");
        }
    }

    private static AuthenticationResult authenticateWithSecurityIntegration(AccessControlContext authContext,
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

            try {
                authContext.setAuthenticationProvider(provider);
                // Cleared per attempt: a value left by an earlier provider in the chain says nothing
                // about the one that ends up succeeding.
                authContext.setAuthenticatedUserName(null);
                provider.authenticate(authContext, UserIdentity.createEphemeralUserIdent(user, remoteHost), authResponse);
            } catch (AuthenticationException e) {
                exceptions.add(new Pair<>(authMechanism, e));
                continue;
            }

<<<<<<< HEAD
            authenticationResult = new AuthenticationResult(
                    UserIdentity.createEphemeralUserIdent(user, remoteHost),
                    securityIntegration.getGroupProviderName() == null ?
                            List.of(Config.group_provider) : securityIntegration.getGroupProviderName(),
=======
            // getGroupProviderName() returns an empty list (never null) when the security integration has no
            // `group_provider` property, so falling back to the global default must test isEmpty(), not null.
            List<String> groupProviderNames = securityIntegration.getGroupProviderName().isEmpty()
                    ? List.of(Config.group_provider)
                    : securityIntegration.getGroupProviderName();

            // This user has no entry in the user table, so the name the client typed is the only thing
            // StarRocks would otherwise identify it by - and for LDAP that name carries whatever casing
            // the client felt like using. Settle on one spelling here, once, so the session, the audit
            // log and current_user() all show the same identity. The directory's own spelling is the
            // authoritative one; lowercase is only the fallback for the direct-bind path, which never
            // searches and so never sees the entry.
            String authenticatedUserName = user;
            // `provider` is scoped to the try block above; the context holds the one that succeeded.
            if (isLdapProvider(authContext.getAuthenticationProvider())
                    && Config.authentication_ldap_case_insensitive) {
                String fromDirectory = authContext.getAuthenticatedUserName();
                authenticatedUserName = fromDirectory != null ? fromDirectory : LDAPAuthProvider.normalizeUsername(user);
            }

            authenticationResult = new AuthenticationResult(
                    UserIdentity.createEphemeralUserIdent(authenticatedUserName, remoteHost),
                    groupProviderNames,
>>>>>>> 0e135bc2c76 ([Enhancement] Treat LDAP/AD user and group names as case-insensitive (#61403))
                    securityIntegration.getGroupAllowedLoginList(),
                    authMechanism);
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

        // Step 1: Set user identity to context
        // Set the authenticated user identity as the current user for authorization purposes
        context.setCurrentUserIdentity(authenticationResult.authenticatedUser);
        // Set the qualified username for this connection session
        context.setQualifiedUser(user);

        // Step 2: Set distinguished name to context if it is empty
        // Distinguished name is used for LDAP authentication and group resolution
        // If not already set, use the username as the distinguished name
        if (context.getDistinguishedName().isEmpty()) {
            context.setDistinguishedName(user);
        }

        // Step 3: Set security integration to context
        // Record which security integration method was used for authentication
        // This helps track authentication method (native, LDAP, OAuth2, etc.)
        if (authenticationResult.securityIntegration != null) {
            context.setSecurityIntegration(authenticationResult.securityIntegration);
        }

        // Step 4: Resolve and set user groups
        // Get user groups from configured group providers (e.g., LDAP groups)
        // Groups are used for role-based access control and permission management
        Set<String> groups = getGroups(context.getCurrentUserIdentity(), context.getDistinguishedName(),
                authenticationResult.groupProviderName);
        context.setGroups(groups);
        // Set current role IDs based on the authenticated user and groups
        context.setCurrentRoleIds(authenticationResult.authenticatedUser, groups);

        // Step 5: Validate group access permissions
        // If authentication result specifies allowed groups, verify user belongs to at least one
        // This ensures users can only access groups they are authorized for
        if (authenticationResult.authenticatedGroupList != null && !authenticationResult.authenticatedGroupList.isEmpty()) {
<<<<<<< HEAD
            Set<String> intersection = new HashSet<>(groups);
            intersection.retainAll(authenticationResult.authenticatedGroupList);
            if (intersection.isEmpty()) {
                throw new AuthenticationException(ErrorCode.ERR_GROUP_ACCESS_DENY, user, Joiner.on(",").join(groups));
=======
            // Matched ignoring case: an LDAP `cn` is case-insensitive in the directory, so a group
            // name that differs only in case must not silently fail the gate. The group names
            // themselves are never rewritten - they are handed to Ranger, which matches
            // case-sensitively. The same relaxation is applied to the group-to-role lookup in
            // AuthorizationMgr#getRoleIdListByGroup, and the two must stay in sync.
            Set<String> allowedLowerCase = LDAPMemberOfExtractor.toLowerCaseSet(authenticationResult.authenticatedGroupList);
            boolean allowed = groups.stream()
                    .anyMatch(group -> group != null && allowedLowerCase.contains(group.toLowerCase(Locale.ROOT)));
            if (!allowed) {
                throw new AuthenticationException(ErrorCode.ERR_GROUP_ACCESS_DENY, user,
                        Joiner.on(",").join(groups),
                        Joiner.on(",").join(authenticationResult.authenticatedGroupList));
>>>>>>> 0e135bc2c76 ([Enhancement] Treat LDAP/AD user and group names as case-insensitive (#61403))
            }
        }

        // Step 6: Apply user properties for non-ephemeral users
        // Load and apply user-specific properties (session variables, resource limits, etc.)
        // Ephemeral users (from external auth) don't have stored properties
        if (!authenticationResult.authenticatedUser.isEphemeral()) {
            UserProperty userProperty = GlobalStateMgr.getCurrentState().getAuthenticationMgr()
                    .getUserProperty(user);
            context.updateByUserProperty(userProperty);
        }
    }

    private static class AuthenticationResult {
        private final UserIdentity authenticatedUser;
        private final List<String> groupProviderName;
        private final List<String> authenticatedGroupList;
        private final String securityIntegration;

        public AuthenticationResult(UserIdentity authenticatedUser,
                                    List<String> groupProviderName,
                                    List<String> authenticatedGroupList,
                                    String securityIntegration) {
            this.authenticatedUser = authenticatedUser;
            this.groupProviderName = groupProviderName;
            this.authenticatedGroupList = authenticatedGroupList;
            this.securityIntegration = securityIntegration;
        }
    }

<<<<<<< HEAD
    public static Set<String> getGroups(UserIdentity userIdentity, String distinguishedName, List<String> groupProviderList) {
=======
    /**
     * Whether the configured group providers contribute to this login's group set. Only an LDAP login
     * can turn them off (`group_source = memberof`), and the question is asked of the provider rather
     * than of the config, which would disable them for native users too.
     */
    private static boolean isGroupProviderUsed(AccessControlContext accessControlContext) {
        AuthenticationProvider provider = accessControlContext.getAuthenticationProvider();
        if (!(provider instanceof LDAPAuthProvider)) {
            return true;
        }
        // enable_auth_check = false skips provider.authenticate() above, so memberOf was never read:
        // letting `memberof` suppress the providers too would leave the login with no groups at all.
        return !Config.enable_auth_check || ((LDAPAuthProvider) provider).isGroupProviderUsed();
    }

    /**
     * The mirror of {@link #isGroupProviderUsed}. Any other provider leaves the field empty, so this
     * is belt and braces - but it keeps the two sources from drifting apart.
     */
    private static boolean isMemberOfUsed(AccessControlContext accessControlContext) {
        AuthenticationProvider provider = accessControlContext.getAuthenticationProvider();
        return provider instanceof LDAPAuthProvider && ((LDAPAuthProvider) provider).isMemberOfUsed();
    }

    /**
     * Whether the credentials were checked against an LDAP directory. Both providers count: the
     * enterprise type='ldap' integration authenticates against a directory just as the community one
     * does. It is not a subclass of {@link LDAPAuthProvider}, which is why this cannot be a single
     * instanceof.
     */
    private static boolean isLdapProvider(AuthenticationProvider provider) {
        return provider instanceof LDAPAuthProvider || provider instanceof LDAPAuthProviderForExternal;
    }

    /**
     * Ask the listed group providers which groups this user belongs to, and union their answers.
     * <p>
     * This is **one source out of two**, not the final group set of a session:
     * <ul>
     *     <li>the groups read from the user's own LDAP entry (`memberOf`) are <b>not</b> included -
     *     they are merged on top of this in {@link #setAuthenticationResultToContext};</li>
     *     <li>`permitted_groups` is <b>not</b> applied - that is a login gate, not a filter, and it
     *     runs after the merge.</li>
     * </ul>
     * Callers that want "the groups of this session" should read AccessControlContext#getGroups()
     * instead.
     */
    public static Set<String> resolveGroupsFromProviders(UserIdentity userIdentity, String distinguishedName,
                                                         List<String> groupProviderList) {
>>>>>>> 0e135bc2c76 ([Enhancement] Treat LDAP/AD user and group names as case-insensitive (#61403))
        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();

        HashSet<String> groups = new HashSet<>();
        for (String groupProviderName : groupProviderList) {
            GroupProvider groupProvider = authenticationMgr.getGroupProvider(groupProviderName);
            if (groupProvider == null) {
                continue;
            }
            groups.addAll(groupProvider.getGroup(userIdentity, distinguishedName));
        }

        return groups;
    }
}
