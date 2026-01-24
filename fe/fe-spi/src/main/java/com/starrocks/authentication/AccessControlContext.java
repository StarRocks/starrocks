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

import com.starrocks.catalog.UserIdentity;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * AccessControlContext encapsulates authentication and authorization information for a connection session.
 * This includes user identity, roles, groups, and authentication-related metadata.
 * <p>
 * AccessControlContext unifies both the previous AuthenticationContext and AuthorizationContext
 * so that identity, auth metadata, role/group memberships, and access-control flags live together.
 */
public class AccessControlContext {
    // `qualifiedUser` is the user used when the user establishes connection and authentication.
    // It is the real user used for this connection.
    // Different from the `currentUserIdentity` authentication user of execute as,
    // `qualifiedUser` should not be changed during the entire session.
    private String qualifiedUser;

    // `currentUserIdentity` is the user used for authorization. Under normal circumstances,
    // `currentUserIdentity` and `qualifiedUser` are the same user,
    // but currentUserIdentity may be modified by execute as statement.
    private UserIdentity currentUserIdentity;

    // Original (login) user context snapshot.
    // This is initialized once right after authentication succeeds and should not change even after EXECUTE AS.
    // It is used for cases where permission checks (e.g. IMPERSONATE) need to be based on the original login user
    // instead of the current impersonated user.
    private UserIdentity originalUserIdentity = null;
    private Set<String> originalGroups = null;
    private Set<Long> originalRoleIds = null;

    // Distinguished name (DN) used for LDAP authentication and group resolution
    // In LDAP context, this represents the unique identifier of a user in the directory
    // For non-LDAP authentication, this typically defaults to the username
    // Used by group providers to resolve user group memberships
    protected String distinguishedName = "";

    // The Token in the OpenIDConnect authentication method is obtained
    // from the authentication logic and stored in the AuthenticationContext.
    // If the downstream system needs it, it needs to be obtained from the AuthenticationContext.
    private volatile String authToken = null;

    // The security integration method used for authentication.
    protected String securityIntegration = "native";

    // The authentication provider used for this authentication.
    private AuthenticationProvider authenticationProvider = null;

    // After negotiate and switching with the client,
    // the auth plugin type used for this authentication is finally determined.
    private String authPlugin = null;

    // Auth Data salt generated at mysql negotiate used for password salting
    private byte[] authDataSalt = null;

    // groups of current user
    private Set<String> groups = new HashSet<>();

    // currentRoleIds is the role that has taken effect in the current session.
    private Set<Long> currentRoleIds = new HashSet<>();

    // Bypass the authorizer check for certain cases
    private boolean bypassAuthorizerCheck = false;

    public AccessControlContext() {
        // Default constructor
    }

    public String getQualifiedUser() {
        return qualifiedUser;
    }

    public void setQualifiedUser(String qualifiedUser) {
        this.qualifiedUser = qualifiedUser;
    }

    public UserIdentity getCurrentUserIdentity() {
        return currentUserIdentity;
    }

    public void setCurrentUserIdentity(UserIdentity currentUserIdentity) {
        this.currentUserIdentity = currentUserIdentity;
    }

    /**
     * Initialize or update the original(login) user context from the result of a successful
     * authentication. Called once per login and again on re-authentication of the same
     * connection (e.g. to refresh roles after grant/revoke). For CHANGE USER, the caller
     * must invoke {@link #resetOriginalUserContext()} before re-auth so this sets the new user.
     */
    public void initOriginalUserContext(UserIdentity userIdentity, Set<String> groups, Set<Long> roleIds) {
        this.originalUserIdentity = userIdentity;
        this.originalGroups = groups == null ? new HashSet<>() : new HashSet<>(groups);
        this.originalRoleIds = roleIds == null ? new HashSet<>() : new HashSet<>(roleIds);
    }

    /**
     * Reset the original(login) user context.
     * This should be called when re-authenticating (e.g., MySQL CHANGE USER) to prevent
     * the new user from inheriting IMPERSONATE privileges from the previous login user.
     */
    public void resetOriginalUserContext() {
        this.originalUserIdentity = null;
        this.originalGroups = null;
        this.originalRoleIds = null;
    }

    /**
     * Restore the original(login) user context from a backup.
     * Used when re-authentication (e.g., CHANGE USER) fails and the session must remain
     * unchanged; otherwise the original snapshot would be lost and EXECUTE AS chaining
     * would use the wrong privileges.
     */
    public void setOriginalUserContext(UserIdentity userIdentity, Set<String> groups, Set<Long> roleIds) {
        this.originalUserIdentity = userIdentity;
        this.originalGroups = groups == null ? null : new HashSet<>(groups);
        this.originalRoleIds = roleIds == null ? null : new HashSet<>(roleIds);
    }

    public UserIdentity getOriginalUserIdentity() {
        return originalUserIdentity;
    }

    public Set<String> getOriginalGroups() {
        return originalGroups == null ? Collections.emptySet() : originalGroups;
    }

    public Set<Long> getOriginalRoleIds() {
        return originalRoleIds == null ? Collections.emptySet() : originalRoleIds;
    }

    public void setDistinguishedName(String distinguishedName) {
        this.distinguishedName = distinguishedName;
    }

    public String getDistinguishedName() {
        return distinguishedName;
    }

    public String getAuthToken() {
        return authToken;
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    public AuthenticationProvider getAuthenticationProvider() {
        return authenticationProvider;
    }

    public void setAuthenticationProvider(AuthenticationProvider authenticationProvider) {
        this.authenticationProvider = authenticationProvider;
    }

    public String getAuthPlugin() {
        return authPlugin;
    }

    public void setAuthPlugin(String authPlugin) {
        this.authPlugin = authPlugin;
    }

    public byte[] getAuthDataSalt() {
        return authDataSalt;
    }

    public void setAuthDataSalt(byte[] authDataSalt) {
        this.authDataSalt = authDataSalt;
    }

    public String getSecurityIntegration() {
        return securityIntegration;
    }

    public void setSecurityIntegration(String securityIntegration) {
        this.securityIntegration = securityIntegration;
    }

    public Set<Long> getCurrentRoleIds() {
        return currentRoleIds;
    }

    public void setCurrentRoleIds(Set<Long> currentRoleIds) {
        this.currentRoleIds = currentRoleIds;
    }

    public Set<String> getGroups() {
        return groups;
    }

    public void setGroups(Set<String> groups) {
        this.groups = groups;
    }

    public boolean isBypassAuthorizerCheck() {
        return bypassAuthorizerCheck;
    }

    public void setBypassAuthorizerCheck(boolean bypassAuthorizerCheck) {
        this.bypassAuthorizerCheck = bypassAuthorizerCheck;
    }
}


