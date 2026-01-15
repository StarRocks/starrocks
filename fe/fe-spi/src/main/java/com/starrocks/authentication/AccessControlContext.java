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


