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

    /**
     * @see #getAuthenticatedUserName()
     */
    private String authenticatedUserName = null;

    // After negotiate and switching with the client,
    // the auth plugin type used for this authentication is finally determined.
    private String authPlugin = null;

    // Auth Data salt generated at mysql negotiate used for password salting
    private byte[] authDataSalt = null;

    // The session's effective group set: the union of every source enabled at login. Two things it
    // is NOT: it is not filtered by `permitted_groups` (that is a login gate - an admitted user
    // keeps all its groups), and it is not recomputed while the session runs. Values are kept
    // exactly as the directory returned them, because Ranger matches group names case-sensitively.
    private Set<String> groups = new HashSet<>();

    // One input to `groups` above, not a second group set: the groups read from the `memberOf`
    // attribute of the user's own LDAP entry. The LDAP provider writes it on every authentication -
    // empty when the group source does not read memberOf - so it always states what *this* login
    // resolved. Nothing caches it. Never null.
    private Set<String> memberOfGroups = new HashSet<>();

    // currentRoleIds is the role that has taken effect in the current session.
    private Set<Long> currentRoleIds = new HashSet<>();

    // Bypass the authorizer check for certain cases
    private boolean bypassAuthorizerCheck = false;

    protected boolean passwordExpired = false;

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
     * The user name exactly as the directory holds it, filled in by an authentication provider that
     * looked the entry up. Null when the provider had no chance to read it, for example when it bound
     * directly through a DN pattern instead of searching.
     */
    public String getAuthenticatedUserName() {
        return authenticatedUserName;
    }

    public void setAuthenticatedUserName(String authenticatedUserName) {
        this.authenticatedUserName = authenticatedUserName;
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

    public void setPasswordExpired(boolean passwordExpired) {
        this.passwordExpired = passwordExpired;
    }

    public boolean isPasswordExpired() {
        return passwordExpired;
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

    /**
     * @return the effective group set of this session - the union of all enabled sources, not
     * filtered by `permitted_groups`. See the field comment for what does and does not go in.
     */
    public Set<String> getGroups() {
        return groups;
    }

    public Set<String> getMemberOfGroups() {
        return memberOfGroups;
    }

    public void setMemberOfGroups(Set<String> memberOfGroups) {
        this.memberOfGroups = memberOfGroups == null ? new HashSet<>() : memberOfGroups;
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


