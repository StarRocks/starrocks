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

package com.starrocks.epack.authentication;

import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import javax.naming.NamingEnumeration;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

public class LDAPAuthProviderForExternal implements AuthenticationProvider {
    private static final Logger LOG = LogManager.getLogger(LDAPAuthProviderForExternal.class);
    private final String securityIntegrationName;

    public LDAPAuthProviderForExternal(String securityIntegrationName) {
        this.securityIntegrationName = securityIntegrationName;
    }

    private static boolean checkLdapUserPwd(LDAPSecurityIntegration securityIntegration,
                                            String userDn, String userPwd) throws Exception {
        DirContext ctx = null;
        try {
            // this will send a bind call to ldap server, throw exception if failed
            ctx = securityIntegration.createDirContextOnConnection(userDn, userPwd);
            if (ctx == null) {
                throw new AuthenticationException(String.format("create ldap context for %s failed, " +
                                "pwd len: %s, security integration: %s", userDn,
                        userPwd == null ? "null" : userPwd.length(), securityIntegration));
            }
            return true;
        } finally {
            if (ctx != null) {
                ctx.close();
            }
        }
    }

    public static boolean authenticate(String username, String userPwd,
                                       LDAPSecurityIntegration securityIntegration) throws Exception {

        DirContext rootCtx = null;
        NamingEnumeration<SearchResult> results = null;

        try {
            rootCtx = securityIntegration.createDirContextOnConnection();
            if (rootCtx == null) {
                throw new Exception("create dir context failed");
            }
            SearchControls searchControls = new SearchControls();
            searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            String searchFilter = "(" + securityIntegration.getLdapUserSearchAttr() + "=" + username + ")";
            // In case of PartialResultException, we support multi base dn to search from,
            // i.e. the base dn property of security integration can be configured with
            // multi specific DNs, separated by ';'.
            String baseDnPropVal = securityIntegration.getLdapBindBaseDn();
            Set<String> baseDnSet = Arrays.stream(baseDnPropVal.split(";"))
                    .map(String::trim)
                    .collect(Collectors.toSet());
            String currentBaseDn = null;
            for (String baseDn : baseDnSet) {
                LOG.debug("searching user {} with baseDn {}", searchFilter, baseDn);
                currentBaseDn = baseDn;
                try {
                    results = rootCtx.search(baseDn, searchFilter, searchControls);
                } catch (Exception e) {
                    LOG.info("searching user {} from {} failed with exception, error: {}",
                            searchFilter, baseDn, e.getMessage());
                    continue;
                }
                // If we find the user from this base dn, loop ends.
                if (results.hasMore()) {
                    break;
                }
            }

            String userDn;
            SearchResult searchResult = null;
            while (results != null && results.hasMore()) {
                if (searchResult != null) {
                    throw new Exception(
                            String.format("Got more than one search entry from ldap server for user %s" +
                                            " with filter %s, previous: %s, current: %s, security integration: %s," +
                                            " current using baseDn: %s", username, searchFilter, searchResult,
                                    results.next().toString(), securityIntegration, currentBaseDn));
                } else {
                    searchResult = results.next();
                }
            }
            if (searchResult == null) {
                throw new Exception(String.format("cannot find user %s from ldap security integration: %s",
                        username, securityIntegration));
            }
            userDn = searchResult.getNameInNamespace();
            return checkLdapUserPwd(securityIntegration, userDn, userPwd);
        } finally {
            if (results != null) {
                results.close();
            }
            if (rootCtx != null) {
                rootCtx.close();
            }
        }
    }

    @Override
    public void authenticate(ConnectContext context, UserIdentity userIdentity, byte[] authResponse)
            throws AuthenticationException {
        LDAPSecurityIntegration ldapSecurityIntegration = (LDAPSecurityIntegration) GlobalStateMgr.getCurrentState()
                .getAuthenticationMgr().getSecurityIntegration(securityIntegrationName);
        try {
            boolean authenticated = LDAPAuthProviderForExternal.authenticate(
                    userIdentity.getUser(), StringUtils.stripEnd(new String(authResponse), "\0"), ldapSecurityIntegration);
            if (!authenticated) {
                throw new AuthenticationException(String.format(
                        "external ldap authentication failure for user %s@%s", userIdentity.getUser(), userIdentity.getHost()));
            }
        } catch (Exception e) {
            throw new AuthenticationException(String.format(
                    "external ldap authentication failure for user %s@%s with exception, error: %s",
                    userIdentity.getUser(), userIdentity.getHost(), e.getMessage()), e);
        }

        AuthorizationMgr authorizationMgr = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        Set<Long> roleIds = authorizationMgr.getRoleMappingMetaMgr()
                .getMappedRoleIdsForLdapUser(securityIntegrationName, userIdentity.getUser());
        if (roleIds.isEmpty()) {
            LOG.info("authenticate '{}' with security integration '{}' successfully," +
                            " but cannot map any role, will try other auth mechanisms",
                    userIdentity.getUser(), securityIntegrationName);
            throw new AuthenticationException("Cannot map any role ids for security integration");
        } else {
            context.setCurrentRoleIds(roleIds);
        }
    }
}
