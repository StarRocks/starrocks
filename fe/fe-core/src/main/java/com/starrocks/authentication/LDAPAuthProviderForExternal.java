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

import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.mysql.privilege.Password;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.commons.lang3.StringUtils;

import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

public class LDAPAuthProviderForExternal implements AuthenticationProvider {
    public static final String PLUGIN_NAME = AuthPlugin.AUTHENTICATION_LDAP_SIMPLE_FOR_EXTERNAL.name();

    @Override
    public UserAuthenticationInfo validAuthenticationInfo(UserIdentity userIdentity,
                                                          String password, String textForAuthPlugin)
            throws AuthenticationException {
        throw new AuthenticationException("unsupported");
    }

    private static Hashtable<String, String> buildEnvironment(String host, String port, String dn, String pwd) {
        String url = "ldap://" + host + ":" + port;
        Hashtable<String, String> environment = new Hashtable<>();
        dn = StringUtils.strip(dn, "\"'");
        environment.put(Context.SECURITY_CREDENTIALS, pwd);
        environment.put(Context.SECURITY_PRINCIPAL, dn);
        environment.put(Context.SECURITY_AUTHENTICATION, "simple");
        environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        environment.put(Context.PROVIDER_URL, url);

        return environment;
    }

    private static boolean checkLdapUserPwd(String host, String port, String userDn, String pwd) throws Exception {
        Hashtable<String, String> environment = buildEnvironment(host, port, userDn, pwd);
        DirContext ctx = null;
        try {
            // this will send a bind call to ldap server, throw exception if failed
            ctx = new InitialDirContext(environment);
            return true;
        } finally {
            if (ctx != null) {
                ctx.close();
            }
        }
    }

    public static boolean authenticate(String username, String password,
                                       String host, String port,
                                       String baseDn, String searchAttr,
                                       String rootDn, String rootPwd) throws Exception {
        Hashtable<String, String> environment = buildEnvironment(host, port, rootDn, rootPwd);
        DirContext ctx = null;
        NamingEnumeration<SearchResult> results = null;
        try {
            baseDn = StringUtils.strip(baseDn, "\"'");
            ctx = new InitialDirContext(environment);
            SearchControls searchControls = new SearchControls();
            searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
            String searchFilter = "(" + searchAttr + "=" + username + ")";
            results = ctx.search(baseDn, searchFilter, searchControls);

            String userDn;
            SearchResult searchResult = null;
            while (results.hasMore()) {
                if (searchResult != null) {
                    throw new Exception(
                            String.format("Got more than one search entry from ldap server %s:%s for user %s" +
                                            " with filter %s, previous: %s, current: %s",
                                    host, port, username, searchFilter, searchResult, results.next().toString()));
                } else {
                    searchResult = results.next();
                }
            }
            if (searchResult == null) {
                throw new Exception(String.format("cannot find user %s from ldap server %s:%s", username, host, port));
            }
            userDn = searchResult.getNameInNamespace();
            return checkLdapUserPwd(host, port, userDn, password);
        } finally {
            if (results != null) {
                results.close();
            }
            if (ctx != null) {
                ctx.close();
            }
        }
    }

    @Override
    public void authenticate(String user, String host, byte[] password, byte[] randomString,
                             UserAuthenticationInfo authenticationInfo) throws AuthenticationException {
        LDAPSecurityIntegration ldapSecurityIntegration =
                (LDAPSecurityIntegration) authenticationInfo.extraInfo.get(PLUGIN_NAME);
        try {
            boolean authenticated = LDAPAuthProviderForExternal.authenticate(
                    user, StringUtils.stripEnd(new String(password), "\0"),
                    ldapSecurityIntegration.getLdapServerHost(),
                    ldapSecurityIntegration.getLdapServerPort(),
                    ldapSecurityIntegration.getLdapBindBaseDn(),
                    ldapSecurityIntegration.getLdapUserSearchAttr(),
                    ldapSecurityIntegration.getLdapBindRootDn(),
                    ldapSecurityIntegration.getLdapBindRootPwd());
            if (!authenticated) {
                throw new AuthenticationException(String.format(
                                "external ldap authentication failure for user %s@%s", user, host));
            }
        } catch (Exception e) {
            throw new AuthenticationException(String.format(
                    "external ldap authentication failure for user %s@%s with exception, error: %s",
                    user, host, e.getMessage()),
                    e);
        }
    }

    @Override
    public UserAuthenticationInfo upgradedFromPassword(UserIdentity userIdentity, Password password)
            throws AuthenticationException {
        throw new AuthenticationException("unsupported");
    }
}
