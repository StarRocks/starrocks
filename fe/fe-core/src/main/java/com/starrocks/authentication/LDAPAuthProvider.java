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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.util.NetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Optional;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.PartialResultException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.net.ssl.SSLContext;

public class LDAPAuthProvider implements AuthenticationProvider {
    private static final Logger LOG = LogManager.getLogger(LDAPAuthProvider.class);
    private final String ldapServerHost;
    private final int ldapServerPort;
    private final boolean useSSL;
    private final String trustStorePath;
    private final String trustStorePwd;
    private final String ldapBindRootDN;
    private final String ldapBindRootPwd;
    private final String ldapBindBaseDN;
    private final String ldapSearchFilter;
    private final String ldapUserDN;

    public LDAPAuthProvider(String ldapServerHost,
                            int ldapServerPort,
                            boolean useSSL,
                            String trustStorePath,
                            String trustStorePwd,
                            String ldapBindRootDN,
                            String ldapBindRootPwd,
                            String ldapBindBaseDN,
                            String ldapSearchFilter,
                            String ldapUserDN) {
        this.ldapServerHost = ldapServerHost;
        this.ldapServerPort = ldapServerPort;
        this.useSSL = useSSL;
        this.trustStorePath = trustStorePath;
        this.trustStorePwd = trustStorePwd;
        this.ldapBindRootDN = ldapBindRootDN;
        this.ldapBindRootPwd = ldapBindRootPwd;
        this.ldapBindBaseDN = ldapBindBaseDN;
        this.ldapSearchFilter = ldapSearchFilter;
        this.ldapUserDN = ldapUserDN;
    }

    @Override
    public void authenticate(AccessControlContext authContext, UserIdentity userIdentity, byte[] authResponse)
            throws AuthenticationException {
        //clear password terminate string
        byte[] clearPassword = authResponse;
        if (authResponse[authResponse.length - 1] == 0) {
            clearPassword = Arrays.copyOf(authResponse, authResponse.length - 1);
        }

        try {
            String distinguishedName;
            if (!Strings.isNullOrEmpty(ldapUserDN)) {
                distinguishedName = ldapUserDN;
            } else {
                distinguishedName = findUserDNByRoot(userIdentity.getUser());
            }
            Preconditions.checkNotNull(distinguishedName);
            checkPassword(distinguishedName, new String(clearPassword, StandardCharsets.UTF_8));

            // set distinguished name to auth context
            authContext.setDistinguishedName(distinguishedName);
        } catch (Exception e) {
            LOG.warn("check password failed for user: {}", userIdentity.getUser(), e);
            throw new AuthenticationException(e.getMessage());
        }
    }

    private String getURL() {
        if (useSSL) {
            return "ldaps://" + NetUtils.getHostPortInAccessibleFormat(ldapServerHost, ldapServerPort);
        } else {
            return "ldap://" + NetUtils.getHostPortInAccessibleFormat(ldapServerHost, ldapServerPort);
        }
    }

    private void setSSLContext(Hashtable<String, String> env) throws Exception {
        SSLContext sslContext = SslUtils.createSSLContext(
                Optional.empty(), /* For now, we don't support server to verify us(client). */
                Optional.empty(),
                Strings.isNullOrEmpty(trustStorePath) ? Optional.empty() : Optional.of(new File(trustStorePath)),
                Strings.isNullOrEmpty(trustStorePwd) ? Optional.empty() : Optional.of(trustStorePwd));
        LdapSslSocketFactory.setSslContextForCurrentThread(sslContext);
        // Refer to https://docs.oracle.com/javase/jndi/tutorial/ldap/security/ssl.html.
        env.put("java.naming.ldap.factory.socket", LdapSslSocketFactory.class.getName());
    }

    //bind to ldap server to check password
    protected void checkPassword(String dn, String password) throws Exception {
        if (Strings.isNullOrEmpty(password)) {
            throw new AuthenticationException("empty password is not allowed for simple authentication");
        }

        String url = getURL();
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_CREDENTIALS, password);
        env.put(Context.SECURITY_PRINCIPAL, dn);
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, url);
        if (useSSL) {
            setSSLContext(env);
        }

        DirContext ctx = null;
        try {
            //this will send a bind call to ldap server, throw exception if failed
            ctx = new InitialDirContext(env);
        } finally {
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (Exception e) {
                }
            }
        }
    }

    //1. bind ldap server by root dn
    //2. search user
    //3. if match exactly one, return the user's actual DN
    protected String findUserDNByRoot(String user) throws Exception {
        if (Strings.isNullOrEmpty(ldapBindRootPwd)) {
            throw new AuthenticationException("empty password is not allowed for simple authentication");
        }

        String url = getURL();
        Hashtable<String, String> env = new Hashtable<>();
        //dn contains '=', so we should use ' or " to wrap the value in config file
        String rootDN = ldapBindRootDN;
        rootDN = trim(rootDN, "\"");
        rootDN = trim(rootDN, "'");
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_CREDENTIALS, ldapBindRootPwd);
        env.put(Context.SECURITY_PRINCIPAL, rootDN);
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, url);
        if (useSSL) {
            setSSLContext(env);
        }

        DirContext ctx = null;
        try {
            String baseDN = ldapBindBaseDN;
            baseDN = trim(baseDN, "\"");
            baseDN = trim(baseDN, "'");
            SearchControls sc = new SearchControls();
            sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
            // Escapes special characters in user input to prevent LDAP injection
            String safeUser = escapeLdapValue(user);
            String searchFilter = "(" + ldapSearchFilter + "=" + safeUser + ")";
            ctx = new InitialDirContext(env);
            NamingEnumeration<SearchResult> results = ctx.search(baseDN, searchFilter, sc);

            String userDN = null;
            int matched = 0;

            try {
                while (results.hasMore()) {
                    matched++;
                    if (matched > 1) {
                        throw new AuthenticationException("searched more than one entry from ldap server for user " + user);
                    }

                    SearchResult result = results.next();
                    userDN = result.getNameInNamespace();
                }
            } catch (PartialResultException e) {
                LOG.warn("ldap search partial result exception", e);
            }

            if (matched != 1) {
                throw new AuthenticationException("ldap search matched user count " + matched);
            }

            return userDN;
        } finally {
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (Exception e) {
                }
            }
        }
    }

    // trim prefix and suffix of target from src
    private static String trim(String src, String target) {
        if (src != null && target != null) {
            if (src.startsWith(target)) {
                src = src.substring(target.length());
            }
            if (src.endsWith(target)) {
                src = src.substring(0, src.length() - target.length());
            }
        }
        return src;
    }

    public static String escapeLdapValue(String value) {
        if (value == null) {
            return null;
        }

        value = value.replace("\\", "\\5c");
        value = value.replace("*", "\\2a");
        value = value.replace("(", "\\28");
        value = value.replace(")", "\\29");
        value = value.replace("|", "\\7c");
        value = value.replace("\\u0000", "\\00");
        return value;
    }
}
