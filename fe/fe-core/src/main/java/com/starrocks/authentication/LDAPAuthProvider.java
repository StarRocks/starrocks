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
<<<<<<< HEAD
=======
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
>>>>>>> 7cb1013 ([BugFix] Authenticate security integration (LDAP) users on the non-MySQL channels (#79165))
import com.starrocks.common.util.NetUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserIdentity;
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
    private final String ldapBindDNPattern;

    public LDAPAuthProvider(String ldapServerHost,
                            int ldapServerPort,
                            boolean useSSL,
                            String trustStorePath,
                            String trustStorePwd,
                            String ldapBindRootDN,
                            String ldapBindRootPwd,
                            String ldapBindBaseDN,
                            String ldapSearchFilter,
                            String ldapUserDN,
                            String ldapBindDNPattern) {
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
        this.ldapBindDNPattern = ldapBindDNPattern;
    }

    @Override
    public void authenticate(ConnectContext context, UserIdentity userIdentity, byte[] authResponse)
            throws AuthenticationException {
        // clear password terminate string: the MySQL clear-password frame appends a trailing \0, while
        // HTTP Basic sends the bare password and may even send an empty one.
        byte[] clearPassword = authResponse;
        if (authResponse.length > 0 && authResponse[authResponse.length - 1] == 0) {
            clearPassword = Arrays.copyOf(authResponse, authResponse.length - 1);
        }

        try {
            String password = new String(clearPassword, StandardCharsets.UTF_8);
            String distinguishedName;
            if (!Strings.isNullOrEmpty(ldapUserDN)) {
                // Priority 1: per-user DN (from CREATE USER ... AS 'dn')
                distinguishedName = ldapUserDN;
                checkPassword(distinguishedName, password);
            } else if (!Strings.isNullOrEmpty(ldapBindDNPattern)) {
                // Priority 2: direct bind via DN pattern
                distinguishedName = authenticateByPattern(userIdentity.getUser(), password);
            } else {
                // Priority 3: search-and-bind
                distinguishedName = findUserDNByRoot(userIdentity.getUser());
                checkPassword(distinguishedName, password);
            }
            Preconditions.checkNotNull(distinguishedName);

            // set distinguished name to auth context
<<<<<<< HEAD
            context.setDistinguishedName(distinguishedName);
        } catch (Exception e) {
            LOG.warn("check password failed for user: {}", userIdentity.getUser(), e);
=======
            authContext.setDistinguishedName(distinguishedName);
        } catch (AuthenticationException e) {
            // Already classified (empty password, user not found, or a nested bind failure): pass it through.
            LOG.warn("check password failed for user: {}", user, e);
            throw e;
        } catch (javax.naming.AuthenticationException e) {
            // The directory answered and rejected the credential -- a definitive "wrong password".
            LOG.warn("check password failed for user: {}", user, e);
>>>>>>> 7cb1013 ([BugFix] Authenticate security integration (LDAP) users on the non-MySQL channels (#79165))
            throw new AuthenticationException(e.getMessage());
        } catch (Exception e) {
            // Anything else means we could not get an answer: connection refused, timeout, TLS failure.
            // Mark it transient so a caller that caches rejections does not hold a valid credential back
            // until the directory recovers.
            LOG.warn("cannot reach the directory while authenticating user: {}", user, e);
            throw new AuthenticationException(e.getMessage()).asTransient();
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

    // Try each DN pattern in order, return the first successfully bound DN.
    // Patterns are separated by semicolon ';'.
    protected String authenticateByPattern(String user, String password) throws Exception {
        String safeUser = escapeDnValue(normalizeUsername(user));
        String[] rawPatterns = ldapBindDNPattern.split(";");
        // Pre-validate all patterns before attempting any bind
        String[] patterns = new String[rawPatterns.length];
        for (int i = 0; i < rawPatterns.length; i++) {
            patterns[i] = trim(trim(rawPatterns[i].trim(), "\""), "'");
            if (!patterns[i].contains("${USER}")) {
                throw new AuthenticationException(
                        "Invalid bind DN pattern: '" + patterns[i] + "' does not contain ${USER} placeholder. " +
                        "Each pattern segment must include ${USER} to prevent shared-DN authentication bypass.");
            }
            if (!patterns[i].contains("=")) {
                throw new AuthenticationException(
                        "Invalid bind DN pattern: '" + patterns[i] + "' is not a valid DN format. " +
                        "Pattern must produce a Distinguished Name (e.g., 'uid=${USER},ou=People,dc=example,dc=com'). " +
                        "UPN-style patterns like '${USER}@domain' are not supported.");
            }
        }
        Exception lastException = null;
        for (String pattern : patterns) {
            String dn = pattern.replace("${USER}", safeUser);
            try {
                checkPassword(dn, password);
                return dn;
            } catch (Exception e) {
                lastException = e;
<<<<<<< HEAD
                LOG.debug("direct bind failed for pattern '{}' with user '{}': {}", pattern, user, e.getMessage());
=======
                LOG.debug("direct bind failed for dn '{}' with user '{}': {}", dn, user, e.getMessage());
            }
        }
        // Null only when the pattern produced no candidate at all - `";"` splits to an empty array.
        throw lastException != null ? lastException
                : new AuthenticationException("bind dn pattern '" + ldapBindDNPattern + "' produced no candidate DN");
    }

    /**
     * Build the JNDI environment for a simple bind as `principal`. Shared by the three places that
     * open a connection - the user bind, the service account search and the memberOf probe - so that
     * a change to URL handling, SSL or timeouts only has to be made once.
     */
    // Package-private so a test can assert the two timeouts below are actually set.
    Hashtable<String, String> buildEnv(String principal, String credentials) throws Exception {
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_CREDENTIALS, credentials);
        env.put(Context.SECURITY_PRINCIPAL, principal);
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, getURL());
        // Both timeouts matter: without them the bind falls back to the OS TCP timeout, so an unreachable
        // directory can pin the calling thread -- an HTTP worker or a thrift handler now that a security
        // integration serves those channels too. LDAPGroupProvider sets the same two properties.
        env.put("com.sun.jndi.ldap.connect.timeout",
                String.valueOf(Config.authentication_ldap_simple_conn_timeout_ms));
        env.put("com.sun.jndi.ldap.read.timeout",
                String.valueOf(Config.authentication_ldap_simple_conn_read_timeout_ms));
        if (useSSL) {
            setSSLContext(env);
        }
        return env;
    }

    private static void closeQuietly(DirContext ctx) {
        if (ctx != null) {
            try {
                ctx.close();
            } catch (Exception e) {
                // ignore
>>>>>>> 7cb1013 ([BugFix] Authenticate security integration (LDAP) users on the non-MySQL channels (#79165))
            }
        }
        throw lastException;
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
            // Normalize username for case-insensitive LDAP search
            // LDAP treats usernames as case-insensitive by default, aligning with Microsoft Active Directory
            String normalizedUser = normalizeUsername(user);
            // Escapes special characters in user input to prevent LDAP injection
            String safeUser = escapeLdapValue(normalizedUser);
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

    /**
     * Escape special characters in a value used in an LDAP search filter (RFC 4515).
     * Prevents LDAP filter injection by escaping: \ * ( ) \0
     */
    public static String escapeLdapValue(String value) {
        if (value == null) {
            return null;
        }

        value = value.replace("\\", "\\5c");
        value = value.replace("*", "\\2a");
        value = value.replace("(", "\\28");
        value = value.replace(")", "\\29");
        value = value.replace("|", "\\7c");
        value = value.replace("\u0000", "\\00");
        return value;
    }

    /**
     * Escape special characters in a value used in an LDAP Distinguished Name (RFC 4514).
     * Prevents DN injection by escaping: \ , + " < > ;
     * and leading space/#, trailing space.
     */
    public static String escapeDnValue(String value) {
        if (value == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(value.length() + 10);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\':
                case ',':
                case '+':
                case '"':
                case '<':
                case '>':
                case ';':
                    sb.append('\\').append(c);
                    break;
                case '\0':
                    sb.append("\\00");
                    break;
                default:
                    if ((i == 0 && (c == ' ' || c == '#')) ||
                            (i == value.length() - 1 && c == ' ')) {
                        sb.append('\\').append(c);
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }

    /**
     * Normalize username for case-insensitive matching with LDAP/Active Directory.
     * LDAP treats usernames as case-insensitive by default, so we normalize to lowercase
     * to ensure consistent identity mapping regardless of input casing.
     *
     * @param username the original username
     * @return normalized username in lowercase
     */
    public static String normalizeUsername(String username) {
        if (username == null) {
            return null;
        }
        return username.toLowerCase();
    }
}
