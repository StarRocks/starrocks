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
import com.starrocks.common.Config;
import com.starrocks.common.util.NetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import javax.naming.Context;
import javax.naming.InvalidNameException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.PartialResultException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapName;
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
    private final LdapGroupSource ldapGroupSource;
    private final String ldapMemberOfAttr;

    /**
     * Kept so that callers that do not care about group resolution - and the existing tests - do not
     * have to be touched. Behaves exactly as before: groups come from the group providers only.
     */
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
        this(ldapServerHost, ldapServerPort, useSSL, trustStorePath, trustStorePwd, ldapBindRootDN, ldapBindRootPwd,
                ldapBindBaseDN, ldapSearchFilter, ldapUserDN, ldapBindDNPattern,
                LdapGroupSource.GROUP_PROVIDER, "memberOf");
    }

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
                            String ldapBindDNPattern,
                            LdapGroupSource ldapGroupSource,
                            String ldapMemberOfAttr) {
        this.ldapGroupSource = ldapGroupSource == null ? LdapGroupSource.GROUP_PROVIDER : ldapGroupSource;
        this.ldapMemberOfAttr = Strings.isNullOrEmpty(ldapMemberOfAttr) ? "memberOf" : ldapMemberOfAttr;
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
    public void authenticate(AccessControlContext authContext, UserIdentity userIdentity, byte[] authResponse)
            throws AuthenticationException {
        // clear password terminate string: the MySQL clear-password frame appends a trailing \0, while
        // HTTP Basic sends the bare password and may even send an empty one.
        byte[] clearPassword = authResponse;
        if (authResponse.length > 0 && authResponse[authResponse.length - 1] == 0) {
            clearPassword = Arrays.copyOf(authResponse, authResponse.length - 1);
        }

        String user = userIdentity.getUser();
        boolean legacyPerUserDN = !Strings.isNullOrEmpty(ldapUserDN);
        // The legacy `AS \'<dn>\'` form intentionally does not read memberOf, see below.
        boolean readMemberOf = ldapGroupSource.readsMemberOf() && !legacyPerUserDN;
        // The entry's own spelling of the name, wanted only when the session identity is going to be
        // taken from the directory rather than from what the client typed.
        boolean readCanonicalName = Config.authentication_ldap_case_insensitive && !legacyPerUserDN;
        // Empty array means "return no attribute at all"; leaving it unset would make JNDI ask the
        // directory for every attribute of the entry and then throw them away.
        List<String> requested = new ArrayList<>();
        if (readMemberOf) {
            requested.add(ldapMemberOfAttr);
        }
        if (readCanonicalName) {
            requested.add(ldapSearchFilter);
        }
        String[] requestedAttributes = requested.toArray(new String[0]);

        String distinguishedName;
        LdapUserEntry userEntry = null;
        boolean directBind = false;
        try {
            String password = new String(clearPassword, StandardCharsets.UTF_8);
            if (legacyPerUserDN) {
                // Priority 1: per-user DN (from CREATE USER ... AS \'dn\')
                // NOTE: `CREATE USER ... IDENTIFIED WITH authentication_ldap_simple AS \'<dn>\'` is the
                // legacy per-user-DN form and is scheduled for deprecation. It intentionally does NOT
                // support the memberOf group source added later - use a security integration instead.
                distinguishedName = ldapUserDN;
                checkPassword(distinguishedName, password);
            } else if (!Strings.isNullOrEmpty(ldapBindDNPattern)) {
                // Priority 2: direct bind via DN pattern
                directBind = true;
                userEntry = checkPasswordByDnPattern(user, password, requestedAttributes);
                distinguishedName = userEntry.dn();
            } else {
                // Priority 3: search-and-bind. The search that resolves the DN also carries the
                // requested attributes back, so reading memberOf costs no extra request here.
                if (requestedAttributes.length > 0) {
                    userEntry = findUserEntryByRoot(user, requestedAttributes);
                    distinguishedName = userEntry.dn();
                } else {
                    // Original signature on the default path - it is the documented entry point and
                    // the one the existing tests intercept.
                    distinguishedName = findUserDNByRoot(user);
                }
                checkPassword(distinguishedName, password);
            }
            Preconditions.checkNotNull(distinguishedName);

            // set distinguished name to auth context
            authContext.setDistinguishedName(distinguishedName);
        } catch (AuthenticationException e) {
            // Already classified (empty password, user not found, or a nested bind failure): pass it through.
            LOG.warn("check password failed for user: {}", user, e);
            throw e;
        } catch (javax.naming.AuthenticationException e) {
            // The directory answered and rejected the credential -- a definitive "wrong password".
            LOG.warn("check password failed for user: {}", user, e);
            throw new AuthenticationException(e.getMessage());
        } catch (Exception e) {
            // Anything else means we could not get an answer: connection refused, timeout, TLS failure.
            // Mark it transient so a caller that caches rejections does not hold a valid credential back
            // until the directory recovers.
            LOG.warn("cannot reach the directory while authenticating user: {}", user, e);
            throw new AuthenticationException(e.getMessage()).asTransient();
        }

        if (readCanonicalName) {
            authContext.setAuthenticatedUserName(
                    userEntry == null ? null : readSearchAttrValue(userEntry.attributes()));
        }

        if (readMemberOf) {
            // Authentication already succeeded at this point. A group-resolution failure must never
            // turn into an authentication failure, so everything below is contained.
            try {
                authContext.setMemberOfGroups(
                        resolveMemberOfGroups(user, distinguishedName,
                                userEntry == null ? null : userEntry.attributes(), directBind));
            } catch (Exception e) {
                LOG.warn("failed to resolve memberOf groups for user: {}, dn: {}", user, distinguishedName, e);
                authContext.setMemberOfGroups(Set.of());
            }
        } else {
            // Write the field in this branch too, so it always states what *this* authentication
            // resolved instead of keeping whatever it happened to hold. Leaving it untouched would
            // make correctness depend on "nobody wrote it earlier", which is exactly the kind of
            // assumption that breaks when the field gets reused somewhere else later.
            authContext.setMemberOfGroups(Set.of());
        }
    }

    /**
     * Read the entry's own spelling of the attribute the search filtered on, e.g. `uid` or
     * `sAMAccountName`. That is the authoritative form of the name; anything StarRocks derives on its
     * own is a guess at a spelling the directory may not hold.
     *
     * @return null when the attribute was not returned or is not textual
     */
    private String readSearchAttrValue(Attributes attributes) {
        if (attributes == null) {
            return null;
        }
        try {
            Attribute attribute = attributes.get(ldapSearchFilter);
            if (attribute == null || attribute.size() == 0) {
                return null;
            }
            Object value = attribute.get(0);
            return value instanceof String ? (String) value : null;
        } catch (NamingException e) {
            LOG.debug("cannot read '{}' back from the user entry", ldapSearchFilter, e);
            return null;
        }
    }

    /**
     * @return true if the configured group providers take part in this login's group set.
     * <p>
     * False means `memberof`, and then they are not merely dropped from the result - they are not
     * called at all: calling and discarding would pay for a read we said we do not want, and a broken
     * group provider would still be able to affect a mode that explicitly excludes it.
     */
    public boolean isGroupProviderUsed() {
        return ldapGroupSource.usesGroupProvider();
    }

    /**
     * @return true if the groups read from the user's own entry take part in this login's group set.
     * The counterpart of {@link #isGroupProviderUsed()}: both sides of the union are gated by the
     * configured group source, so neither can silently keep contributing after the other is turned
     * off.
     */
    public boolean isMemberOfUsed() {
        return ldapGroupSource.readsMemberOf();
    }

    /**
     * Resolve the groups of a user we are *not* authenticating - the target of `EXECUTE AS`.
     * <p>
     * Impersonation never presents the target's password, so this path can only work with the
     * service account, and that is enough: reading somebody else's attributes never needed their
     * credential. How the DN is obtained differs by mode, and neither way needs a password:
     * <ul>
     *     <li>search-and-bind: one search as the service account returns the DN and the attribute
     *     together - the same request a login makes;</li>
     *     <li>`bind_dn_pattern`: the DN is computed from the pattern, then read as the service
     *     account. With several patterns configured we cannot know which one is the real entry
     *     without binding, so they are tried in the configured order and the first one that answers
     *     wins.</li>
     * </ul>
     *
     * @return the resolved group names, empty when this mode/configuration cannot resolve them - an
     * empty result is never an error here, it just means the caller keeps the group providers only
     */
    public TargetUserGroups resolveMemberOfGroupsForUser(String user) {
        if (!ldapGroupSource.readsMemberOf()) {
            // Nothing to read, and deliberately no request: the default configuration must not pay
            // for a directory round trip it does not need.
            return TargetUserGroups.NONE;
        }
        if (!Strings.isNullOrEmpty(ldapUserDN)) {
            // The legacy per-user-DN form does not support memberOf at all, see authenticate().
            return TargetUserGroups.NONE;
        }
        if (!canProbeWithServiceAccount()) {
            LOG.info("cannot resolve {} for user {} without authenticating it: no service account " +
                            "({}, {}) is configured", ldapMemberOfAttr, user,
                    SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_DN,
                    SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_PWD);
            return TargetUserGroups.NONE;
        }

        String[] requested = new String[] {ldapMemberOfAttr};
        try {
            if (!Strings.isNullOrEmpty(ldapBindDNPattern)) {
                // On the login path the password decides which of several candidate DNs is the right
                // entry. Here there is no password, so a name that exists under more than one pattern
                // is genuinely ambiguous - two different people can share a uid across two OUs, which
                // is exactly why multiple patterns exist. Handing out the first one that answers would
                // give the target another person's groups and, through GRANT ... TO EXTERNAL GROUP,
                // their roles. Resolve only when exactly one candidate answers.
                String resolvedDn = null;
                Attributes resolvedAttributes = null;
                List<String> collisions = new ArrayList<>();
                for (String dn : candidateDNsFromPattern(user)) {
                    try {
                        Attributes attributes = readAttributesAsServiceAccount(dn, requested);
                        collisions.add(dn);
                        if (resolvedDn == null) {
                            resolvedDn = dn;
                            resolvedAttributes = attributes;
                        }
                    } catch (Exception e) {
                        LOG.debug("cannot read {} at dn '{}' for user '{}': {}",
                                ldapMemberOfAttr, dn, user, e.getMessage());
                    }
                }
                if (collisions.size() > 1) {
                    LOG.warn("refusing to resolve {} for user {} without authenticating it: the name exists " +
                                    "under more than one bind dn pattern ({}), and no password is available to " +
                                    "tell them apart. Falling back to the group providers only.",
                            ldapMemberOfAttr, user, collisions);
                    return TargetUserGroups.NONE;
                }
                if (resolvedDn == null) {
                    return TargetUserGroups.NONE;
                }
                return new TargetUserGroups(resolvedDn,
                        LDAPMemberOfExtractor.extractGroupNames(resolvedAttributes, ldapMemberOfAttr, user));
            } else {
                LdapUserEntry entry = findUserEntryByRoot(user, requested);
                return new TargetUserGroups(entry.dn(),
                        LDAPMemberOfExtractor.extractGroupNames(entry.attributes(), ldapMemberOfAttr, user));
            }
        } catch (Exception e) {
            LOG.warn("failed to resolve {} for user {} with the service account: {}",
                    ldapMemberOfAttr, user, e.getMessage());
            return TargetUserGroups.NONE;
        }
    }

    /**
     * What could be learned about a user without authenticating it.
     *
     * @param distinguishedName the resolved DN, or null when it could not be established. Worth
     *                          passing on: a group provider without `ldap_user_search_attr` keys its
     *                          cache by DN, and on this path the caller would otherwise only have a
     *                          login name to offer it.
     * @param memberOfGroups    the groups read from the user's own entry, possibly empty
     */
    public record TargetUserGroups(String distinguishedName, Set<String> memberOfGroups) {
        private static final TargetUserGroups NONE = new TargetUserGroups(null, Set.of());
    }

    public LdapGroupSource getGroupSource() {
        return ldapGroupSource;
    }

    public String getMemberOfAttr() {
        return ldapMemberOfAttr;
    }

    /**
     * Turn the attributes read during authentication into group names, falling back to a service
     * account probe when the user could not read the attribute on its own.
     */
    private Set<String> resolveMemberOfGroups(String user, String dn, Attributes attributes, boolean directBind) {
        Set<String> groups = LDAPMemberOfExtractor.extractGroupNames(attributes, ldapMemberOfAttr, user);
        if (!groups.isEmpty()) {
            return groups;
        }

        // Nothing came back. On the direct-bind path the reader was the logging-in user itself, and a
        // directory may forbid a user from reading its own group membership - retry once as the
        // service account rather than forcing the customer to change authentication mode.
        // The decision looks only at what memberOf returned: the group providers have not run yet at
        // this point, so their result is not visible here by construction.
        if (directBind && canProbeWithServiceAccount()) {
            try {
                Attributes probed = readAttributesAsServiceAccount(dn, new String[] {ldapMemberOfAttr});
                groups = LDAPMemberOfExtractor.extractGroupNames(probed, ldapMemberOfAttr, user);
                LOG.info("probed {} for user {} with the service account, resolved {} group(s)",
                        ldapMemberOfAttr, user, groups.size());
            } catch (Exception e) {
                LOG.warn("failed to probe {} for user {} (dn: {}) with the service account",
                        ldapMemberOfAttr, user, dn, e);
            }
        }

        if (groups.isEmpty()) {
            // Two very different situations look identical on the wire - the user really belongs to no
            // group, or the directory does not publish the attribute at all - so say both out loud.
            // Without the hint this reads as a StarRocks bug, while on OpenLDAP it is usually a
            // missing memberof overlay.
            LOG.warn("resolved no group from attribute '{}' for user {} (dn: {}){}: either the user belongs to " +
                            "no group, or the directory does not maintain this attribute - OpenLDAP only does " +
                            "with the memberof overlay loaded, and a directory may also forbid reading it",
                    ldapMemberOfAttr, user, dn,
                    directBind && !canProbeWithServiceAccount()
                            ? " and no service account is configured to probe with" : "");
        }
        return groups;
    }

    private boolean canProbeWithServiceAccount() {
        return !Strings.isNullOrEmpty(ldapBindRootDN) && !Strings.isNullOrEmpty(ldapBindRootPwd);
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

    /**
     * Validate every configured DN pattern and substitute the user into each of them.
     * <p>
     * The patterns are validated up front, before any of them is used, so a typo in the second
     * pattern is reported even when the first one would have worked.
     *
     * @return the candidate DNs, in the configured order
     */
    private String[] candidateDNsFromPattern(String user) throws AuthenticationException {
        String safeUser = escapeDnValue(normalizeUsername(user));
        String[] rawPatterns = ldapBindDNPattern.split(";");
        String[] dns = new String[rawPatterns.length];
        for (int i = 0; i < rawPatterns.length; i++) {
            String pattern = trim(trim(rawPatterns[i].trim(), "\""), "'");
            if (!pattern.contains("${USER}")) {
                throw new AuthenticationException(
                        "Invalid bind DN pattern: '" + pattern + "' does not contain ${USER} placeholder. " +
                        "Each pattern segment must include ${USER} to prevent shared-DN authentication bypass.");
            }
            if (!pattern.contains("=")) {
                throw new AuthenticationException(
                        "Invalid bind DN pattern: '" + pattern + "' is not a valid DN format. " +
                        "Pattern must produce a Distinguished Name (e.g., 'uid=${USER},ou=People,dc=example,dc=com'). " +
                        "UPN-style patterns like '${USER}@domain' are not supported.");
            }
            dns[i] = pattern.replace("${USER}", safeUser);
        }
        return dns;
    }

    /**
     * Expand the semicolon-separated bind DN pattern and check the password against each candidate DN
     * in order, returning the first entry that accepts it - the pattern loop over
     * {@link #checkPassword} and {@link #checkPasswordAndReadAttributes}. An empty
     * `requestedAttributes` means only check the password, which is what the default path does.
     */
    protected LdapUserEntry checkPasswordByDnPattern(String user, String password, String[] requestedAttributes)
            throws Exception {
        Exception lastException = null;
        for (String dn : candidateDNsFromPattern(user)) {
            try {
                if (requestedAttributes.length == 0) {
                    checkPassword(dn, password);
                    return new LdapUserEntry(dn, null);
                }
                return checkPasswordAndReadAttributes(dn, password, requestedAttributes);
            } catch (Exception e) {
                lastException = e;
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
            }
        }
    }

    //bind to ldap server to check password
    protected void checkPassword(String dn, String password) throws Exception {
        checkPasswordAndReadAttributes(dn, password, new String[0]);
    }

    /**
     * Bind as the user and, on the same connection, read the requested attributes.
     * <p>
     * A bind response cannot carry attributes, so on this path reading memberOf costs one extra read
     * request - but no extra connection and no extra bind. The read has to happen between the bind
     * and closing the context, which is why it lives in this method rather than in a caller.
     * <p>
     * A failure of the attribute read is swallowed: the bind - the actual authentication - already
     * succeeded, and the caller falls back to the service account probe.
     *
     * @param requestedAttributes attributes to read, empty for none (then this is a plain bind)
     */
    protected LdapUserEntry checkPasswordAndReadAttributes(String dn, String password, String[] requestedAttributes)
            throws Exception {
        if (Strings.isNullOrEmpty(password)) {
            throw new AuthenticationException("empty password is not allowed for simple authentication");
        }

        Hashtable<String, String> env = buildEnv(dn, password);

        DirContext ctx = null;
        try {
            //this will send a bind call to ldap server, throw exception if failed
            ctx = new InitialDirContext(env);
            if (requestedAttributes.length == 0) {
                return new LdapUserEntry(dn, null);
            }
            try {
                // LdapName, not the String overload: getAttributes(String, ...) parses its argument
                // as a JNDI *composite* name, where '/' separates components. A DN may legally
                // contain '/' (AD's `CN=Smith/Jones,...` is common), and escapeDnValue() does not
                // escape it, so the String form would split the DN and fail with NameNotFoundException.
                return new LdapUserEntry(dn, ctx.getAttributes(new LdapName(dn), requestedAttributes));
            } catch (Exception e) {
                LOG.warn("bound as {} but failed to read attributes {}: {}",
                        dn, Arrays.toString(requestedAttributes), e.getMessage());
                return new LdapUserEntry(dn, null);
            }
        } finally {
            closeQuietly(ctx);
        }
    }

    /**
     * Read attributes of `dn` with the configured service account. Only used as the fallback probe of
     * the direct-bind path, so it is the single place that pays for an extra connection and bind.
     */
    protected Attributes readAttributesAsServiceAccount(String dn, String[] requestedAttributes) throws Exception {
        String rootDN = trim(trim(ldapBindRootDN, "\""), "'");
        Hashtable<String, String> env = buildEnv(rootDN, ldapBindRootPwd);
        DirContext ctx = null;
        try {
            ctx = new InitialDirContext(env);
            // LdapName for the same reason as in checkPasswordAndReadAttributes().
            return ctx.getAttributes(new LdapName(dn), requestedAttributes);
        } finally {
            closeQuietly(ctx);
        }
    }

    //1. bind ldap server by root dn
    //2. search user
    //3. if match exactly one, return the user's actual DN
    protected String findUserDNByRoot(String user) throws Exception {
        return findUserEntryByRoot(user, new String[0]).dn();
    }

    /**
     * Same as {@link #findUserDNByRoot(String)}, but the search also brings back the requested
     * attributes. On this path reading memberOf is free: the search of the user entry has to happen
     * anyway, so the attribute name is simply added to the ones it returns.
     *
     * @param requestedAttributes attributes to return. An empty array means "no attribute at all",
     *                            which is what the default path wants - leaving it unset would make
     *                            the directory return every attribute of the entry, only for all of
     *                            them to be discarded.
     */
    protected LdapUserEntry findUserEntryByRoot(String user, String[] requestedAttributes) throws Exception {
        if (Strings.isNullOrEmpty(ldapBindRootPwd)) {
            throw new AuthenticationException("empty password is not allowed for simple authentication");
        }

        //dn contains '=', so we should use ' or " to wrap the value in config file
        String rootDN = ldapBindRootDN;
        rootDN = trim(rootDN, "\"");
        rootDN = trim(rootDN, "'");
        Hashtable<String, String> env = buildEnv(rootDN, ldapBindRootPwd);

        DirContext ctx = null;
        try {
            String baseDN = ldapBindBaseDN;
            baseDN = trim(baseDN, "\"");
            baseDN = trim(baseDN, "'");
            SearchControls sc = new SearchControls();
            sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
            sc.setReturningAttributes(requestedAttributes);
            // Normalize username for case-insensitive LDAP search
            // LDAP treats usernames as case-insensitive by default, aligning with Microsoft Active Directory
            String normalizedUser = normalizeUsername(user);
            // Escapes special characters in user input to prevent LDAP injection
            String safeUser = escapeLdapValue(normalizedUser);
            String searchFilter = "(" + ldapSearchFilter + "=" + safeUser + ")";
            ctx = new InitialDirContext(env);
            NamingEnumeration<SearchResult> results = ctx.search(baseDN, searchFilter, sc);

            String userDN = null;
            Attributes userAttributes = null;
            int matched = 0;

            try {
                while (results.hasMore()) {
                    matched++;
                    if (matched > 1) {
                        throw new AuthenticationException("searched more than one entry from ldap server for user " + user);
                    }

                    SearchResult result = results.next();
                    userDN = result.getNameInNamespace();
                    userAttributes = result.getAttributes();
                }
            } catch (PartialResultException e) {
                LOG.warn("ldap search partial result exception", e);
            }

            if (matched != 1) {
                throw new AuthenticationException("ldap search matched user count " + matched);
            }

            return new LdapUserEntry(userDN, userAttributes);
        } finally {
            closeQuietly(ctx);
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
        // Locale.ROOT, never the default locale: under a Turkish locale 'I' lowercases to the dotless
        // 'i', which would silently change ASCII user names such as 'Li'.
        return username.toLowerCase(Locale.ROOT);
    }

    /**
     * Canonicalize an LDAP distinguished name so that two DNs which denote the same entry map to the
     * same string. {@link LdapName} takes care of the RFC 4514 syntax (separator whitespace, escaping,
     * attribute type case), and the extra lowercase covers attribute values, which LDAP and Active
     * Directory also compare without regard to case.
     *
     * @param dn the distinguished name, may be null
     * @return the canonical form, or a best-effort lowercase when the input is not a parsable DN
     */
    public static String canonicalDn(String dn) {
        if (dn == null) {
            return null;
        }
        try {
            // Rebuilding the name from its parsed RDNs is what actually normalizes it: an LdapName that
            // was constructed from a string hands that same string back from toString(), whitespace
            // around the separators included.
            LdapName parsed = new LdapName(dn);
            return new LdapName(parsed.getRdns()).toString().toLowerCase(Locale.ROOT);
        } catch (InvalidNameException e) {
            // Expected whenever the distinguished name falls back to the bare user name, so this stays
            // at debug level; lowercase is the best we can do and matches the previous behaviour.
            LOG.debug("'{}' is not a valid LDAP distinguished name, fall back to lowercase", dn);
            return dn.toLowerCase(Locale.ROOT);
        }
    }
}
