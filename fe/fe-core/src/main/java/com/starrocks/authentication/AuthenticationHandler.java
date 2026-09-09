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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.Pair;
import com.starrocks.epack.authentication.LDAPAuthProviderForExternal;
import com.starrocks.epack.authentication.LDAPSecurityIntegration;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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

    /**
     * Authenticate a request that carries a cleartext password instead of a MySQL handshake response.
     *
     * <p>Entry point for the non-MySQL channels: HTTP Basic (REST / Stream Load / transaction API), the
     * BE-&gt;FE load RPCs and the Arrow Flight Basic handshake. Those channels build a fresh
     * {@link ConnectContext} whose {@code authPlugin} is unset, while the security integration chain filters
     * candidates by exactly that field. With no plugin declared every integration was skipped, so a user
     * defined through a security integration could log in over the MySQL protocol but got a 401 everywhere
     * else.
     *
     * <p>Declaring {@code mysql_clear_password} is the same statement the MySQL protocol makes when it
     * switches an LDAP user to that plugin: the client sends its password in the clear. It lets the chain
     * match the integrations that consume a password (LDAP) and keep skipping the token based ones
     * (JWT / OAuth2), whose client plugins differ.
     *
     * @param context    the context to authenticate onto; it also receives the resolved groups and the roles
     *                   they map to, so a caller that authorizes afterwards must reuse this same context
     * @param user       username sent by the client
     * @param remoteHost remote host address
     * @param password   cleartext password sent by the client
     * @return authenticated user identity
     * @throws AuthenticationException when authentication fails
     */
    public static UserIdentity authenticateWithClearPassword(ConnectContext context, String user, String remoteHost,
                                                             String password)
            throws AuthenticationException {
        context.setAuthPlugin(AuthPlugin.Client.MYSQL_CLEAR_PASSWORD.toString());
        UserIdentity authenticatedUser = authenticate(context, user, remoteHost, password.getBytes(StandardCharsets.UTF_8));

        // A provider may answer "OK" without having verified anything, expecting the protocol to finish the
        // handshake later: OAuth2AuthenticationProvider returns immediately when the client did not declare the
        // OAuth2 plugin, and the MySQL channel only closes that hole on the first command
        // (ConnectProcessor#checkLoginSuccess). These channels have no later command -- the request is served
        // right here -- so the same check has to happen before we hand the identity back.
        AuthenticationProvider provider = context.getAuthenticationProvider();
        if (provider != null) {
            provider.checkLoginSuccess(context.getConnectionId(), context.getAccessControlContext());
        }
        return authenticatedUser;
    }

    /**
     * Forget every remembered rejection. The cache is process-wide, so tests must reset it between cases; an
     * operator can achieve the same by flipping {@code authentication_failure_cache_ttl_second} (any change to
     * the TTL or the capacity rebuilds the cache).
     */
    public static void invalidateRejectedCredentialCache() {
        RejectedCredentialCache.invalidateAll();
    }

    private static AuthenticationResult authenticateWithNative(AccessControlContext authContext, String user, String remoteHost,
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

            return new AuthenticationResult(matchedUserIdentity.getKey(), List.of(Config.group_provider), null, "native");
        }
    }


    /**
     * Remembers credentials the security integration chain has just rejected, so a client stuck on a wrong
     * password does not turn every retry into an LDAP bind. Two properties make this safe to have on by default:
     *
     * <ul>
     *   <li>The key includes a hash of the credential, so it only short-circuits <em>the same wrong
     *       credential</em>. The moment the client sends a different password -- or the operator fixes the one
     *       in the client -- the key changes and the chain runs for real; there is no stale-rejection window.</li>
     *   <li>Only an outright credential rejection is cached. A failure caused by the directory itself being
     *       unreachable is not, so a recovering directory is retried immediately instead of after the TTL.</li>
     * </ul>
     *
     * <p>What it does not do: an attacker cycling usernames produces a new key every time and still reaches the
     * directory. That is a different problem (a flood, not an account lockout) and wants a different control --
     * bind timeouts bound each attempt, and nothing here lets one account be locked out by repeated retries.
     *
     * <p>The credential is never stored: the key holds a SHA-256 over a per-process random salt plus the
     * credential bytes, so the cache cannot be read back into passwords and the hashes differ across FEs.
     */
    private static final class RejectedCredentialCache {
        private static final byte[] SALT = new byte[32];

        static {
            new SecureRandom().nextBytes(SALT);
        }

        private static volatile Cache<String, String> cache;
        private static volatile int cachedTtl = -1;
        private static volatile int cachedCapacity = -1;

        private static Cache<String, String> cache() {
            int ttl = Config.authentication_failure_cache_ttl_second;
            int capacity = Config.authentication_failure_cache_capacity;
            Cache<String, String> current = cache;
            // Both settings are mutable at runtime; rebuild when either changes.
            if (current == null || cachedTtl != ttl || cachedCapacity != capacity) {
                synchronized (RejectedCredentialCache.class) {
                    if (cache == null || cachedTtl != ttl || cachedCapacity != capacity) {
                        cache = CacheBuilder.newBuilder()
                                .expireAfterWrite(Math.max(ttl, 0), TimeUnit.SECONDS)
                                .maximumSize(Math.max(capacity, 0))
                                .build();
                        cachedTtl = ttl;
                        cachedCapacity = capacity;
                    }
                    current = cache;
                }
            }
            return current;
        }

        private static String key(String user, byte[] authResponse) {
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                digest.update(SALT);
                digest.update(authResponse);
                // Key on the same canonical username the LDAP paths bind with (LDAPAuthProvider lowercases
                // it, matching Active Directory's case-insensitive accounts). Keying on the raw spelling
                // would let a client miss the cache by varying case -- Alice, ALICE, alice -- and keep
                // binding against the one directory account this cache exists to protect.
                return LDAPAuthProvider.normalizeUsername(user) + '\u0000'
                        + Base64.getEncoder().encodeToString(digest.digest());
            } catch (NoSuchAlgorithmException e) {
                // SHA-256 is mandated by the JLS; treat its absence as "no caching" rather than failing a login.
                return null;
            }
        }

        static String getRejection(String user, byte[] authResponse) {
            if (Config.authentication_failure_cache_ttl_second <= 0) {
                return null;
            }
            String key = key(user, authResponse);
            return key == null ? null : cache().getIfPresent(key);
        }

        static void invalidateAll() {
            Cache<String, String> current = cache;
            if (current != null) {
                current.invalidateAll();
            }
        }

        static void remember(String user, byte[] authResponse, String message) {
            if (Config.authentication_failure_cache_ttl_second <= 0) {
                return;
            }
            String key = key(user, authResponse);
            if (key != null) {
                cache().put(key, message);
            }
        }
    }

    private static AuthenticationResult authenticateWithSecurityIntegration(AccessControlContext authContext,
                                                                            String user,
                                                                            String remoteHost,
                                                                            byte[] authResponse) throws AuthenticationException {
        List<Pair<String, AuthenticationException>> exceptions = Lists.newArrayList();
        AuthenticationResult authenticationResult = null;
        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();

        // This chain is the only path that reaches out to a directory, so it is the only one worth
        // short-circuiting: a client retrying the same rejected credential must not produce a bind per attempt.
        String cachedRejection = RejectedCredentialCache.getRejection(user, authResponse);
        if (cachedRejection != null) {
            throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL_IN_AUTH_CHAIN, cachedRejection);
        }
        boolean directoryUnusable = false;

        String[] authChain = Config.authentication_chain;
        for (String authMechanism : authChain) {
            if (authenticationResult != null) {
                break;
            }

            SecurityIntegration securityIntegration = authenticationMgr.getSecurityIntegration(authMechanism);
            if (securityIntegration == null) {
                continue;
            }

            // Match the integration against the client plugin the caller declared: mysql_clear_password for a
            // cleartext password (MySQL clear-password frame or HTTP Basic), a token plugin for JWT / OAuth2.
            // An integration type without a client-side plugin cannot match, so skip it instead of throwing.
            String expectedClientPlugin = AuthPlugin.covertFromServerToClient(securityIntegration.getType());
            if (expectedClientPlugin == null && !(securityIntegration instanceof LDAPSecurityIntegration)) {
                // The type -> client plugin table is maintained by hand, so a type supported by
                // SecurityIntegrationFactory but missing from it would silently refuse every user of that
                // integration. Say so once per attempt instead of leaving nothing in the log.
                LOG.warn("security integration {} has type {}, which maps to no client-side auth plugin; skipping it",
                        authMechanism, securityIntegration.getType());
                continue;
            }
            if (expectedClientPlugin != null
                    && !expectedClientPlugin.equalsIgnoreCase(authContext.getAuthPlugin())
                    && !(securityIntegration instanceof LDAPSecurityIntegration)) {
                continue;
            }

            try {
                // Building the provider is part of trying this mechanism: it parses the integration's
                // properties (SimpleLDAPSecurityIntegration parses the port with Integer.parseInt), so one
                // misconfigured integration must fail only itself instead of aborting the whole chain -- an
                // unchecked exception escaping here would leave the caller with an HTTP 500 / raw thrift error
                // rather than an authentication failure.
                AuthenticationProvider provider = securityIntegration.getAuthenticationProvider();
                if (provider == null) {
                    LOG.warn("authentication provider is null for security integration: {}", authMechanism);
                    continue;
                }
                authContext.setAuthenticationProvider(provider);
                provider.authenticate(authContext, UserIdentity.createEphemeralUserIdent(user, remoteHost), authResponse);
            } catch (AuthenticationException e) {
                // A provider that could not reach its directory / IdP reports a transient failure: the same
                // credential may well be valid once the service is back, so it must not be remembered.
                directoryUnusable |= e.isTransientFailure();
                exceptions.add(new Pair<>(authMechanism, e));
                continue;
            } catch (RuntimeException e) {
                LOG.warn("security integration {} is unusable: {}", authMechanism, e.getMessage(), e);
                directoryUnusable = true;
                exceptions.add(new Pair<>(authMechanism, new AuthenticationException(e.getMessage())));
                continue;
            }

            // getGroupProviderName() returns an empty list (never null) when the security integration has no
            // `group_provider` property, so falling back to the global default must test isEmpty(), not null.
            List<String> groupProviderNames = securityIntegration.getGroupProviderName().isEmpty()
                    ? List.of(Config.group_provider)
                    : securityIntegration.getGroupProviderName();

            authenticationResult = new AuthenticationResult(
                    UserIdentity.createEphemeralUserIdent(user, remoteHost),
                    groupProviderNames,
                    securityIntegration.getGroupAllowedLoginList(),
                    authMechanism);
        }

        if (authenticationResult == null && !exceptions.isEmpty()) {
            String message = Joiner.on(", ").join(exceptions.stream().map(e -> e.first + ": " + e.second.getMessage())
                    .collect(Collectors.toList()));
            // Do not remember a failure the directory caused: it would delay recovery by the whole TTL.
            if (!directoryUnusable) {
                RejectedCredentialCache.remember(user, authResponse, message);
            }
            throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL_IN_AUTH_CHAIN, message);
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

        // Step 4: union the two group sources - the configured group providers, and what the
        // authentication provider read from the user's own entry (LDAP `memberOf`). It has to happen
        // here, before roles are derived and before the permitted_groups gate, so both sources take
        // part in both. Each side is gated on the group source the provider reports.
        AccessControlContext accessControlContext = context.getAccessControlContext();
        Set<String> groups = new HashSet<>();
        if (isGroupProviderUsed(accessControlContext)) {
            groups.addAll(resolveGroupsFromProviders(context.getCurrentUserIdentity(),
                    context.getDistinguishedName(), authenticationResult.groupProviderName));
        }
        if (isMemberOfUsed(accessControlContext)) {
            groups.addAll(accessControlContext.getMemberOfGroups());
        }
        context.setGroups(groups);

        if (context.getAuthenticationProvider() != null &&
                context.getAuthenticationProvider() instanceof LDAPAuthProviderForExternal) {
            // For LDAP external auth, has set role id in LDAPAuthProviderForExternal#authenticate
        } else {
            // Set current role IDs based on the authenticated user and groups
            context.setCurrentRoleIds(authenticationResult.authenticatedUser, groups);
        }

        // Step 5: Validate group access permissions
        // `permitted_groups` is a gate, not a filter: the user is refused unless at least one of its
        // groups is on the list, but the group set itself is left whole - a user admitted by one
        // group keeps all of them, and they all take part in role mapping and Ranger.
        if (authenticationResult.authenticatedGroupList != null && !authenticationResult.authenticatedGroupList.isEmpty()) {
            // Matched ignoring case: an LDAP `cn` is case-insensitive in the directory, so a group
            // name that differs only in case must not silently fail the gate. The group names
            // themselves are never rewritten - they are handed to Ranger, which matches
            // case-sensitively. The same relaxation is applied to the group-to-role lookup in
            // AuthorizationMgr#getRoleIdListByGroup, and the two must stay in sync.
            Set<String> allowedLowerCase = LDAPMemberOfExtractor.toLowerCaseSet(authenticationResult.authenticatedGroupList);
            boolean allowed = groups.stream()
                    .anyMatch(group -> group != null && allowedLowerCase.contains(group.toLowerCase(Locale.ROOT)));
            if (!allowed) {
                throw new AuthenticationException(ErrorCode.ERR_GROUP_ACCESS_DENY, user, Joiner.on(",").join(groups));
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
