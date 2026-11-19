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
import com.nimbusds.jwt.SignedJWT;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.Pair;
import com.starrocks.metric.MetricRepo;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AuthenticationHandler {
    private static final Logger LOG = LogManager.getLogger(AuthenticationHandler.class);

    /**
     * Authenticate a request described by a protocol-neutral {@link AuthCredentials}.
     *
     * <p>Dispatch is by credential shape:
     * <ul>
     *   <li>{@code BEARER_TOKEN}: routed by the token's {@code iss} claim (there is no declared
     *       user to look up natively), so the native lookup returns nothing and the chain
     *       carries on to the integrations.</li>
     *   <li>{@code MYSQL_NATIVE_SCRAMBLE} / {@code PLAIN_PASSWORD}: native lookup first, then the
     *       security integration chain as fallback. Native first avoids making internal users
     *       wait on, e.g., an OAuth2 round-trip.</li>
     * </ul>
     *
     * @return the authenticated user identity
     * @throws AuthenticationException when authentication fails
     */
    public static UserIdentity authenticate(ConnectContext context, AuthCredentials cred) throws AuthenticationException {
        // Single point for auth observability: every channel funnels through here, so the counters
        // cannot miss an entry point the way per-entry-point instrumentation would (MetricRepo
        // COUNTER_AUTH_*).
        try {
            AuthenticationResult result = authenticateInternal(context, cred);
            // A provider may answer "OK" without having verified anything, expecting the protocol
            // to finish the handshake later: OAuth2AuthenticationProvider returns immediately when
            // the client did not declare the OAuth2 plugin, and the MySQL channel only closes that
            // hole on the first command (ConnectProcessor#checkLoginSuccess). The non-MySQL
            // channels have no later command -- the request is served right here -- so the same
            // check has to happen before we hand the identity back.
            // The test is on the channel, not the kind: MySQL now produces PLAIN_PASSWORD and
            // BEARER_TOKEN too, and its OAuth2 handshake has a kind of its own, so any kind-based
            // test here would run this check inline on a MySQL connection whose token has not
            // arrived yet and fail the handshake.
            if (cred.channel() != AuthChannel.MYSQL) {
                AuthenticationProvider provider = context.getAuthenticationProvider();
                if (provider != null) {
                    provider.checkLoginSuccess(context.getConnectionId(), context.getAccessControlContext());
                }
            }
            // When the groups and the permitted_groups gate are deferred (a stock MySQL client on an
            // OAuth2 integration that reads a claim), the handshake has not decided the login yet:
            // the deferred step records the final verdict, so a user the gate refuses is counted
            // as a failure and not as a success followed by nothing.
            if (!result.groupResolutionDeferred) {
                recordAuthSuccess(cred);
            }
            return result.authenticatedUser;
        } catch (AuthenticationException e) {
            recordAuthFailure(cred, e);
            throw e;
        }
    }

    private static void recordAuthSuccess(AuthCredentials cred) {
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_AUTH_SUCCESS.getMetric(cred.channelLabel(), cred.kindLabel()).increase(1L);
        }
    }

    private static void recordAuthFailure(AuthCredentials cred, AuthenticationException e) {
        AuthFailureReason reason = (e.getReason() == null) ? AuthFailureReason.BAD_CREDENTIAL : e.getReason();
        recordAuthFailure(cred, reason);
    }

    /**
     * Counts a credential the caller could not even parse (a Basic header that is not Base64, or
     * that names no user) as a {@link AuthFailureReason#MALFORMED} failure. The caller refuses such
     * a request without calling {@link #authenticate}, which otherwise counts every failure.
     */
    public static void recordMalformedCredential(AuthCredentials cred) {
        recordAuthFailure(cred, AuthFailureReason.MALFORMED);
    }

    private static void recordAuthFailure(AuthCredentials cred, AuthFailureReason reason) {
        if (!MetricRepo.hasInit) {
            return;
        }
        MetricRepo.COUNTER_AUTH_FAILURE.getMetric(cred.channelLabel(), cred.kindLabel()).increase(1L);
        MetricRepo.COUNTER_AUTH_FAILURE_REASON.getMetric(reason.label()).increase(1L);
    }

    private static AuthenticationResult authenticateInternal(ConnectContext context, AuthCredentials cred)
            throws AuthenticationException {
        AccessControlContext authContext = context.getAccessControlContext();

        // The context outlives one authentication (COM_CHANGE_USER authenticates again on the same
        // one), and only a provider that resolves groups by itself writes this field. Every
        // authentication starts from an empty set, so whatever this login resolves - or does not -
        // the previous user's authenticator groups are never carried over to the new one.
        authContext.setAuthenticatorGroups(Set.of());

        // Input validation, not dispatch: anything but a token has to name the user it is for.
        // A token does not - the name is inside it.
        if (cred.kind() != AuthCredentialKind.BEARER_TOKEN
                && (cred.userDeclared() == null || cred.userDeclared().isEmpty())) {
            throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, "",
                    cred.rawSecret().length == 0 ? "NO" : "YES");
        }

        // Native first, then the security integration chain. There is no branch for Bearer here on
        // purpose: a Bearer request declares no user, so the native lookup has nothing to look up
        // and says so itself. Giving it its own top-level path would read as "tokens are an SI-only
        // concept", which is not true - a native user can carry a JWT plugin and does so over
        // MySQL.
        AuthenticationResult authenticationResult = authenticateWithNative(authContext, cred);
        if (authenticationResult == null) {
            authenticationResult = authenticateWithSecurityIntegration(authContext, cred);
        }

        if (authenticationResult == null) {
            // Nothing claimed the credential. For a token that means no integration was tried for
            // its issuer - a configuration gap, which the metric should not file under bad
            // credentials.
            throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL,
                    cred.userDeclared() == null ? "" : cred.userDeclared(),
                    cred.rawSecret().length == 0 ? "NO" : "YES")
                    .withReason(cred.kind() == AuthCredentialKind.BEARER_TOKEN
                            ? AuthFailureReason.NO_MATCHING_SI : AuthFailureReason.BAD_CREDENTIAL);
        }

        setAuthenticationResultToContext(context, authenticationResult, cred);
        return authenticationResult;
    }

    /**
     * Forget every remembered rejection. The cache is process-wide, so tests must reset it between cases; an
     * operator can achieve the same by flipping {@code authentication_failure_cache_ttl_second} (any change to
     * the TTL or the capacity rebuilds the cache).
     */
    public static void invalidateRejectedCredentialCache() {
        RejectedCredentialCache.invalidateAll();
    }

    private static AuthenticationResult authenticateWithNative(AccessControlContext authContext, AuthCredentials cred)
            throws AuthenticationException {
        String user = cred.userDeclared();
        if (user == null || user.isEmpty()) {
            // Nothing to look up. Returning null rather than throwing keeps this a statement about
            // the native table alone and lets the chain carry on to the integrations.
            return null;
        }
        String remoteHost = cred.remoteHost();
        boolean noPassword = cred.rawSecret().length == 0;
        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();

        Map.Entry<UserIdentity, UserAuthenticationInfo> matchedUserIdentity =
                authenticationMgr.getBestMatchedUserIdentityForLogin(user, remoteHost);
        if (matchedUserIdentity == null) {
            if (Config.enable_auth_check) {
                LOG.debug("cannot find user {}@{}", user, remoteHost);
                return null;
            } else {
                LOG.info("enable_auth_check is false, but cannot find user '{}'@'{}'", user, remoteHost);
                throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, user, noPassword ? "NO" : "YES");
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
                throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, user, noPassword ? "NO" : "YES");
            }
            authContext.setAuthenticationProvider(provider);

            if (Config.enable_auth_check) {
                //Throw an exception directly and feedback to the client
                UserIdentity verified = provider.authenticate(authContext, matchedUserIdentity.getKey(), cred);
                // The session is going to be the matched row's user, so the provider must have
                // verified exactly that user: a result naming somebody else (a token whose
                // principal is not this user), or no result at all, must not open it.
                if (verified == null || !verified.getUser().equalsIgnoreCase(user)) {
                    LOG.warn("provider {} authenticated '{}' while the client declared '{}'@'{}'",
                            provider.getClass().getSimpleName(), verified == null ? null : verified.getUser(),
                            user, remoteHost);
                    throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL, user, noPassword ? "NO" : "YES");
                }
            }

            return new AuthenticationResult(matchedUserIdentity.getKey(), List.of(Config.group_provider), null, "native");
        }
    }


    /**
     * Remembers credentials the security integration chain has just rejected, so a client stuck on
     * a wrong password does not turn every retry into an LDAP bind. Two properties make this safe
     * to have on by default:
     *
     * <ul>
     *   <li>The key includes a hash of the credential, so it only short-circuits <em>the same wrong
     *       credential</em>. The moment the client sends a different password -- or the operator
     *       fixes the one in the client -- the key changes and the chain runs for real; there is no
     *       stale-rejection window.</li>
     *   <li>Only an outright credential rejection is cached. A failure caused by the directory
     *       itself being unreachable is not, so a recovering directory is retried immediately
     *       instead of after the TTL.</li>
     * </ul>
     *
     * <p>What it does not do: an attacker cycling usernames produces a new key every time and still
     * reaches the directory. That is a different problem (a flood, not an account lockout) and
     * wants a different control -- bind timeouts bound each attempt, and nothing here lets one
     * account be locked out by repeated retries.
     *
     * <p>The credential is never stored: the key holds a SHA-256 over a per-process random salt
     * plus the credential bytes, so the cache cannot be read back into passwords and the hashes
     * differ across FEs.
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
                // binding against the one directory account this cache exists to protect. A Bearer
                // credential declares no user, and null stays null.
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
                                                                            AuthCredentials cred)
            throws AuthenticationException {
        List<Pair<String, AuthenticationException>> exceptions = Lists.newArrayList();
        AuthenticationResult authenticationResult = null;
        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();

        // Parsed once, before the loop: every integration asks the same question of the same token.
        // Reading the issuer without verifying the signature is safe because it only prunes - a
        // forged issuer routes the token to an integration whose signature check then fails.
        String tokenIssuer = null;
        if (cred.kind() == AuthCredentialKind.BEARER_TOKEN) {
            // The switch is about the non-MySQL Bearer entry points. MySQL's OpenID Connect
            // handshake also arrives as BEARER_TOKEN now and predates the switch, so gating it
            // here would turn the flag into a MySQL regression.
            if (cred.channel() != AuthChannel.MYSQL && !Config.enable_rest_bearer_authentication) {
                throw new AuthenticationException(
                        "Bearer token authentication is disabled (enable_rest_bearer_authentication=false)")
                        .withReason(AuthFailureReason.DISABLED);
            }
            try {
                tokenIssuer = SignedJWT.parse(cred.tokenString()).getJWTClaimsSet().getIssuer();
            } catch (ParseException e) {
                throw new AuthenticationException("Malformed JWT").withReason(AuthFailureReason.MALFORMED);
            }
        }

        // This chain is the only path that reaches out to a directory, so it is the only one worth
        // short-circuiting: a client retrying the same rejected credential must not produce a bind per attempt.
        // An OAuth2-aware MySQL client carries no credential: whether its handshake succeeds depends
        // on the user finishing the browser flow, not on any bytes it sent. Remembering a timeout as
        // a rejected credential would refuse the retry the user makes right after authorizing, for
        // the whole TTL.
        boolean cacheable = cred.kind() != AuthCredentialKind.MYSQL_OAUTH2_CLIENT;
        String cachedRejection = cacheable
                ? RejectedCredentialCache.getRejection(cred.userDeclared(), cred.rawSecret()) : null;
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

            // A token needs one more filter than a password does. A password names the user it is
            // for, so the chain can just try each integration that consumes passwords; a token
            // names nobody, and which key verifies it depends on who issued it - hence the issuer
            // check. It only prunes: an integration that declares no required_issuer is tried, and
            // the signature check decides.
            if (cred.kind() == AuthCredentialKind.BEARER_TOKEN
                    && !issuerAllows(securityIntegration, tokenIssuer)) {
                continue;
            }

            UserIdentity authenticatedUser;
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
                // The provider says what it can read; that is the whole filter. It replaces the
                // client-plugin comparison this loop used to do, which only ever worked on MySQL
                // (ctx.authPlugin is filled by the handshake and nothing else) and was the reason
                // a security-integration user got a 401 on every other channel.
                if (!provider.supportsCredentialKind(cred.kind())) {
                    continue;
                }
                authContext.setAuthenticationProvider(provider);
                // Cleared per attempt: a value left by an earlier provider in the chain says nothing
                // about the one that ends up succeeding.
                authContext.setAuthenticatedUserName(null);
                // Hand over the declared identity when there is one. This is a security control,
                // not a convenience: a token provider that receives an identity checks the
                // principal claim against it, and one that receives null takes the claim as the
                // user name. Deciding this on the kind instead would drop that binding for MySQL
                // JWT users, whose credential is a token but who did declare a name.
                UserIdentity declared = (cred.userDeclared() == null)
                        ? null
                        : UserIdentity.createEphemeralUserIdent(cred.userDeclared(), cred.remoteHost());
                authenticatedUser = provider.authenticate(authContext, declared, cred);
            } catch (AuthenticationException e) {
                // A directory or IdP that could not be reached is not a wrong credential: the same
                // credential will be valid again as soon as the service recovers, so it must not
                // be remembered as rejected, and it is counted as internal rather than bad.
                if (e.isTransientFailure()) {
                    directoryUnusable = true;
                    e.withReason(AuthFailureReason.INTERNAL);
                }
                exceptions.add(new Pair<>(authMechanism, e));
                continue;
            } catch (RuntimeException e) {
                LOG.warn("security integration {} is unusable: {}", authMechanism, e.getMessage(), e);
                directoryUnusable = true;
                exceptions.add(new Pair<>(authMechanism,
                        new AuthenticationException(e.getMessage()).withReason(AuthFailureReason.INTERNAL)));
                continue;
            }

            // getGroupProviderName() returns an empty list -- never null -- when the integration declares no
            // group_provider, so an == null test would never fall back and the integration would resolve no
            // groups at all. An integration without its own provider falls back to the instance-wide one.
            List<String> groupProviders = securityIntegration.getGroupProviderName();
            if (groupProviders == null || groupProviders.isEmpty()) {
                groupProviders = List.of(Config.group_provider);
            }

            // This user has no entry in the user table, so the name the client typed is the only thing
            // StarRocks would otherwise identify it by - and for LDAP that name carries whatever casing
            // the client felt like using. Settle on one spelling here, once, so the session, the audit
            // log and current_user() all show the same identity. The directory's own spelling is the
            // authoritative one; lowercase is only the fallback for the direct-bind path, which never
            // searches and so never sees the entry.
            if (isLdapProvider(authContext.getAuthenticationProvider())
                    && Config.authentication_ldap_case_insensitive) {
                String fromDirectory = authContext.getAuthenticatedUserName();
                String settled = fromDirectory != null
                        ? fromDirectory : LDAPAuthProvider.normalizeUsername(authenticatedUser.getUser());
                authenticatedUser = UserIdentity.createEphemeralUserIdent(settled, cred.remoteHost());
            }

            authenticationResult = new AuthenticationResult(
                    authenticatedUser,
                    groupProviders,
                    securityIntegration.getGroupAllowedLoginList(),
                    authMechanism);
        }

        if (authenticationResult == null && !exceptions.isEmpty()) {
            String message = Joiner.on(", ").join(exceptions.stream().map(e -> e.first + ": " + e.second.getMessage())
                    .collect(Collectors.toList()));
            // Do not remember a failure the directory caused: it would delay recovery by the whole TTL.
            if (!directoryUnusable && cacheable) {
                RejectedCredentialCache.remember(cred.userDeclared(), cred.rawSecret(), message);
            }
            throw new AuthenticationException(ErrorCode.ERR_AUTHENTICATION_FAIL_IN_AUTH_CHAIN, message)
                    .withReason(sharedReason(exceptions));
        }

        return authenticationResult;
    }

    /**
     * The reason to report when every integration that was tried failed: theirs when they agree,
     * which is the usual case (a token is tried by the one integration that knows its issuer), and
     * the default otherwise, because promoting one integration's reason would misdescribe the rest.
     */
    private static AuthFailureReason sharedReason(List<Pair<String, AuthenticationException>> exceptions) {
        AuthFailureReason first = exceptions.get(0).second.getReason();
        for (Pair<String, AuthenticationException> e : exceptions) {
            if (e.second.getReason() != first) {
                return AuthFailureReason.BAD_CREDENTIAL;
            }
        }
        return first;
    }

    /**
     * Whether an integration may be tried for a token with this issuer.
     *
     * <p>An integration that declares no {@code required_issuer} is allowed through: the issuer is
     * a pruning hint, not a routing key, so the signature check is what decides. Reading it as
     * "unset means never match" would be the old routing-key model, and after the MySQL OpenID
     * Connect frame started arriving as a token it would lock out every MySQL JWT user whose
     * integration left the property unset - a property that has always been optional there.
     *
     * <p>Because it only prunes, several integrations may share an issuer and differ by audience;
     * the chain tries them in order and the first one that verifies wins, exactly as the password
     * side works.
     */
    private static boolean issuerAllows(SecurityIntegration securityIntegration, String iss) {
        String[] requiredIssuers = securityIntegration.getRequiredIssuer();
        if (requiredIssuers == null || requiredIssuers.length == 0) {
            return true;
        }
        return Arrays.asList(requiredIssuers).contains(iss);
    }

    private static void setAuthenticationResultToContext(ConnectContext context,
                                                         AuthenticationResult authenticationResult,
                                                         AuthCredentials cred)
            throws AuthenticationException {
        AuthChannel channel = cred.channel();
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

        // Steps 4 and 5 decide the session's groups, its roles and the permitted_groups gate. They
        // normally run now. A provider whose groups come from a credential that has not arrived yet
        // - the OAuth2 authorization-code flow over MySQL, where the token comes through the browser
        // callback - takes them over instead and runs them before the first statement executes.
        AuthenticationProvider provider = context.getAuthenticationProvider();
        if (provider != null && provider.isGroupResolutionDeferred()) {
            context.setGroups(new HashSet<>());
            context.setCurrentRoleIds(authenticationResult.authenticatedUser, Set.of());
            authenticationResult.groupResolutionDeferred = true;
            // The metrics record the verdict here rather than on the handshake, see authenticate().
            provider.deferGroupResolution(() -> {
                try {
                    resolveGroupsAndCheckAccess(context, authenticationResult, channel);
                    recordAuthSuccess(cred);
                } catch (AuthenticationException e) {
                    recordAuthFailure(cred, e);
                    throw e;
                }
            });
        } else {
            resolveGroupsAndCheckAccess(context, authenticationResult, channel);
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

    /**
     * Steps 4 and 5 of {@link #setAuthenticationResultToContext}: the session's groups, the roles
     * derived from them, and the permitted_groups gate. Separate so a provider can run it later on
     * the same connection, see {@link AuthenticationProvider#isGroupResolutionDeferred()}.
     */
    private static void resolveGroupsAndCheckAccess(ConnectContext context, AuthenticationResult authenticationResult,
                                                    AuthChannel channel) throws AuthenticationException {
        String user = authenticationResult.authenticatedUser.getUser();

        // Step 4: union the two group sources - the configured group providers, and what the
        // authenticator resolved by itself (LDAP `memberOf`, or a claim of the verified JWT). It has
        // to happen here, before roles are derived and before the permitted_groups gate, so both
        // sources take part in both. Each side is gated on the group source the provider reports.
        AccessControlContext accessControlContext = context.getAccessControlContext();
        boolean fromProviders = isGroupProviderUsed(accessControlContext);
        boolean fromAuthenticator = isAuthenticatorGroupsUsed(accessControlContext);
        Set<String> providerGroups = fromProviders
                ? resolveGroupsFromProviders(context.getCurrentUserIdentity(),
                        context.getDistinguishedName(), authenticationResult.groupProviderName)
                : Set.of();
        Set<String> authenticatorGroups = fromAuthenticator
                ? accessControlContext.getAuthenticatorGroups() : Set.of();
        Set<String> groups = new HashSet<>(providerGroups);
        groups.addAll(authenticatorGroups);
        context.setGroups(groups);

        // Both sources at once is the only case where `current_group()` alone cannot say where a
        // group came from - with a single source the integration's own `group_source` answers it.
        // So say it here rather than inventing a SQL surface for the attribution.
        //
        // The level depends on how often the channel authenticates, because the same line is either
        // once per session or once per request: MySQL and Arrow Flight authenticate when the session
        // opens, so one INFO line per login is what an operator wants; HTTP and the load RPCs
        // authenticate on every single request, and an INFO line there would be one log line per
        // query on a busy cluster. On those, the attribution is available by turning on DEBUG for
        // this class.
        if (fromProviders && fromAuthenticator) {
            boolean oncePerSession = channel == AuthChannel.MYSQL || channel == AuthChannel.ARROW_FLIGHT;
            String message = "authenticated {} via {}: group_source=both, from group providers {}, " +
                    "from authenticator {} -> effective {}";
            if (oncePerSession) {
                LOG.info(message, user, authenticationResult.securityIntegration,
                        providerGroups, authenticatorGroups, groups);
            } else if (LOG.isDebugEnabled()) {
                LOG.debug(message, user, authenticationResult.securityIntegration,
                        providerGroups, authenticatorGroups, groups);
            }
        }

        // Set current role IDs based on the authenticated user and groups
        context.setCurrentRoleIds(authenticationResult.authenticatedUser, groups);

        // Step 5: Validate group access permissions
        // `permitted_groups` is a gate, not a filter: the user is refused unless at least one of its
        // groups is on the list, but the group set itself is left whole - a user admitted by one
        // group keeps all of them, and they all take part in role mapping and Ranger.
        if (authenticationResult.authenticatedGroupList != null && !authenticationResult.authenticatedGroupList.isEmpty()) {
            // Compared through GroupNameMatch, the same predicate the group-to-role lookup uses
            // (AuthorizationMgr#getRoleIdListByGroup), so the gate and the role mapping cannot
            // disagree on whether case counts. The group names themselves are never rewritten -
            // they are handed to Ranger, which matches case-sensitively.
            List<String> allowedGroups = authenticationResult.authenticatedGroupList;
            boolean allowed = groups.stream().anyMatch(group ->
                    allowedGroups.stream().anyMatch(allowedGroup -> GroupNameMatch.equal(group, allowedGroup)));
            if (!allowed) {
                throw new AuthenticationException(ErrorCode.ERR_GROUP_ACCESS_DENY, user,
                        Joiner.on(",").join(groups),
                        Joiner.on(",").join(authenticationResult.authenticatedGroupList));
            }
        }
    }

    private static class AuthenticationResult {
        private final UserIdentity authenticatedUser;
        private final List<String> groupProviderName;
        private final List<String> authenticatedGroupList;
        private final String securityIntegration;
        // Set when the groups and the permitted_groups gate were handed to the provider to run
        // later (AuthenticationProvider#isGroupResolutionDeferred): the login is not decided yet.
        private boolean groupResolutionDeferred;

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
     * Whether the configured group providers contribute to this login's group set. Only a provider
     * that resolves groups by itself can turn them off (`group_source = authenticator`), and the
     * question is asked of the provider rather than of the config, which would disable them for
     * native users too.
     */
    private static boolean isGroupProviderUsed(AccessControlContext accessControlContext) {
        AuthenticationProvider provider = accessControlContext.getAuthenticationProvider();
        if (provider == null) {
            return true;
        }
        // enable_auth_check = false skips provider.authenticate() above, so the authenticator never
        // read its own groups: letting `authenticator` suppress the providers too would leave the
        // login with no groups at all.
        return !Config.enable_auth_check || provider.isGroupProviderUsed();
    }

    /**
     * The mirror of {@link #isGroupProviderUsed}. A provider that resolves no groups of its own
     * leaves the field empty and answers false by default, so this is belt and braces - but it keeps
     * the two sources from drifting apart.
     */
    private static boolean isAuthenticatorGroupsUsed(AccessControlContext accessControlContext) {
        AuthenticationProvider provider = accessControlContext.getAuthenticationProvider();
        return provider != null && provider.isAuthenticatorGroupsUsed();
    }

    /**
     * Whether the credentials were checked against an LDAP directory. {@link LDAPAuthProvider} is the
     * only LDAP provider in this code base, so a single instanceof is enough.
     */
    private static boolean isLdapProvider(AuthenticationProvider provider) {
        return provider instanceof LDAPAuthProvider;
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
