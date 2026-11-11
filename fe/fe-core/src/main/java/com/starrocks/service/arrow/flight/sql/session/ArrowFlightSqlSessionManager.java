// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.service.arrow.flight.sql.session;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;
import com.nimbusds.jwt.SignedJWT;
import com.starrocks.authentication.AuthChannel;
import com.starrocks.authentication.AuthCredentials;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectScheduler;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.arrow.flight.sql.ArrowFlightSqlConnectContext;
import com.starrocks.system.Frontend;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.text.ParseException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Sessions on the Arrow Flight SQL port. Two kinds of token appear here and are named apart: a
 * <b>session token</b> is the UUID (or {@code FE_HOST|UUID} with the proxy on) this FE issues for a
 * session - it is what {@code tokenCache} holds and what every RPC after the first presents; a
 * <b>JWT</b> is an IdP-issued token this FE only verifies, and only ever sees on the first RPC of a
 * connection, because the client adopts the session token handed back in the response headers just
 * as it does after a Basic handshake. Plain {@code token} means either.
 */
public class ArrowFlightSqlSessionManager {

    private final LoadingCache<String, ArrowFlightSqlTokenInfo> tokenCache;

    public ArrowFlightSqlSessionManager() {
        this.tokenCache = CacheBuilder.newBuilder()
                .maximumSize(Math.min(Config.arrow_token_cache_size, Config.qe_max_connection))
                .expireAfterWrite(Config.arrow_token_cache_expire_second, TimeUnit.SECONDS)
                .removalListener((RemovalNotification<String, ArrowFlightSqlTokenInfo> notification) -> {
                    ArrowFlightSqlConnectContext context =
                            ExecuteEnv.getInstance().getScheduler().getArrowFlightSqlConnectContext(notification.getKey());
                    if (context != null) {
                        context.kill(true, "token is expired or evicted");
                    }
                })
                .build(new CacheLoader<>() {
                    @NotNull
                    @Override
                    public ArrowFlightSqlTokenInfo load(@NotNull String key) {
                        // Since we only use add, getIfPresent, expired and evicted function of cache,
                        // this method should never be invoked.
                        return ArrowFlightSqlTokenInfo.createInvalidTokenInfo();
                    }
                });
    }

    public String initializeSession(String username, String remoteIP, String password) {
        // Arrow Flight Basic carries a plaintext password (PLAIN_PASSWORD), not a MySQL handshake
        // frame; naming the kind is what lets a security-integration LDAP user authenticate here
        // too.
        return initializeSession(AuthCredentials.ofPassword(username, password, AuthChannel.ARROW_FLIGHT, remoteIP));
    }

    /**
     * Open a session from an IdP-issued JWT, the same way {@link #initializeSession(String, String, String)}
     * opens one from a user name and password: the JWT is authenticated through the central handler
     * (issuer routing plus signature verification) and exchanged for a session token, which the
     * client uses from then on. One connection, one session, released by CloseSession - as for Basic.
     *
     * <p>The one difference is the deadline. A JWT says how long its holder may be trusted, and a
     * session that outlived it would be a cache extending a credential; so the session inherits the
     * JWT's {@code exp} on top of the configured {@code arrow_token_cache_expire_second} ceiling,
     * and {@link #validateToken} refuses it past that point. A JWT without {@code exp} sets no
     * deadline of its own.
     */
    public String initializeSessionWithJwt(String jwt, String remoteIP) {
        return initializeSession(AuthCredentials.ofBearer(jwt, AuthChannel.ARROW_FLIGHT, remoteIP), () -> {
            // Read only after the handler accepted the JWT, so this never trusts an unverified claim.
            Date exp = expirationOf(jwt);
            return exp == null ? Long.MAX_VALUE : exp.getTime();
        });
    }

    private static Date expirationOf(String jwt) {
        try {
            return SignedJWT.parse(jwt).getJWTClaimsSet().getExpirationTime();
        } catch (ParseException e) {
            return null;
        }
    }

    private String initializeSession(AuthCredentials cred) {
        return initializeSession(cred, () -> Long.MAX_VALUE);
    }

    private String initializeSession(AuthCredentials cred, java.util.function.LongSupplier deadlineAfterAuth) {
        String sessionToken = generateToken();
        ArrowFlightSqlConnectContext ctx = new ArrowFlightSqlConnectContext(sessionToken);
        ctx.setRemoteIP(cred.remoteHost());

        try {
            AuthenticationHandler.authenticate(ctx, cred);
        } catch (AuthenticationException e) {
            throw CallStatus.UNAUTHENTICATED
                    .withDescription("Access denied")
                    .withCause(e)
                    .toRuntimeException();
        }

        ArrowFlightSqlTokenInfo tokenInfo = new ArrowFlightSqlTokenInfo(ctx.getCurrentUserIdentity(), sessionToken,
                deadlineAfterAuth.getAsLong());

        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));

        // Assign connection ID
        ConnectScheduler connectScheduler = ExecuteEnv.getInstance().getScheduler();
        try {
            ctx.setConnectionId(connectScheduler.getNextConnectionId());
        } catch (ConnectScheduler.ConnectionIdExhaustedException e) {
            ctx.kill(true, "failed to allocate connection ID");
            throw CallStatus.RESOURCE_EXHAUSTED
                    .withDescription("failed to allocate connection ID: " + e.getMessage())
                    .withCause(e)
                    .toRuntimeException();
        }
        ctx.resetConnectionStartTime();

        Pair<Boolean, String> isSuccessAndErrorMsg = connectScheduler.registerConnection(ctx);
        if (!isSuccessAndErrorMsg.first) {
            String errorMsg = isSuccessAndErrorMsg.second;
            ctx.getState().setError(errorMsg);
            ctx.kill(true, "failed to register connection: " + errorMsg);
            throw CallStatus.RESOURCE_EXHAUSTED
                    .withDescription("failed to register connection: " + errorMsg)
                    .toRuntimeException();
        }

        // Only a session that fully registered gets a token: a token left behind for a failed
        // registration would validate against a context that was killed.
        tokenCache.put(sessionToken, tokenInfo);
        return sessionToken;
    }

    public void validateToken(String sessionToken) throws IllegalArgumentException {
        if (StringUtils.isEmpty(sessionToken)) {
            throw new IllegalArgumentException("bearer token is empty");
        }

        ArrowFlightSqlTokenInfo tokenInfo = tokenCache.getIfPresent(sessionToken);
        if (tokenInfo == null) {
            throw new IllegalArgumentException(String.format("invalid bearer token [%s], please try to reconnect. " +
                    "Maybe the token is expired or evicted, could modify fe.conf " +
                    "[arrow_token_cache_expire_second] and [arrow_token_cache_size]", sessionToken));
        }
        if (tokenInfo.isExpiredAt(System.currentTimeMillis())) {
            // The JWT that opened this session has expired; the removal listener kills the context.
            tokenCache.invalidate(sessionToken);
            throw new IllegalArgumentException(String.format(
                    "bearer token [%s] is no longer valid: the credential it was opened with has expired, " +
                            "please authenticate again", sessionToken));
        }
    }

    public void closeSession(String sessionToken) {
        tokenCache.invalidate(sessionToken);
    }

    @NotNull
    public ArrowFlightSqlConnectContext validateAndGetConnectContext(String sessionToken) throws FlightRuntimeException {
        ArrowFlightSqlConnectContext connectContext =
                ExecuteEnv.getInstance().getScheduler().getArrowFlightSqlConnectContext(sessionToken);
        if (connectContext == null) {
            throw CallStatus.NOT_FOUND
                    .withDescription("cannot find connect arrow context of the token [" + sessionToken + "]")
                    .toRuntimeException();
        }
        return connectContext;
    }

    public boolean isLocalToken(String sessionToken) {
        if (!GlobalVariable.isArrowFlightProxyEnabled()) {
            return true;  // Without proxy, all tokens are local
        }
        String feHost = extractFeHost(sessionToken);
        if (feHost == null) {
            return true;  // Legacy token format (no host prefix)
        }
        String selfHost = GlobalStateMgr.getCurrentState().getNodeMgr().getSelfNode().first;
        return feHost.equals(selfHost);
    }

    /**
     * Extract the FE host from a token.
     * Token format when proxy enabled: "FE_HOST|UUID"
     * Uses '|' as delimiter to support IPv6 addresses (which contain ':').
     * Returns null if token doesn't contain host prefix.
     */
    public static String extractFeHost(String token) {
        if (StringUtils.isEmpty(token)) {
            return null;
        }
        int separatorIndex = token.indexOf('|');
        if (separatorIndex <= 0) {
            return null;  // No host prefix or invalid format
        }
        return token.substring(0, separatorIndex);
    }

    /**
     * Validate that a host is a known FE in the cluster.
     * This prevents forwarding requests to arbitrary/malicious hosts.
     */
    public static boolean isValidFeHost(String host) {
        if (StringUtils.isEmpty(host)) {
            return false;
        }
        for (Frontend fe : GlobalStateMgr.getCurrentState().getNodeMgr().getFrontends(null)) {
            if (host.equals(fe.getHost())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Generate a token. When proxy is enabled, includes FE host prefix.
     * Format: "FE_HOST|UUID" (proxy enabled) or "UUID" (proxy disabled)
     * Uses '|' as delimiter to support IPv6 addresses (which contain ':').
     */
    private String generateToken() {
        String uuid = UUIDUtil.genUUID().toString();
        if (GlobalVariable.isArrowFlightProxyEnabled()) {
            String selfHost = GlobalStateMgr.getCurrentState().getNodeMgr().getSelfNode().first;
            return selfHost + "|" + uuid;
        }
        return uuid;
    }
}
