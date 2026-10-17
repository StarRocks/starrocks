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

<<<<<<< HEAD
=======
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
>>>>>>> dc172843477... [Feature] [PRE-574] Read a security integration user's groups from a claim of the verified token (#63504)
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.ErrorCode;
import com.starrocks.mysql.MysqlCodec;
import com.starrocks.mysql.privilege.AuthPlugin;

import java.io.ByteArrayOutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class OAuth2AuthenticationProvider implements AuthenticationProvider {
    public static final String OAUTH2_AUTH_SERVER_URL = "auth_server_url";
    public static final String OAUTH2_TOKEN_SERVER_URL = "token_server_url";
    public static final String OAUTH2_REDIRECT_URL = "redirect_url";
    public static final String OAUTH2_CLIENT_ID = "client_id";
    public static final String OAUTH2_CLIENT_SECRET = "client_secret";
    public static final String OAUTH2_JWKS_URL = "jwks_url";
    public static final String OAUTH2_PRINCIPAL_FIELD = "principal_field";
    public static final String OAUTH2_REQUIRED_ISSUER = "required_issuer";
    public static final String OAUTH2_REQUIRED_AUDIENCE = "required_audience";
    public static final String OAUTH2_CONNECT_WAIT_TIMEOUT = "connect_wait_timeout";

    private final OAuth2Context oAuth2Context;
    private final GroupSource groupSource;
    private final String groupClaim;
    private final String integrationName;

    // A provider instance is built per login (SecurityIntegration#getAuthenticationProvider), so
    // none of the fields below is shared between connections. They are volatile because one
    // connection is served by more than one thread: the handshake, the statements, and - for the
    // MySQL authorization-code flow - the HTTP thread of the /api/oauth2 callback.

    // True once this provider holds a token whose signature was checked: by verifyBearer, or by the
    // callback through acceptVerifiedToken.
    private volatile boolean tokenVerified = false;
    // Groups the handshake could not decide yet (isGroupResolutionDeferred); run once by
    // checkLoginSuccess when the token is in.
    private volatile DeferredGroupResolution deferredGroupResolution;
    // Once the deferred resolution refused the login, every later statement is refused with it too.
    private volatile AuthenticationException groupResolutionFailure;

    public OAuth2AuthenticationProvider(OAuth2Context oAuth2Context) {
        this(oAuth2Context, GroupSource.GROUP_PROVIDER, null, null);
    }

    public OAuth2AuthenticationProvider(OAuth2Context oAuth2Context, GroupSource groupSource,
                                        String groupClaim, String integrationName) {
        this.oAuth2Context = oAuth2Context;
        this.groupSource = groupSource == null ? GroupSource.GROUP_PROVIDER : groupSource;
        this.groupClaim = groupClaim;
        this.integrationName = integrationName;
    }

    @Override
    public void authenticate(AccessControlContext authContext, UserIdentity userIdentity, byte[] authResponse)
            throws AuthenticationException {
        /*
          If the auth plugin used by the client for this authentication is not AUTHENTICATION_OAUTH2_CLIENT,
          then the authentication success will be directly returned.
          However, this is not a true authentication success, the user cannot perform any operations on this connection.
          Any operation will return an auth url to guide the user to perform oauth2 authentication.
          This is because in many scenarios, the client cannot directly launch a web browser.
         */
        if (!AuthPlugin.Client.AUTHENTICATION_OAUTH2_CLIENT.toString().equals(authContext.getAuthPlugin())) {
            return;
        }

        long startTime = System.currentTimeMillis();
        String token;
        while (true) {
            token = authContext.getAuthToken();
            if (token != null) {
                break;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (System.currentTimeMillis() - startTime > oAuth2Context.connectWaitTimeout() * 1000) {
                break;
            }
        }

        if (token == null) {
            throw new AuthenticationException("OAuth2 authentication wait callback timeout");
        }
<<<<<<< HEAD
=======
        return userIdentity;
    }

    /**
     * Verify a token handed over directly (HTTP / Arrow Flight Bearer, or the MySQL OpenID Connect
     * handshake frame). The user name the token is checked against is the declared one when there is
     * one: a native OAuth2 user logging in over MySQL declared its name in the handshake, and the
     * token's principal claim must be that name, exactly as {@code OAuth2Action} requires of the
     * browser-flow callback and {@link JWTAuthenticationProvider} of its tokens. Without that binding
     * any valid token from the same IdP would log in as whichever OAuth2 user the client named.
     * Only when nobody declared a user (plain Bearer) does the principal claim become the user.
     */
    private UserIdentity verifyBearer(AccessControlContext authContext, UserIdentity userIdentity, String idToken,
                                      String remoteHost) throws AuthenticationException {
        String userName = (userIdentity != null) ? userIdentity.getUser()
                : OpenIdConnectVerifier.extractPrincipal(idToken, oAuth2Context.principalFiled());
        try {
            JWKSource<SecurityContext> jwkSource =
                    GlobalStateMgr.getCurrentState().getJwkMgr().getJwkSource(oAuth2Context.jwksUrl());
            JWTClaimsSet claims = OpenIdConnectVerifier.verify(idToken, userName, jwkSource,
                    oAuth2Context.principalFiled(), oAuth2Context.requiredIssuer(),
                    oAuth2Context.requiredAudience());
            // Only after the signature holds - see JWTAuthenticationProvider for why.
            if (groupSource.usesAuthenticatorGroups()) {
                authContext.setAuthenticatorGroups(ClaimGroupExtractor.extract(claims, groupClaim, integrationName));
            }
            tokenVerified = true;
            authContext.setAuthToken(idToken);
        } catch (AuthenticationException e) {
            throw e;
        } catch (Exception e) {
            throw new AuthenticationException(e.getMessage());
        }
        return (userIdentity != null) ? userIdentity : UserIdentity.createEphemeralUserIdent(userName, remoteHost);
    }

    /**
     * Take the id token the {@code /api/oauth2} callback of the MySQL authorization-code flow has
     * just verified. The caller has checked the signature, issuer, audience and principal; this
     * reads the groups out of it the same way {@link #verifyBearer} does, and only then publishes the
     * token.
     *
     * <p>The order is what makes the groups visible to the connection: this runs on the callback's
     * HTTP thread, and the connection learns the token has arrived by reading the volatile
     * {@code authToken} - either the handshake waiting in {@link #authenticateMysqlBrowserFlow} or
     * {@link #checkLoginSuccess} on the next statement. Everything written before that volatile
     * write is visible to whoever reads it.
     */
    public void acceptVerifiedToken(AccessControlContext authContext, String idToken, JWTClaimsSet claims) {
        if (groupSource.usesAuthenticatorGroups()) {
            authContext.setAuthenticatorGroups(ClaimGroupExtractor.extract(claims, groupClaim, integrationName));
        }
        tokenVerified = true;
        authContext.setAuthToken(idToken);
    }

    @Override
    public boolean isGroupProviderUsed() {
        return groupSource.usesGroupProvider();
    }

    @Override
    public boolean isAuthenticatorGroupsUsed() {
        return groupSource.usesAuthenticatorGroups() && tokenVerified;
    }

    /**
     * Deferred exactly when the groups are to be read from a token this provider does not hold yet.
     * That is the stock-client branch of the MySQL authorization-code flow, whose handshake returns
     * before the browser login. The branch for a client with the OAuth2 plugin waits in the
     * handshake until the callback has delivered the token, so its groups are known in time; so are
     * Bearer logins. An integration that only uses group providers keeps deciding the groups - and
     * the permitted_groups gate - on the handshake, as before: they need nothing but the user name,
     * and the gate then refuses a user before the browser login rather than after it.
     */
    @Override
    public boolean isGroupResolutionDeferred() {
        return groupSource.usesAuthenticatorGroups() && !tokenVerified;
    }

    @Override
    public void deferGroupResolution(DeferredGroupResolution resolution) {
        this.deferredGroupResolution = resolution;
    }

    @Override
    public boolean supportsCredentialKind(AuthCredentialKind kind) {
        // The chain may hand this provider a scramble too: an ordinary MySQL client that could not
        // speak the OAuth2 plugin, connecting through an OAuth2 security integration. It gets the
        // same treatment as a native OAuth2 user on that client - the handshake is answered and
        // the first statement carries the authorization URL - so the two kinds of OAuth2 user are
        // one experience. The chain is consulted for names in no user table, so this answers the
        // handshake for any name; what makes that acceptable is that such a connection can do
        // nothing until the callback binds a token to the name (ConnectProcessor refuses every
        // command, not only statements) and is closed by connect_wait_timeout if the token never
        // arrives (ConnectContext#checkTimeout) - the same bound the plugin-aware client's
        // handshake waits for.
        return kind == AuthCredentialKind.MYSQL_NATIVE_SCRAMBLE
                || kind == AuthCredentialKind.MYSQL_OAUTH2_CLIENT
                || kind == AuthCredentialKind.BEARER_TOKEN;
>>>>>>> dc172843477... [Feature] [PRE-574] Read a security integration user's groups from a claim of the verified token (#63504)
    }

    @Override
    public byte[] authMoreDataPacket(AccessControlContext authContext, String user, String host) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] bytes = oAuth2Context.authServerUrl().getBytes(StandardCharsets.UTF_8);
        MysqlCodec.writeInt2(outputStream, bytes.length);
        MysqlCodec.writeBytes(outputStream, bytes);

        bytes = oAuth2Context.clientId().getBytes(StandardCharsets.UTF_8);
        MysqlCodec.writeInt2(outputStream, bytes.length);
        MysqlCodec.writeBytes(outputStream, bytes);

        bytes = oAuth2Context.redirectUrl().getBytes(StandardCharsets.UTF_8);
        MysqlCodec.writeInt2(outputStream, bytes.length);
        MysqlCodec.writeBytes(outputStream, bytes);

        return outputStream.toByteArray();
    }

    @Override
    public byte[] authSwitchRequestPacket(AccessControlContext authContext, String user, String host) {
        return authMoreDataPacket(authContext, user, host);
    }

    @Override
    public void checkLoginSuccess(int connectionId, AccessControlContext context) throws AuthenticationException {
        if (context.getAuthToken() == null) {
            String authUrl = oAuth2Context.authServerUrl() +
                    "?response_type=code" +
                    "&client_id=" + URLEncoder.encode(oAuth2Context.clientId(), StandardCharsets.UTF_8) +
                    "&redirect_uri=" + URLEncoder.encode(oAuth2Context.redirectUrl(), StandardCharsets.UTF_8) +
                    "&state=" + connectionId +
                    "&scope=openid";

            throw new AuthenticationException(ErrorCode.ERR_OAUTH2_NOT_AUTHENTICATED, authUrl);
        }
        if (groupResolutionFailure != null) {
            throw groupResolutionFailure;
        }
        DeferredGroupResolution resolution = deferredGroupResolution;
        if (resolution != null) {
            // Cleared only once it has decided, so the connection can never get past this point with
            // the gate unevaluated: an unexpected exception leaves it in place for the next statement.
            try {
                resolution.resolve();
                deferredGroupResolution = null;
            } catch (AuthenticationException e) {
                groupResolutionFailure = e;
                deferredGroupResolution = null;
                throw e;
            }
        }
    }

    public OAuth2Context getoAuth2Context() {
        return oAuth2Context;
    }
}