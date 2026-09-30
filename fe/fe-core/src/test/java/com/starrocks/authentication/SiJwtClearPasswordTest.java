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
import com.starrocks.common.Config;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.NoOpEditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * A channel that negotiates no authentication plugin of its own -- Arrow Flight SQL, the HTTP REST API, the
 * BE to FE load RPCs -- declares mysql_clear_password and carries the whole credential in the password field.
 * That is also how such a client presents an id token, so a JWT security integration has to be reachable
 * there; the openid_connect client plugin it would otherwise have to declare exists only inside the MySQL
 * handshake.
 * <p>
 * The companion constraint is that a password must never be handed to a JWT verifier, which
 * {@link SiLdapClearPasswordTest#testClearPasswordSkipsTokenBasedSecurityIntegration} pins. Both hold because
 * the token integration is considered only for a credential shaped like a compact JWS.
 */
public class SiJwtClearPasswordTest {
    private static final String JWT_SI = "jwt_corp";

    private final MockTokenUtils mockTokenUtils = new MockTokenUtils();

    private AuthenticationMgr authenticationMgr;
    private String[] savedAuthChain;
    private AuthenticationMgr savedAuthenticationMgr;
    private EditLog savedEditLog;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @BeforeEach
    public void setUp() throws Exception {
        savedEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        savedAuthenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        GlobalStateMgr.getCurrentState().setEditLog(new NoOpEditLog());
        authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        GlobalStateMgr.getCurrentState().setJwkMgr(new MockTokenUtils.MockJwkMgr());
        UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);

        savedAuthChain = Config.authentication_chain;
        AuthenticationHandler.invalidateRejectedCredentialCache();
        registerJwtSecurityIntegration(JWT_SI);
        Config.authentication_chain = new String[] {"native", JWT_SI};
    }

    @AfterEach
    public void tearDown() {
        Config.authentication_chain = savedAuthChain;
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(savedAuthenticationMgr);
        GlobalStateMgr.getCurrentState().setEditLog(savedEditLog);
    }

    private void registerJwtSecurityIntegration(String name) {
        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY,
                AuthPlugin.Server.AUTHENTICATION_JWT.name());
        properties.put(JWTAuthenticationProvider.JWT_JWKS_URL, "jwks.json");
        properties.put(JWTAuthenticationProvider.JWT_PRINCIPAL_FIELD, "preferred_username");
        authenticationMgr.replayCreateSecurityIntegration(name, properties);
    }

    @Test
    public void testIdTokenOnAClearPasswordChannelAuthenticates() throws Exception {
        String idToken = mockTokenUtils.generateTestOIDCToken(3600 * 1000);
        ConnectContext context = new ConnectContext();

        UserIdentity user = AuthenticationHandler.authenticateWithClearPassword(
                context, "harbor", "10.1.2.3", idToken);

        Assertions.assertNotNull(user);
        Assertions.assertEquals("harbor", user.getUser());
        Assertions.assertEquals(JWT_SI, context.getSecurityIntegration());
        // The verified token has to reach the context: IcebergRESTCatalog reads it back to authenticate the
        // catalog call as this user, which is the point of carrying a per-user JWT on these channels at all.
        Assertions.assertEquals(idToken, context.getAuthToken());
    }

    @Test
    public void testPasswordOnAClearPasswordChannelDoesNotReachTheVerifier() {
        // Not shaped like a compact JWS, so the integration is never selected and the chain reports the
        // ordinary "no user" failure rather than a token verification error.
        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class,
                () -> AuthenticationHandler.authenticateWithClearPassword(
                        new ConnectContext(), "harbor", "10.1.2.3", "a-plain-password"));
        Assertions.assertFalse(e.getMessage().contains(JWT_SI), e.getMessage());
    }

    @Test
    public void testForgedTokenOnAClearPasswordChannelIsRejected() throws Exception {
        String forged = mockTokenUtils.getOpenIdConnect("fake-oidc.json");

        AuthenticationException e = Assertions.assertThrows(AuthenticationException.class,
                () -> AuthenticationHandler.authenticateWithClearPassword(
                        new ConnectContext(), "harbor", "10.1.2.3", forged));
        // It was selected and verified, and verification is what refused it.
        Assertions.assertTrue(e.getMessage().contains(JWT_SI), e.getMessage());
    }

    @Test
    public void testMysqlOpenIdConnectPathStillReadsTheFramedToken() throws Exception {
        String idToken = mockTokenUtils.generateTestOIDCToken(3600 * 1000);
        java.io.ByteArrayOutputStream framed = new java.io.ByteArrayOutputStream();
        com.starrocks.mysql.MysqlCodec.writeInt1(framed, 1);
        com.starrocks.mysql.MysqlCodec.writeLenEncodedString(framed, idToken);

        ConnectContext context = new ConnectContext();
        context.setAuthPlugin(AuthPlugin.Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString());

        UserIdentity user = AuthenticationHandler.authenticate(
                context, "harbor", "10.1.2.3", framed.toByteArray());

        Assertions.assertEquals("harbor", user.getUser());
        Assertions.assertEquals(idToken, context.getAuthToken());
    }
}
