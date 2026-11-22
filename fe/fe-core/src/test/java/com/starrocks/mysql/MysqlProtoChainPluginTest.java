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

package com.starrocks.mysql;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.JWTAuthenticationProvider;
import com.starrocks.authentication.MockTokenUtils;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authentication.SimpleLDAPSecurityIntegration;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.persist.NoOpEditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Which integration on {@code authentication_chain} decides the client plugin of a MySQL handshake
 * when the user is not in the user table. The handshake happens before any credential is seen, so
 * the choice can only follow the plugin the client declared: an LDAP integration and a JWT
 * integration on one chain each serve the clients that declare their plugin, and a client that
 * declares nothing of ours keeps getting the first integration, as before.
 */
public class MysqlProtoChainPluginTest {
    private static final String LDAP_SI = "ldap_si";
    private static final String JWT_SI = "jwt_si";
    private static final String CLEAR = AuthPlugin.Client.MYSQL_CLEAR_PASSWORD.toString();
    private static final String NATIVE = AuthPlugin.Client.MYSQL_NATIVE_PASSWORD.toString();
    private static final String OPENID = AuthPlugin.Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString();

    /** Every packet the server sent during one handshake, so the AuthSwitchRequest can be inspected. */
    private static final List<byte[]> SENT = new ArrayList<>();

    private AuthenticationMgr authenticationMgr;
    private ConnectContext ctx;
    private String[] savedChain;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        new MockUp<MysqlChannel>() {
            @Mock
            public void sendAndFlush(ByteBuffer packet) {
                byte[] bytes = new byte[packet.remaining()];
                packet.get(bytes);
                SENT.add(bytes);
            }

            @Mock
            public ByteBuffer fetchOnePacket() {
                // Whatever the client would answer the switch with; its content is not under test.
                return ByteBuffer.wrap(new byte[] {'p', 'w', 0});
            }
        };
    }

    @BeforeEach
    public void setUp() throws Exception {
        GlobalStateMgr.getCurrentState().setEditLog(new NoOpEditLog());
        GlobalStateMgr.getCurrentState().setJwkMgr(new MockTokenUtils.MockJwkMgr());
        authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        savedChain = Config.authentication_chain;
        SENT.clear();

        Map<String, String> ldap = new HashMap<>();
        ldap.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_ldap_simple");
        ldap.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_HOST, "localhost");
        ldap.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_PORT, "389");
        ldap.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_DN_PATTERN,
                "uid=${USER},ou=People,dc=example,dc=com");
        authenticationMgr.replayCreateSecurityIntegration(LDAP_SI, ldap);

        Map<String, String> jwt = new HashMap<>();
        jwt.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_jwt");
        jwt.put(JWTAuthenticationProvider.JWT_JWKS_URL, "jwks.json");
        jwt.put(JWTAuthenticationProvider.JWT_PRINCIPAL_FIELD, "preferred_username");
        jwt.put(JWTAuthenticationProvider.JWT_REQUIRED_ISSUER, "http://localhost:38080/realms/master");
        authenticationMgr.replayCreateSecurityIntegration(JWT_SI, jwt);
    }

    @AfterEach
    public void tearDown() {
        Config.authentication_chain = savedChain;
    }

    private static void chain(String... names) {
        Config.authentication_chain = names;
    }

    /** Run the handshake for a user not in the user table and return the plugin the packet ends up with. */
    private String negotiate(String user, String declaredPlugin) throws Exception {
        SENT.clear();
        MysqlAuthPacket packet = MysqlAuthPacketTest.buildPacket(user, new byte[20],
                AuthPlugin.Client.valueOf(declaredPlugin.toUpperCase()));
        MysqlProto.switchAuthPlugin(packet, new ConnectContext());
        return packet.getPluginName();
    }

    /** The plugin named by the one AuthSwitchRequest the server sent, or null when it sent none. */
    private static String switchRequested() {
        for (byte[] packet : SENT) {
            if (packet.length > 0 && (packet[0] & 0xff) == 0xfe) {
                int end = 1;
                while (packet[end] != 0) {
                    end++;
                }
                return new String(packet, 1, end - 1, StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    @Test
    public void testDeclaredPluginPicksTheMatchingIntegrationRegardlessOfOrder() {
        chain("native", JWT_SI, LDAP_SI);
        Assertions.assertEquals(LDAP_SI, MysqlProto.pickSecurityIntegration(CLEAR).getName());
        Assertions.assertEquals(JWT_SI, MysqlProto.pickSecurityIntegration(OPENID).getName());

        chain("native", LDAP_SI, JWT_SI);
        Assertions.assertEquals(LDAP_SI, MysqlProto.pickSecurityIntegration(CLEAR).getName());
        Assertions.assertEquals(JWT_SI, MysqlProto.pickSecurityIntegration(OPENID).getName());
    }

    @Test
    public void testUndeclaredClientKeepsGettingTheFirstIntegration() {
        // mysql_native_password is what a client sends when it has no opinion: no integration maps
        // to it, so the first one wins, exactly the behaviour before plugin-aware selection.
        chain("native", JWT_SI, LDAP_SI);
        Assertions.assertEquals(JWT_SI, MysqlProto.pickSecurityIntegration(NATIVE).getName());
        chain("native", LDAP_SI, JWT_SI);
        Assertions.assertEquals(LDAP_SI, MysqlProto.pickSecurityIntegration(NATIVE).getName());
    }

    @Test
    public void testSingleTypeChainIsUnchangedByAForeignDeclaration() {
        // A JWT-only chain still switches a password-declaring client to the OpenID plugin, and an
        // LDAP-only chain still switches a token-declaring client to clear password: nothing a
        // single-type deployment relies on moves.
        chain("native", JWT_SI);
        Assertions.assertEquals(JWT_SI, MysqlProto.pickSecurityIntegration(CLEAR).getName());
        chain("native", LDAP_SI);
        Assertions.assertEquals(LDAP_SI, MysqlProto.pickSecurityIntegration(OPENID).getName());
    }

    @Test
    public void testMissingIntegrationOnTheChainIsSkipped() {
        chain("native", "dropped_si", LDAP_SI);
        Assertions.assertEquals(LDAP_SI, MysqlProto.pickSecurityIntegration(NATIVE).getName());
        chain("native", "dropped_si");
        Assertions.assertNull(MysqlProto.pickSecurityIntegration(NATIVE));
    }

    @Test
    public void testHandshakeSwitchesEachClientToItsOwnIntegration() throws Exception {
        chain("native", LDAP_SI, JWT_SI);

        // Declares nothing of ours: first integration, which is LDAP here, so a switch to clear password.
        Assertions.assertEquals(CLEAR, negotiate("alice", NATIVE));
        Assertions.assertEquals(CLEAR, switchRequested());

        // Declares the OpenID plugin: the JWT integration, even though it is second on the chain.
        // The declaration already matches, so no AuthSwitchRequest is needed.
        Assertions.assertEquals(OPENID, negotiate("harbor", OPENID));
        Assertions.assertNull(switchRequested());

        chain("native", JWT_SI, LDAP_SI);

        // JWT first: an undeclared client is switched to the OpenID plugin, as before.
        Assertions.assertEquals(OPENID, negotiate("alice", NATIVE));
        Assertions.assertEquals(OPENID, switchRequested());

        // A client that declares clear password is routed to the LDAP integration behind it.
        Assertions.assertEquals(CLEAR, negotiate("alice", CLEAR));
        Assertions.assertNull(switchRequested());
    }

    @Test
    public void testNativeUserIsNotRoutedByTheChain() throws Exception {
        CreateUserStmt stmt = (CreateUserStmt) SqlParser.parse(
                "create user local_user identified with mysql_native_password by 'pw'", 32).get(0);
        Analyzer.analyze(stmt, ctx);
        authenticationMgr.createUser(stmt);
        chain("native", JWT_SI, LDAP_SI);

        // The user table wins over the chain whatever the client declared.
        Assertions.assertEquals(NATIVE, negotiate("local_user", OPENID));
        Assertions.assertEquals(NATIVE, switchRequested());
    }
}
