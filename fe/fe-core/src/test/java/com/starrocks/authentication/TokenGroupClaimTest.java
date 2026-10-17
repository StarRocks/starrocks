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

import com.nimbusds.jwt.JWTClaimsSet;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.metric.MetricRepo;
import com.starrocks.mysql.MysqlCodec;
import com.starrocks.mysql.MysqlProto;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.persist.NoOpEditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ExecuteAsExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.GrantType;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.ast.expression.InformationFunction;
import com.starrocks.sql.ast.integration.AlterSecurityIntegrationStatement;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Groups read off a claim of the verified token, for both integration types that hold one.
 * <p>
 * Most cases drive the whole login through {@link AuthenticationHandler} rather than the provider
 * alone, because what the feature has to get right is the group set the <b>session</b> ends up with:
 * which of the two sources contributed, whether the group providers were consulted at all, and what
 * the `permitted_groups` gate then does with the result. Reading the claim itself is covered one
 * layer down in {@link ClaimGroupExtractorTest}.
 */
public class TokenGroupClaimTest {
    private static final String ISSUER = "http://localhost:38080/realms/master";
    private static final String AUDIENCE = "12345";
    /** The `preferred_username` of every token the mock signs. */
    private static final String USER = "harbor";

    private static final String SI_NAME = "jwt_si";
    private static final String GP_NAME = "file_gp";

    /** Only ever reachable through the claim, so the two sources can be told apart. */
    private static final String CLAIM_GROUP = "tenant_a_writers";
    /** Only ever reachable through the group provider. */
    private static final String PROVIDER_GROUP = "Data Platform";

    /**
     * How many times a group provider was asked. `group_source = authenticator` has to
     * short-circuit rather than call-and-discard: a broken group provider must not be able to
     * affect a mode that excludes it, and asserting only on the resulting set would pass either way.
     */
    private static final AtomicInteger PROVIDER_CALLS = new AtomicInteger();

    private final MockTokenUtils mockTokenUtils = new MockTokenUtils();
    private ConnectContext ctx;
    private AuthenticationMgr authenticationMgr;
    private String[] savedChain;
    private String[] savedGroupProvider;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @BeforeEach
    public void setUp() throws Exception {
        GlobalStateMgr.getCurrentState().setEditLog(new NoOpEditLog());
        GlobalStateMgr.getCurrentState().setJwkMgr(new MockTokenUtils.MockJwkMgr());
        authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        savedChain = Config.authentication_chain;
        savedGroupProvider = Config.group_provider;
        PROVIDER_CALLS.set(0);

        // A group provider that needs no file and answers one fixed group, counting the calls.
        new MockUp<FileGroupProvider>() {
            @Mock
            public void init() {
            }

            @Mock
            public Set<String> getGroup(UserIdentity userIdentity, String distinguishedName) {
                PROVIDER_CALLS.incrementAndGet();
                return Set.of(PROVIDER_GROUP);
            }
        };
    }

    @AfterEach
    public void tearDown() {
        Config.authentication_chain = savedChain;
        Config.group_provider = savedGroupProvider;
    }

    // ---- fixtures ---------------------------------------------------------------------------

    private Map<String, String> jwtProperties(String groupSource, String groupClaim) {
        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_jwt");
        properties.put(JWTAuthenticationProvider.JWT_JWKS_URL, "jwks.json");
        properties.put(JWTAuthenticationProvider.JWT_PRINCIPAL_FIELD, "preferred_username");
        properties.put(JWTAuthenticationProvider.JWT_REQUIRED_ISSUER, ISSUER);
        properties.put(JWTAuthenticationProvider.JWT_REQUIRED_AUDIENCE, AUDIENCE);
        if (groupSource != null) {
            properties.put(JWTBaseSecurityIntegration.GROUP_SOURCE, groupSource);
        }
        if (groupClaim != null) {
            properties.put(JWTBaseSecurityIntegration.GROUP_CLAIM, groupClaim);
        }
        return properties;
    }

    private Map<String, String> oauth2Properties(String groupSource, String groupClaim) {
        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_oauth2");
        properties.put(OAuth2AuthenticationProvider.OAUTH2_JWKS_URL, "jwks.json");
        properties.put(OAuth2AuthenticationProvider.OAUTH2_PRINCIPAL_FIELD, "preferred_username");
        properties.put(OAuth2AuthenticationProvider.OAUTH2_REQUIRED_ISSUER, ISSUER);
        properties.put(OAuth2AuthenticationProvider.OAUTH2_REQUIRED_AUDIENCE, AUDIENCE);
        properties.put(OAuth2AuthenticationProvider.OAUTH2_TOKEN_SERVER_URL, "http://localhost:38080/token");
        properties.put(OAuth2AuthenticationProvider.OAUTH2_REDIRECT_URL, "http://localhost:8030/api/oauth2");
        properties.put(OAuth2AuthenticationProvider.OAUTH2_CLIENT_ID, "starrocks");
        properties.put(OAuth2AuthenticationProvider.OAUTH2_CLIENT_SECRET, "secret");
        if (groupSource != null) {
            properties.put(JWTBaseSecurityIntegration.GROUP_SOURCE, groupSource);
        }
        if (groupClaim != null) {
            properties.put(JWTBaseSecurityIntegration.GROUP_CLAIM, groupClaim);
        }
        return properties;
    }

    /** Register an integration and make it the only non-native link of the chain. */
    private void register(Map<String, String> properties) {
        authenticationMgr.replayCreateSecurityIntegration(SI_NAME, properties);
        Config.authentication_chain = new String[] {"native", SI_NAME};
    }

    private void createGroupProvider() throws DdlException {
        authenticationMgr.replayCreateGroupProvider(GP_NAME,
                Map.of(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "file",
                        FileGroupProvider.GROUP_FILE_URL, "file_group"));
    }

    private String tokenWith(Map<String, Object> claims) throws Exception {
        return mockTokenUtils.generateTokenWithClaims(claims, AUDIENCE, 3600 * 1000);
    }

    private ConnectContext loginWith(String token) throws AuthenticationException {
        ConnectContext context = new ConnectContext();
        AuthenticationHandler.authenticate(context,
                AuthCredentials.ofBearer(token, AuthChannel.HTTP, "127.0.0.1"));
        return context;
    }

    /** What `SELECT current_group()` would print on that session. */
    private String currentGroup(ConnectContext context) {
        StatementBase statement = SqlParser.parse("select current_group()", context.getSessionVariable()).get(0);
        Analyzer.analyze(statement, context);
        return ((InformationFunction) ((QueryStatement) statement).getQueryRelation()
                .getOutputExpression().get(0)).getStrValue();
    }

    // ---- which source contributes ------------------------------------------------------------

    /**
     * AC-1 / AC-2: the whole point of the feature. An integration with no group provider at all
     * still gives the session its groups, and the group providers are not consulted.
     */
    @Test
    public void testAuthenticatorSourceTakesTheGroupsFromTheClaim() throws Exception {
        createGroupProvider();
        Map<String, String> properties = jwtProperties("claim", "groups");
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, GP_NAME);
        register(properties);

        ConnectContext context = loginWith(tokenWith(Map.of("groups", List.of(CLAIM_GROUP))));

        Assertions.assertEquals(Set.of(CLAIM_GROUP), context.getGroups());
        Assertions.assertEquals(0, PROVIDER_CALLS.get(),
                "the group providers must be short-circuited, not called and discarded");
        // Ignored, not dropped: switching back has to be a single ALTER.
        Assertions.assertEquals(List.of(GP_NAME),
                authenticationMgr.getSecurityIntegration(SI_NAME).getGroupProviderName());
    }

    /**
     * The default keeps the old behaviour exactly: the claim is not read even when the token
     * carries it, and the group providers are the only source.
     */
    @Test
    public void testDefaultSourceIgnoresTheClaim() throws Exception {
        createGroupProvider();
        Map<String, String> properties = jwtProperties(null, null);
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, GP_NAME);
        register(properties);

        ConnectContext context = loginWith(tokenWith(Map.of("groups", List.of(CLAIM_GROUP))));

        Assertions.assertEquals(Set.of(PROVIDER_GROUP), context.getGroups());
        Assertions.assertEquals(1, PROVIDER_CALLS.get());
        Assertions.assertTrue(context.getAccessControlContext().getAuthenticatorGroups().isEmpty());
    }

    /** `both` is the union, and each side is visible in it. */
    @Test
    public void testBothSourcesAreUnioned() throws Exception {
        createGroupProvider();
        Map<String, String> properties = jwtProperties("both", "groups");
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, GP_NAME);
        register(properties);

        ConnectContext context = loginWith(tokenWith(Map.of("groups", List.of(CLAIM_GROUP))));

        Assertions.assertEquals(Set.of(CLAIM_GROUP, PROVIDER_GROUP), context.getGroups());
        Assertions.assertEquals(1, PROVIDER_CALLS.get());
    }

    /** A group both sources report appears once - the merge is a set union, not a concatenation. */
    @Test
    public void testBothSourcesDeduplicateTheSameGroup() throws Exception {
        createGroupProvider();
        Map<String, String> properties = jwtProperties("both", "groups");
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, GP_NAME);
        register(properties);

        ConnectContext context = loginWith(tokenWith(Map.of("groups", List.of(PROVIDER_GROUP))));

        Assertions.assertEquals(Set.of(PROVIDER_GROUP), context.getGroups());
    }

    /** AC-3: Keycloak's realm roles sit one level down, which is what the dotted path is for. */
    @Test
    public void testNestedClaimPath() throws Exception {
        register(jwtProperties("claim", "realm_access.roles"));

        ConnectContext context = loginWith(tokenWith(
                Map.of("realm_access", Map.of("roles", List.of(CLAIM_GROUP)))));

        Assertions.assertEquals(Set.of(CLAIM_GROUP), context.getGroups());
    }

    /**
     * AC-4: a token without the claim logs in and has no groups. It must not be a failed login -
     * Entra ID drops the claim for a user in more than 200 groups, so this state is reachable from a
     * correct configuration, and reporting it as an authentication failure would point at the wrong
     * thing entirely.
     */
    @Test
    public void testMissingClaimLogsInWithNoGroup() throws Exception {
        register(jwtProperties("claim", "groups"));

        ConnectContext context = loginWith(tokenWith(Map.of()));

        Assertions.assertEquals(USER, context.getCurrentUserIdentity().getUser());
        Assertions.assertTrue(context.getGroups().isEmpty());
        Assertions.assertEquals("NONE", currentGroup(context));
    }

    /**
     * AC-11: `current_group()` shows the merged set, and its rendering is the one it always had.
     */
    @Test
    public void testCurrentGroupShowsTheMergedSet() throws Exception {
        createGroupProvider();
        Map<String, String> properties = jwtProperties("both", "groups");
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, GP_NAME);
        register(properties);

        ConnectContext context = loginWith(tokenWith(Map.of("groups", List.of(CLAIM_GROUP))));

        String rendered = currentGroup(context);
        Assertions.assertTrue(rendered.contains(CLAIM_GROUP) && rendered.contains(PROVIDER_GROUP), rendered);
        Assertions.assertTrue(rendered.contains(", "), "joined the way it always was: " + rendered);
    }

    // ---- the permitted_groups gate -----------------------------------------------------------

    /**
     * AC-8, the admitting half: a claim group on the allowed list lets the login through, and the
     * group set is kept whole - the gate is not a filter. Matched ignoring case, like the LDAP half.
     */
    @Test
    public void testPermittedGroupsAdmitsAClaimGroupAndKeepsTheWholeSet() throws Exception {
        Map<String, String> properties = jwtProperties("claim", "groups");
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, "TENANT_A_WRITERS");
        register(properties);

        ConnectContext context =
                loginWith(tokenWith(Map.of("groups", List.of(CLAIM_GROUP, "another_group"))));

        Assertions.assertEquals(Set.of(CLAIM_GROUP, "another_group"), context.getGroups());
    }

    /** AC-8, the refusing half. */
    @Test
    public void testPermittedGroupsRejectsWhenNoClaimGroupMatches() throws Exception {
        Map<String, String> properties = jwtProperties("claim", "groups");
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, "some_other_group");
        register(properties);

        String token = tokenWith(Map.of("groups", List.of(CLAIM_GROUP)));
        AuthenticationException e =
                Assertions.assertThrows(AuthenticationException.class, () -> loginWith(token));
        Assertions.assertTrue(e.getMessage().contains(USER), e.getMessage());
    }

    // ---- case: matched ignoring case by default, exactly when configured so; never rewritten ---

    /**
     * The default keeps what LDAP shipped: a GRANT written in another case than the identity
     * provider spells the group still takes effect, and the session keeps the provider's spelling.
     */
    @Test
    public void testGroupCaseIsIgnoredByDefaultButNeverRewritten() throws Exception {
        register(jwtProperties("claim", "groups"));
        grantRoleToExternalGroup("claim_role", "claim_writers");

        ConnectContext context = loginWith(tokenWith(Map.of("groups", List.of("Claim_Writers"))));

        Assertions.assertEquals(Set.of("Claim_Writers"), context.getGroups(), "spelling kept");
        Assertions.assertTrue(context.getCurrentRoleIds().contains(roleId("claim_role")), "role mapped ignoring case");
    }

    /**
     * An identity provider that distinguishes groups by case wants exact matching, for the grant
     * and for the login gate alike - the two go through the same predicate.
     */
    @Test
    public void testGroupCaseCountsWhenConfigured() throws Exception {
        boolean saved = Config.authentication_group_case_insensitive;
        Config.authentication_group_case_insensitive = false;
        try {
            Map<String, String> properties = jwtProperties("claim", "groups");
            register(properties);
            grantRoleToExternalGroup("claim_role", "claim_writers");

            ConnectContext context = loginWith(tokenWith(Map.of("groups", List.of("Claim_Writers"))));
            Assertions.assertFalse(context.getCurrentRoleIds().contains(roleId("claim_role")),
                    "a spelling that differs in case is another group now");
            context = loginWith(tokenWith(Map.of("groups", List.of("claim_writers"))));
            Assertions.assertTrue(context.getCurrentRoleIds().contains(roleId("claim_role")));

            // the permitted_groups gate follows the same setting
            properties.put(SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, "claim_writers");
            register(properties);
            String differentCase = tokenWith(Map.of("groups", List.of("Claim_Writers")));
            Assertions.assertThrows(AuthenticationException.class, () -> loginWith(differentCase));
            loginWith(tokenWith(Map.of("groups", List.of("claim_writers"))));
        } finally {
            Config.authentication_group_case_insensitive = saved;
        }
    }

    // ---- MySQL + OAuth2 authorization-code flow: groups from the id token (second phase) -------

    /** A stock MySQL client: its handshake is answered before any token exists. */
    private ConnectContext mysqlOAuth2Handshake() throws AuthenticationException {
        ConnectContext context = new ConnectContext();
        context.setQualifiedUser(USER);
        AuthenticationHandler.authenticate(context, MysqlProto.handshakeCredentials(
                USER, new byte[20], AuthPlugin.Client.MYSQL_NATIVE_PASSWORD.toString(), "127.0.0.1"));
        return context;
    }

    /** What OAuth2Action does once the IdP has answered: verify the id token, then hand it over. */
    private static void oauth2Callback(ConnectContext context, String idToken) throws Exception {
        JWTClaimsSet claims = OpenIdConnectVerifier.verify(idToken, context.getQualifiedUser(),
                GlobalStateMgr.getCurrentState().getJwkMgr().getJwkSource("jwks.json"), "preferred_username",
                new String[] {ISSUER}, new String[] {AUDIENCE});
        ((OAuth2AuthenticationProvider) context.getAuthenticationProvider())
                .acceptVerifiedToken(context.getAccessControlContext(), idToken, claims);
    }

    /** What ConnectProcessor runs before every statement. */
    private static void beforeStatement(ConnectContext context) throws AuthenticationException {
        context.getAuthenticationProvider().checkLoginSuccess(context.getConnectionId(), context.getAccessControlContext());
    }

    /**
     * AC-12 and the deferral half of AC-13. The handshake decides no group and runs no gate - it
     * would have refused this user, whose groups are all in a token that has not arrived. The
     * statement after the browser login gets the claim's groups and their roles, with no group
     * provider consulted.
     */
    @Test
    public void testMysqlAuthorizationCodeFlowTakesTheGroupsFromTheIdToken() throws Exception {
        createGroupProvider();
        Map<String, String> properties = oauth2Properties("claim", "groups");
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, GP_NAME);
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, CLAIM_GROUP);
        register(properties);
        grantRoleToExternalGroup("claim_role", CLAIM_GROUP);

        ConnectContext context = mysqlOAuth2Handshake();
        Assertions.assertTrue(context.getGroups().isEmpty());
        Assertions.assertEquals(0, PROVIDER_CALLS.get());

        AuthenticationException beforeLogin =
                Assertions.assertThrows(AuthenticationException.class, () -> beforeStatement(context));
        Assertions.assertEquals(ErrorCode.ERR_OAUTH2_NOT_AUTHENTICATED, beforeLogin.getErrorCode());

        oauth2Callback(context, tokenWith(Map.of("groups", List.of(CLAIM_GROUP))));
        beforeStatement(context);

        Assertions.assertEquals(Set.of(CLAIM_GROUP), context.getGroups());
        Assertions.assertTrue(context.getCurrentRoleIds().contains(roleId("claim_role")));
        Assertions.assertEquals(0, PROVIDER_CALLS.get());
    }

    /** `both` merges the two sources once, on the first statement, not on every one. */
    @Test
    public void testMysqlAuthorizationCodeFlowResolvesTheGroupsOnce() throws Exception {
        createGroupProvider();
        Map<String, String> properties = oauth2Properties("both", "groups");
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, GP_NAME);
        register(properties);

        ConnectContext context = mysqlOAuth2Handshake();
        oauth2Callback(context, tokenWith(Map.of("groups", List.of(CLAIM_GROUP))));
        beforeStatement(context);
        beforeStatement(context);
        beforeStatement(context);

        Assertions.assertEquals(Set.of(CLAIM_GROUP, PROVIDER_GROUP), context.getGroups());
        Assertions.assertEquals(1, PROVIDER_CALLS.get());
    }

    /**
     * AC-13, the refusing half: the gate runs after the browser login, and once it has refused, the
     * connection stays refused - a second statement must not slip through with the gate cleared.
     */
    @Test
    public void testMysqlAuthorizationCodeFlowGateRefusesAfterTheLoginAndKeepsRefusing() throws Exception {
        Map<String, String> properties = oauth2Properties("claim", "groups");
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, "some_other_group");
        register(properties);

        ConnectContext context = mysqlOAuth2Handshake();
        oauth2Callback(context, tokenWith(Map.of("groups", List.of(CLAIM_GROUP))));

        for (int i = 0; i < 2; i++) {
            AuthenticationException e =
                    Assertions.assertThrows(AuthenticationException.class, () -> beforeStatement(context));
            Assertions.assertEquals(ErrorCode.ERR_GROUP_ACCESS_DENY, e.getErrorCode());
        }
    }

    /**
     * The authentication metrics count the gate's verdict, not the handshake that could not run it
     * yet: a user the gate refuses after the browser login is one failure, never a success, and an
     * admitted one is one success, counted when the gate admits it.
     */
    @Test
    public void testMysqlAuthorizationCodeFlowCountsTheGateVerdictNotTheHandshake() throws Exception {
        boolean savedInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            Map<String, String> properties = oauth2Properties("claim", "groups");
            properties.put(SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, "some_other_group");
            register(properties);
            long successes = mysqlSuccesses();
            long failures = mysqlFailures();

            ConnectContext refused = mysqlOAuth2Handshake();
            Assertions.assertEquals(successes, mysqlSuccesses());
            Assertions.assertEquals(failures, mysqlFailures());

            oauth2Callback(refused, tokenWith(Map.of("groups", List.of(CLAIM_GROUP))));
            Assertions.assertThrows(AuthenticationException.class, () -> beforeStatement(refused));
            Assertions.assertEquals(successes, mysqlSuccesses());
            Assertions.assertEquals(failures + 1, mysqlFailures());
            // The remembered refusal is not counted again.
            Assertions.assertThrows(AuthenticationException.class, () -> beforeStatement(refused));
            Assertions.assertEquals(failures + 1, mysqlFailures());

            properties.put(SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, CLAIM_GROUP);
            register(properties);
            ConnectContext admitted = mysqlOAuth2Handshake();
            oauth2Callback(admitted, tokenWith(Map.of("groups", List.of(CLAIM_GROUP))));
            beforeStatement(admitted);
            beforeStatement(admitted);
            Assertions.assertEquals(successes + 1, mysqlSuccesses());
            Assertions.assertEquals(failures + 1, mysqlFailures());
        } finally {
            MetricRepo.hasInit = savedInit;
        }
    }

    private static long mysqlSuccesses() {
        return MetricRepo.COUNTER_AUTH_SUCCESS.getMetric("mysql", "mysql_native_scramble").getValue();
    }

    private static long mysqlFailures() {
        return MetricRepo.COUNTER_AUTH_FAILURE.getMetric("mysql", "mysql_native_scramble").getValue();
    }

    /** No claim in the id token: logged in with no groups, exactly as on the Bearer path. */
    @Test
    public void testMysqlAuthorizationCodeFlowWithoutTheClaim() throws Exception {
        register(oauth2Properties("claim", "groups"));

        ConnectContext context = mysqlOAuth2Handshake();
        oauth2Callback(context, tokenWith(Map.of()));
        beforeStatement(context);

        Assertions.assertTrue(context.getGroups().isEmpty());
    }

    /**
     * AC-14: an integration on group providers only keeps deciding on the handshake, gate included -
     * a user who is not permitted is refused before the browser login, as before.
     */
    @Test
    public void testMysqlAuthorizationCodeFlowOnGroupProvidersOnlyDecidesOnTheHandshake() throws Exception {
        createGroupProvider();
        Map<String, String> properties = oauth2Properties(null, null);
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, GP_NAME);
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, PROVIDER_GROUP);
        register(properties);

        ConnectContext context = mysqlOAuth2Handshake();
        Assertions.assertEquals(Set.of(PROVIDER_GROUP), context.getGroups());
        Assertions.assertEquals(1, PROVIDER_CALLS.get());

        properties.put(SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, "some_other_group");
        register(properties);
        AuthenticationException e =
                Assertions.assertThrows(AuthenticationException.class, this::mysqlOAuth2Handshake);
        Assertions.assertEquals(ErrorCode.ERR_GROUP_ACCESS_DENY, e.getErrorCode());
    }

    /**
     * A client that speaks the OAuth2 plugin waits in the handshake until the callback has delivered
     * the token, so its groups are known in time and nothing is deferred. The callback runs on
     * another thread here, as it does on the FE's HTTP server, which also exercises the ordering
     * that makes the groups visible to the connection: groups first, then the volatile token.
     */
    @Test
    public void testMysqlPluginClientGetsTheClaimGroupsOnTheHandshake() throws Exception {
        Map<String, String> properties = oauth2Properties("claim", "groups");
        properties.put(OAuth2AuthenticationProvider.OAUTH2_CONNECT_WAIT_TIMEOUT, "10");
        register(properties);
        String token = tokenWith(Map.of("groups", List.of(CLAIM_GROUP)));

        for (int round = 0; round < 5; round++) {
            ConnectContext context = new ConnectContext();
            context.setQualifiedUser(USER);
            java.util.concurrent.atomic.AtomicReference<Exception> callbackError =
                    new java.util.concurrent.atomic.AtomicReference<>();
            Thread callback = new Thread(() -> {
                try {
                    while (!(context.getAuthenticationProvider() instanceof OAuth2AuthenticationProvider)) {
                        Thread.sleep(5);
                    }
                    oauth2Callback(context, token);
                } catch (Exception e) {
                    callbackError.set(e);
                }
            });
            callback.start();
            AuthenticationHandler.authenticate(context, MysqlProto.handshakeCredentials(
                    USER, new byte[0], AuthPlugin.Client.AUTHENTICATION_OAUTH2_CLIENT.toString(), "127.0.0.1"));
            callback.join();
            Assertions.assertNull(callbackError.get(), "callback failed in round " + round);

            Assertions.assertEquals(Set.of(CLAIM_GROUP), context.getGroups(), "round " + round);
            beforeStatement(context);
        }
    }

    // ---- first-phase gaps: EXECUTE AS and the attribution log --------------------------------

    /**
     * AC-7 with groups that really came from a claim: the target identity holds no token, so it can
     * never have claim groups, and the initiator's must not survive the switch.
     */
    @Test
    public void testExecuteAsDropsTheClaimGroupsOfTheInitiator() throws Exception {
        register(jwtProperties("claim", "groups"));
        Config.group_provider = new String[0];
        authenticationMgr.createUser(new CreateUserStmt(new UserRef("target_user", "%"), true, null,
                List.of(), Map.of(), NodePosition.ZERO));

        ConnectContext context = loginWith(tokenWith(Map.of("groups", List.of(CLAIM_GROUP))));
        Assertions.assertEquals(Set.of(CLAIM_GROUP), context.getGroups());

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserRef("target_user", "%"), false), context);

        Assertions.assertEquals("target_user", context.getCurrentUserIdentity().getUser());
        Assertions.assertFalse(context.getGroups().contains(CLAIM_GROUP));
        Assertions.assertTrue(context.getAccessControlContext().getAuthenticatorGroups().isEmpty());
    }

    /** Collects what AuthenticationHandler logs about the `both` attribution. */
    private static class AttributionAppender extends AbstractAppender {
        private final List<String> infoLines = new java.util.ArrayList<>();

        AttributionAppender() {
            super("group-attribution-collector", null, null, true, null);
        }

        @Override
        public void append(LogEvent event) {
            String message = event.getMessage().getFormattedMessage();
            if (event.getLevel() == Level.INFO && message.contains("group_source=both")) {
                infoLines.add(message);
            }
        }
    }

    private static List<String> attributionLinesOf(ThrowingRunnable login) throws Exception {
        AttributionAppender appender = new AttributionAppender();
        org.apache.logging.log4j.core.Logger logger =
                (org.apache.logging.log4j.core.Logger) LogManager.getLogger(AuthenticationHandler.class);
        Level savedLevel = logger.getLevel();
        appender.start();
        logger.addAppender(appender);
        // After addAppender, which rebuilds this logger's configuration from its parent (WARN in the
        // unit-test log4j2.xml) and would undo an earlier setLevel.
        logger.setLevel(Level.INFO);
        try {
            login.run();
        } finally {
            logger.removeAppender(appender);
            appender.stop();
            logger.setLevel(savedLevel);
        }
        return appender.infoLines;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /**
     * AC-11's attribution half: with `both`, a login that authenticates once per session logs what
     * each source contributed at INFO; one that authenticates on every request (HTTP) does not, so a
     * busy cluster does not get an INFO line per query.
     */
    @Test
    public void testBothModeLogsTheAttributionOncePerSessionOnly() throws Exception {
        createGroupProvider();
        Map<String, String> properties = jwtProperties("both", "groups");
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, GP_NAME);
        register(properties);
        String token = tokenWith(Map.of("groups", List.of(CLAIM_GROUP)));

        List<String> mysql = attributionLinesOf(() -> {
            ByteArrayOutputStream frame = new ByteArrayOutputStream();
            MysqlCodec.writeInt1(frame, 1);
            MysqlCodec.writeLenEncodedString(frame, token);
            AuthenticationHandler.authenticate(new ConnectContext(), MysqlProto.handshakeCredentials(
                    USER, frame.toByteArray(), AuthPlugin.Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString(),
                    "127.0.0.1"));
        });
        Assertions.assertEquals(1, mysql.size(), mysql.toString());
        Assertions.assertTrue(mysql.get(0).contains(PROVIDER_GROUP) && mysql.get(0).contains(CLAIM_GROUP), mysql.get(0));

        List<String> http = attributionLinesOf(() -> loginWith(token));
        Assertions.assertEquals(List.of(), http);
    }

    private static AuthorizationMgr authorizationMgr() {
        return GlobalStateMgr.getCurrentState().getAuthorizationMgr();
    }

    /** `CREATE ROLE IF NOT EXISTS <role>; GRANT <role> TO EXTERNAL GROUP <group>`. */
    private void grantRoleToExternalGroup(String role, String group) throws Exception {
        authorizationMgr().createRole(new CreateRoleStmt(List.of(role), true, ""));
        GrantRoleStmt grant = new GrantRoleStmt(List.of(role), group, GrantType.GROUP, NodePosition.ZERO);
        Analyzer.analyze(grant, ctx);
        authorizationMgr().grantRole(grant);
    }

    private static long roleId(String role) throws Exception {
        return authorizationMgr().getRoleIdByNameAllowNull(role);
    }

    // ---- the claim is read after verification, never before ----------------------------------

    /**
     * The security property of the whole feature: a token whose signature or claims do not hold
     * never contributes a group. Otherwise the holder of a token would get to pick their own groups.
     * A sentinel value is planted first, so this asserts that nothing was written - not merely that
     * the result came out empty.
     */
    @Test
    public void testClaimIsNotReadWhenVerificationFails() throws Exception {
        JWTAuthenticationProvider provider = new JWTAuthenticationProvider("jwks.json", "preferred_username",
                new String[] {ISSUER}, new String[] {AUDIENCE}, GroupSource.AUTHENTICATOR, "groups", SI_NAME);
        AccessControlContext authContext = new ConnectContext().getAccessControlContext();
        authContext.setAuthenticatorGroups(Set.of("sentinel"));

        // Expired, but carrying the claim: verification has to refuse it before the claim is read.
        String expired = mockTokenUtils.generateTokenWithClaims(
                Map.of("groups", List.of(CLAIM_GROUP)), AUDIENCE, -1000);

        Assertions.assertThrows(AuthenticationException.class,
                () -> provider.authenticate(authContext, null, AuthCredentials.ofBearer(expired,
                        AuthChannel.HTTP, "127.0.0.1")));
        Assertions.assertEquals(Set.of("sentinel"), authContext.getAuthenticatorGroups());
    }

    // ---- OAuth2 parity ------------------------------------------------------------------------

    /** AC-9: an OAuth2 integration acting as a Resource Server reads the claim the same way. */
    @Test
    public void testOAuth2BearerReadsTheSameClaim() throws Exception {
        createGroupProvider();
        Map<String, String> properties = oauth2Properties("claim", "groups");
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, GP_NAME);
        register(properties);

        ConnectContext context = loginWith(tokenWith(Map.of("groups", List.of(CLAIM_GROUP))));

        Assertions.assertEquals(Set.of(CLAIM_GROUP), context.getGroups());
        Assertions.assertEquals(0, PROVIDER_CALLS.get());
    }

    /**
     * The OAuth2 MySQL authorization-code flow holds no token at authentication time - it arrives
     * later through the `/api/oauth2` callback. With a source that reads the token, the provider
     * therefore asks for the group resolution to be deferred instead of deciding on nothing, and
     * stops asking once the callback has handed it a verified token.
     */
    @Test
    public void testOAuth2MysqlFlowDefersUntilTheTokenArrives() throws Exception {
        register(oauth2Properties("claim", "groups"));
        OAuth2AuthenticationProvider provider = (OAuth2AuthenticationProvider)
                authenticationMgr.getSecurityIntegration(SI_NAME).getAuthenticationProvider();
        Assertions.assertTrue(provider.isGroupResolutionDeferred());
        Assertions.assertFalse(provider.isGroupProviderUsed());
        Assertions.assertFalse(provider.isAuthenticatorGroupsUsed());

        String token = tokenWith(Map.of("groups", List.of(CLAIM_GROUP)));
        provider.acceptVerifiedToken(new ConnectContext().getAccessControlContext(), token,
                com.nimbusds.jwt.SignedJWT.parse(token).getJWTClaimsSet());
        Assertions.assertFalse(provider.isGroupResolutionDeferred());
        Assertions.assertTrue(provider.isAuthenticatorGroupsUsed());

        // Nothing to defer when the groups need no token: an integration on group providers only,
        // and a native OAuth2 user, whose provider has no group source at all.
        register(oauth2Properties(null, null));
        Assertions.assertFalse(authenticationMgr.getSecurityIntegration(SI_NAME)
                .getAuthenticationProvider().isGroupResolutionDeferred());
        Assertions.assertFalse(new OAuth2AuthenticationProvider(null).isGroupResolutionDeferred());
    }

    /**
     * A ConnectContext outlives one authentication (COM_CHANGE_USER authenticates again on the same
     * one), and a provider writes the authenticator groups only when it resolves some. The next
     * authentication therefore starts from an empty set, or a provider that resolves none for this
     * login would hand the previous user's groups to the new one.
     */
    @Test
    public void testTheNextAuthenticationOnTheSameContextStartsWithNoAuthenticatorGroup() throws Exception {
        register(jwtProperties("claim", "groups"));
        ConnectContext context = loginWith(tokenWith(Map.of("groups", List.of(CLAIM_GROUP))));
        Assertions.assertEquals(Set.of(CLAIM_GROUP), context.getAccessControlContext().getAuthenticatorGroups());

        // The integration no longer reads the claim, so the provider writes nothing this time.
        register(jwtProperties(null, null));
        AuthenticationHandler.authenticate(context, AuthCredentials.ofBearer(
                tokenWith(Map.of("groups", List.of(CLAIM_GROUP))), AuthChannel.HTTP, "127.0.0.1"));
        Assertions.assertTrue(context.getAccessControlContext().getAuthenticatorGroups().isEmpty());
        Assertions.assertTrue(context.getGroups().isEmpty());
    }

    /** The absent-claim line is a WARN once a minute per integration and claim, DEBUG in between. */
    @Test
    public void testAbsentClaimIsWarnedOnceAMinute() {
        String key = "si_" + System.nanoTime() + ":groups";
        Assertions.assertTrue(ClaimGroupExtractor.warnAbsentClaimNow(key));
        Assertions.assertFalse(ClaimGroupExtractor.warnAbsentClaimNow(key));
        Assertions.assertTrue(ClaimGroupExtractor.warnAbsentClaimNow(key + ".other"));
    }

    /** The two predicates for the three values, on the type that is always Bearer. */
    @Test
    public void testPredicatesFollowTheGroupSource() {
        Map<GroupSource, boolean[]> expected = Map.of(
                GroupSource.GROUP_PROVIDER, new boolean[] {true, false},
                GroupSource.AUTHENTICATOR, new boolean[] {false, true},
                GroupSource.BOTH, new boolean[] {true, true});
        expected.forEach((source, flags) -> {
            AuthenticationProvider provider = new JWTAuthenticationProvider("jwks.json", "preferred_username",
                    new String[] {ISSUER}, new String[] {AUDIENCE}, source, "groups", SI_NAME);
            Assertions.assertEquals(flags[0], provider.isGroupProviderUsed(), source.getValue());
            Assertions.assertEquals(flags[1], provider.isAuthenticatorGroupsUsed(), source.getValue());
        });
    }

    // ---- DDL validation ------------------------------------------------------------------------

    /**
     * AC-4b: `group_claim` has no default, because the claim an identity provider emits differs per
     * provider and a wrong guess fails as "logged in but has no privileges". So the combination that
     * would resolve no group at all is refused while the administrator is typing.
     */
    @Test
    public void testCreateRequiresTheClaimWhenTheSourceReadsIt() {
        for (String source : new String[] {"claim", "authenticator", "both"}) {
            // A blank path is as absent as a missing one: it would name no claim and resolve no group.
            for (String claim : new String[] {null, "", "   "}) {
                SemanticException e = Assertions.assertThrows(SemanticException.class,
                        () -> SecurityIntegrationFactory
                                .createSecurityIntegration(SI_NAME, jwtProperties(source, claim)).checkProperty());
                Assertions.assertTrue(e.getMessage().contains(
                        JWTBaseSecurityIntegration.GROUP_CLAIM), e.getMessage());
            }
        }
        // With the claim, the same properties pass.
        SecurityIntegrationFactory.createSecurityIntegration(SI_NAME, jwtProperties("claim", "groups"))
                .checkProperty();
        SecurityIntegrationFactory.createSecurityIntegration(SI_NAME, oauth2Properties("both", "groups"))
                .checkProperty();
    }

    /**
     * The same combination together with `permitted_groups` refuses every user, so the message says
     * so rather than leaving the administrator to discover it one login at a time.
     */
    @Test
    public void testCreateMessageMentionsPermittedGroupsWhenItIsConfigured() {
        Map<String, String> properties = jwtProperties("claim", null);
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, "g1");

        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> SecurityIntegrationFactory.createSecurityIntegration(SI_NAME, properties).checkProperty());
        Assertions.assertTrue(
                e.getMessage().contains(SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN),
                e.getMessage());
    }

    /** A claim with no source is a no-op property, which is worth saying out loud. */
    @Test
    public void testCreateRejectsAClaimWithNoSource() {
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> SecurityIntegrationFactory
                        .createSecurityIntegration(SI_NAME, jwtProperties(null, "groups")).checkProperty());
        Assertions.assertTrue(e.getMessage().contains(
                JWTBaseSecurityIntegration.GROUP_SOURCE), e.getMessage());
    }

    /**
     * The third value is spelled `claim` here and `memberof` on an LDAP integration. Each is
     * refused on the other, and the message lists what is legal on this property; the neutral
     * `authenticator` works on both, as a fallback for anything written against the internal name.
     */
    @Test
    public void testCreateRejectsTheLdapSpellingAndAcceptsTheNeutralOne() {
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> SecurityIntegrationFactory
                        .createSecurityIntegration(SI_NAME, jwtProperties("memberof", "groups")).checkProperty());
        Assertions.assertTrue(e.getMessage().contains("group_provider, claim, both"), e.getMessage());
        SecurityIntegrationFactory.createSecurityIntegration(SI_NAME, jwtProperties("authenticator", "groups"))
                .checkProperty();
        // and the error message for a missing claim spells the value the way the user typed it
        e = Assertions.assertThrows(SemanticException.class,
                () -> SecurityIntegrationFactory
                        .createSecurityIntegration(SI_NAME, jwtProperties("claim", null)).checkProperty());
        Assertions.assertTrue(e.getMessage().contains("is 'claim'"), e.getMessage());
    }

    @Test
    public void testCreateRejectsAnIllegalSource() {
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> SecurityIntegrationFactory
                        .createSecurityIntegration(SI_NAME, jwtProperties("token", "groups")).checkProperty());
        Assertions.assertTrue(e.getMessage().contains("token"), e.getMessage());
    }

    /**
     * ALTER does not go through `checkProperty()`, so it is validated in the analyzer - and against
     * the merged properties, because either half of the pair can be set on its own.
     */
    @Test
    public void testAlterIsValidatedAgainstTheMergedProperties() {
        register(jwtProperties(null, null));

        // Turning the source on alone leaves no claim configured: refused.
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> Analyzer.analyze(parseAlter("\"group_source\" = \"claim\""), ctx));
        Assertions.assertTrue(e.getMessage().contains(
                JWTBaseSecurityIntegration.GROUP_CLAIM), e.getMessage());

        // Both at once is fine ...
        Analyzer.analyze(parseAlter("\"group_source\" = \"claim\", \"group_claim\" = \"groups\""), ctx);
        // ... and so is the source alone once the claim is already part of the integration.
        authenticationMgr.replayAlterSecurityIntegration(SI_NAME,
                Map.of(JWTBaseSecurityIntegration.GROUP_CLAIM, "groups"));
        Analyzer.analyze(parseAlter("\"group_source\" = \"both\""), ctx);
        // An ALTER that mentions neither property is not affected by the check.
        Analyzer.analyze(parseAlter("\"principal_field\" = \"sub\""), ctx);
    }

    private AlterSecurityIntegrationStatement parseAlter(String setClause) {
        return (AlterSecurityIntegrationStatement) SqlParser.parseSingleStatement(
                "ALTER SECURITY INTEGRATION " + SI_NAME + " SET (" + setClause + ")",
                ctx.getSessionVariable().getSqlMode());
    }

    /**
     * A value that got past DDL validation - a replayed edit log from a version without the check,
     * or a hand-edited image - must not fail every login of that integration. It falls back to the
     * default and logs an ERROR.
     */
    @Test
    public void testRuntimeBadSourceFallsBackToTheGroupProviders() throws Exception {
        createGroupProvider();
        Map<String, String> properties = jwtProperties("clam", "groups");
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, GP_NAME);
        register(properties);

        Assertions.assertEquals(GroupSource.GROUP_PROVIDER,
                ((JWTBaseSecurityIntegration) authenticationMgr.getSecurityIntegration(SI_NAME)).getGroupSource());

        ConnectContext context = loginWith(tokenWith(Map.of("groups", List.of(CLAIM_GROUP))));

        Assertions.assertEquals(Set.of(PROVIDER_GROUP), context.getGroups());
    }

    /**
     * The claim path is not a MySQL-only or HTTP-only feature: the same token on the MySQL OpenID
     * Connect plugin resolves the same groups, because the entry point normalises the frame and the
     * provider sees one credential shape.
     */
    @Test
    public void testTheSameClaimIsReadOnTheMysqlChannel() throws Exception {
        register(jwtProperties("claim", "groups"));
        String token = tokenWith(Map.of("groups", List.of(CLAIM_GROUP)));

        // What the OpenID Connect client plugin puts on the wire: a one-byte stage plus the
        // length-encoded token. The entry point unwraps it, so the provider sees a Bearer token.
        ByteArrayOutputStream frame = new ByteArrayOutputStream();
        MysqlCodec.writeInt1(frame, 1);
        MysqlCodec.writeLenEncodedString(frame, token);

        ConnectContext context = new ConnectContext();
        context.setAuthPlugin(AuthPlugin.Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString());
        AuthenticationHandler.authenticate(context, MysqlProto.handshakeCredentials(
                USER, frame.toByteArray(),
                AuthPlugin.Client.AUTHENTICATION_OPENID_CONNECT_CLIENT.toString(), "127.0.0.1"));

        Assertions.assertEquals(Set.of(CLAIM_GROUP), context.getGroups());
    }
}
