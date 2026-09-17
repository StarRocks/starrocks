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

import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.PlainPasswordAuthenticationProvider;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authentication.SimpleLDAPSecurityIntegration;
import com.starrocks.authentication.TaskExecutionIdentity;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.authentication.UserProperty;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.DefaultAuthorizationProvider;
import com.starrocks.authorization.UserPrivilegeCollectionV2;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.Pair;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectScheduler;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MysqlProtoAITaskIdentityTest {
    private static final String NEW_USER = "change_user_task_creator";
    private static final String PASSWORD = "task_password";
    private static final byte[] SALT = "01234567890123456789".getBytes(StandardCharsets.UTF_8);

    private AuthenticationMgr previousAuthenticationMgr;
    private AuthorizationMgr previousAuthorizationMgr;
    private ConnectContext previousContext;
    private boolean previousAuthCheck;
    private String[] previousGroupProviders;
    private String[] previousAuthChain;
    private ConnectContext context;
    private TaskExecutionIdentity previousIdentity;
    private AuthenticationProvider previousProvider;
    private Set<Long> previousRoles;
    private boolean rejectUserChange;
    private boolean schedulerCalled;
    private boolean failResponse;
    private SessionVariable previousSessionVariable;
    private Map<String, SystemVariable> previousModifiedSessionVariables;
    private ComputeResource previousComputeResource;

    @BeforeEach
    public void setUp() throws Exception {
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        previousAuthenticationMgr = state.getAuthenticationMgr();
        previousAuthorizationMgr = state.getAuthorizationMgr();
        previousContext = ConnectContext.get();
        previousAuthCheck = Config.enable_auth_check;
        previousGroupProviders = Config.group_provider;
        previousAuthChain = Config.authentication_chain;
        Config.enable_auth_check = true;
        Config.group_provider = new String[0];
        Config.authentication_chain = new String[0];
        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        AuthorizationMgr authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        state.setAuthenticationMgr(authenticationMgr);
        state.setAuthorizationMgr(authorizationMgr);
        authenticationMgr.replayCreateUser(new UserIdentity(NEW_USER, "%"),
                new UserAuthenticationInfo(new UserRef(NEW_USER, "%"),
                        new UserAuthOption(AuthPlugin.Server.MYSQL_NATIVE_PASSWORD.toString(),
                                PASSWORD, true, NodePosition.ZERO)),
                new UserProperty(), new UserPrivilegeCollectionV2(),
                authorizationMgr.getProviderPluginId(), authorizationMgr.getProviderPluginVersion());
        UtFrameUtils.mockInitWarehouseEnv();
        new MockUp<MysqlChannel>() {
            @Mock
            public void sendAndFlush(ByteBuffer packet) throws IOException {
                if (failResponse) {
                    throw new IOException("Cannot send authentication error");
                }
            }

            @Mock
            public String getRemoteIp() {
                return "127.0.0.1";
            }
        };
        new MockUp<ConnectScheduler>() {
            @Mock
            public Pair<Boolean, String> onUserChanged(ConnectContext ctx, String previousUser, String newUser) {
                schedulerCalled = true;
                return Pair.create(!rejectUserChange, "Too many connections");
            }
        };
        ConnectScheduler scheduler = new ConnectScheduler(10);
        new MockUp<ExecuteEnv>() {
            @Mock
            public ConnectScheduler getScheduler() {
                return scheduler;
            }
        };
        context = new ConnectContext();
        context.setCapability(MysqlCapability.DEFAULT_CAPABILITY);
        context.setAuthDataSalt(SALT);
        context.setAuthPlugin(AuthPlugin.Client.MYSQL_NATIVE_PASSWORD.toString());
        AuthenticationHandler.authenticate(context, "root", "127.0.0.1", new byte[0]);
        context.setDatabase("original_db");
        context.setGroups(Set.of("original_group"));
        context.setAuthToken("original_token");
        context.setThreadLocalInfo();
        previousIdentity = TaskExecutionIdentity.capture(context);
        previousProvider = context.getAuthenticationProvider();
        previousRoles = Set.copyOf(context.getCurrentRoleIds());
    }

    @AfterEach
    public void tearDown() {
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        state.setAuthenticationMgr(previousAuthenticationMgr);
        state.setAuthorizationMgr(previousAuthorizationMgr);
        Config.enable_auth_check = previousAuthCheck;
        Config.group_provider = previousGroupProviders;
        Config.authentication_chain = previousAuthChain;
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testWrongPasswordPreservesAuthenticatedCreator() throws Exception {
        Assertions.assertFalse(MysqlProto.changeUser(context, changeUserPacket(NEW_USER, "wrong_password", "")));
        Assertions.assertFalse(schedulerCalled);
        assertOriginalAuthentication();
    }

    @Test
    public void testMissingUserPreservesAuthenticatedCreator() throws Exception {
        Assertions.assertFalse(MysqlProto.changeUser(context, changeUserPacket("missing_task_creator", "", "")));
        Assertions.assertFalse(schedulerCalled);
        assertOriginalAuthentication();
    }

    @Test
    public void testDatabaseFailurePreservesAuthenticatedCreator() throws Exception {
        rejectDatabaseChange();
        Assertions.assertFalse(MysqlProto.changeUser(context, changeUserPacket(NEW_USER, PASSWORD, "missing_db")));
        Assertions.assertEquals("Database does not exist", context.getState().getErrorMessage());
        Assertions.assertFalse(schedulerCalled);
        assertOriginalAuthentication();
    }

    @Test
    public void testConnectionLimitPreservesAuthenticatedCreator() throws Exception {
        rejectUserChange = true;
        Assertions.assertFalse(MysqlProto.changeUser(context, changeUserPacket(NEW_USER, PASSWORD, "")));
        Assertions.assertTrue(schedulerCalled);
        Assertions.assertEquals(ErrorCode.ERR_TOO_MANY_USER_CONNECTIONS, context.getState().getErrorCode());
        assertOriginalAuthentication();
    }

    @Test
    public void testErrorResponseFailurePreservesAuthenticatedCreator() throws Exception {
        rejectDatabaseChange();
        failResponse = true;
        Assertions.assertThrows(IOException.class,
                () -> MysqlProto.changeUser(context, changeUserPacket(NEW_USER, PASSWORD, "missing_db")));
        Assertions.assertEquals("Database does not exist", context.getState().getErrorMessage());
        assertOriginalAuthentication();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDatabaseFailureRestoresUserSessionProperties(boolean changeWarehouse) throws Exception {
        configureUserSessionProperties(changeWarehouse);

        Assertions.assertFalse(MysqlProto.changeUser(context,
                changeUserPacket(NEW_USER, PASSWORD, "default_catalog.missing_change_user_db")));

        Assertions.assertEquals(ErrorCode.ERR_BAD_DB_ERROR, context.getState().getErrorCode());
        Assertions.assertFalse(schedulerCalled);
        assertOriginalAuthentication();
        assertOriginalSessionProperties();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testConnectionLimitRestoresUserSessionProperties(boolean changeWarehouse) throws Exception {
        configureUserSessionProperties(changeWarehouse);
        rejectUserChange = true;

        Assertions.assertFalse(MysqlProto.changeUser(context, changeUserPacket(NEW_USER, PASSWORD, "")));

        Assertions.assertTrue(schedulerCalled);
        Assertions.assertEquals(ErrorCode.ERR_TOO_MANY_USER_CONNECTIONS, context.getState().getErrorCode());
        assertOriginalAuthentication();
        assertOriginalSessionProperties();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testErrorResponseFailureRestoresUserSessionProperties(boolean changeWarehouse) throws Exception {
        configureUserSessionProperties(changeWarehouse);
        failResponse = true;

        Assertions.assertThrows(IOException.class, () -> MysqlProto.changeUser(context,
                changeUserPacket(NEW_USER, PASSWORD, "default_catalog.missing_change_user_db")));

        Assertions.assertEquals(ErrorCode.ERR_BAD_DB_ERROR, context.getState().getErrorCode());
        assertOriginalAuthentication();
        assertOriginalSessionProperties();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testSuccessfulChangeAppliesIsolatedUserSessionProperties(boolean changeWarehouse) throws Exception {
        configureUserSessionProperties(changeWarehouse);

        Assertions.assertTrue(MysqlProto.changeUser(context, changeUserPacket(NEW_USER, PASSWORD, "")));

        Assertions.assertEquals(NEW_USER, TaskExecutionIdentity.capture(context).toThrift().getUser());
        Assertions.assertEquals(17, context.getSessionVariable().getQueryTimeoutS());
        Assertions.assertEquals("new_resource_group", context.getSessionVariable().getResourceGroup());
        Assertions.assertNotSame(previousSessionVariable, context.getSessionVariable());
        Assertions.assertEquals(41, previousSessionVariable.getQueryTimeoutS());
        Assertions.assertEquals("original_resource_group", previousSessionVariable.getResourceGroup());
        if (changeWarehouse) {
            Assertions.assertEquals("default_warehouse", context.getCurrentWarehouseName());
            Assertions.assertNull(context.getCurrentComputeResourceNoAcquire());
        } else {
            Assertions.assertSame(previousComputeResource, context.getCurrentComputeResourceNoAcquire());
        }
    }

    @Test
    public void testSuccessfulChangeRecordsNewCreator() throws Exception {
        Assertions.assertTrue(MysqlProto.changeUser(context, changeUserPacket(NEW_USER, PASSWORD, "")));
        Assertions.assertTrue(schedulerCalled);
        TaskExecutionIdentity identity = TaskExecutionIdentity.capture(context);
        Assertions.assertNotSame(previousIdentity, identity);
        Assertions.assertEquals(NEW_USER, identity.toThrift().getUser());
        Assertions.assertEquals(NEW_USER, context.getQualifiedUser());
        Assertions.assertEquals(NEW_USER, context.getDistinguishedName());
        Assertions.assertEquals("native", context.getSecurityIntegration());
    }

    @Test
    public void testSuccessfulSameUserReauthenticationRecordsCreator() throws Exception {
        Assertions.assertTrue(MysqlProto.changeUser(context, changeUserPacket("root", "", "")));
        Assertions.assertTrue(schedulerCalled);
        TaskExecutionIdentity identity = TaskExecutionIdentity.capture(context);
        Assertions.assertNotSame(previousIdentity, identity);
        Assertions.assertEquals("root", identity.toThrift().getUser());
    }

    @Test
    public void testGroupDenialPreservesOriginalAuthentication() throws Exception {
        String integrationName = "change_user_ldap";
        SecurityIntegration integration = new SimpleLDAPSecurityIntegration(integrationName,
                Map.of("type", "AUTHENTICATION_LDAP_SIMPLE", "permitted_groups", "allowed_group")) {
            @Override
            public AuthenticationProvider getAuthenticationProvider() {
                return (authContext, user, response) -> {
                    new PlainPasswordAuthenticationProvider(MysqlPassword.EMPTY_PASSWORD)
                            .authenticate(authContext, user, response);
                    authContext.setDistinguishedName("uid=denied_user,dc=example");
                    authContext.setAuthToken("rejected_token");
                };
            }
        };
        new MockUp<AuthenticationMgr>() {
            @Mock
            public SecurityIntegration getSecurityIntegration(String name) {
                return integrationName.equals(name) ? integration : null;
            }
        };
        Config.authentication_chain = new String[] {integrationName};
        context.setAuthPlugin(AuthPlugin.Client.MYSQL_CLEAR_PASSWORD.toString());

        ConnectContext deniedContext = new ConnectContext();
        deniedContext.setAuthPlugin(AuthPlugin.Client.MYSQL_CLEAR_PASSWORD.toString());
        AuthenticationException denied = Assertions.assertThrows(AuthenticationException.class,
                () -> AuthenticationHandler.authenticate(deniedContext, "denied_user", "127.0.0.1", new byte[0]));
        Assertions.assertEquals(ErrorCode.ERR_GROUP_ACCESS_DENY.formatErrorMsg("denied_user", "", "allowed_group"),
                denied.getMessage());

        Assertions.assertFalse(MysqlProto.changeUser(context, changeUserPacket("denied_user", "", "")));
        Assertions.assertFalse(schedulerCalled);
        assertOriginalAuthentication();
    }

    private void rejectDatabaseChange() {
        new MockUp<ConnectContext>() {
            @Mock
            public void changeCatalogDb(String database) throws DdlException {
                Assertions.assertEquals(NEW_USER, context.getQualifiedUser());
                Assertions.assertEquals(NEW_USER, TaskExecutionIdentity.capture(context).toThrift().getUser());
                throw new DdlException("Database does not exist");
            }
        };
    }

    private void configureUserSessionProperties(boolean changeWarehouse) throws DdlException {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        context.getSessionVariable().setQueryTimeoutS(41);
        context.getSessionVariable().setResourceGroup("original_resource_group");
        context.getSessionVariable().setWarehouseName("original_warehouse");
        context.modifySystemVariable(new SystemVariable(SessionVariable.SQL_SELECT_LIMIT, new StringLiteral("23")), true);
        previousSessionVariable = context.getSessionVariable();
        previousModifiedSessionVariables = new HashMap<>(context.getModifiedSessionVariablesMap());
        previousComputeResource = WarehouseComputeResource.of(1L);
        context.setCurrentComputeResource(previousComputeResource);

        Map<String, String> userVariables = new HashMap<>(Map.of(
                SessionVariable.QUERY_TIMEOUT, "17", SessionVariable.RESOURCE_GROUP, "new_resource_group"));
        if (changeWarehouse) {
            userVariables.put(SessionVariable.WAREHOUSE_NAME, "default_warehouse");
        }
        GlobalStateMgr.getCurrentState().getAuthenticationMgr().getUserProperty(NEW_USER)
                .setSessionVariables(userVariables);
    }

    private void assertOriginalSessionProperties() {
        Assertions.assertAll(
                () -> Assertions.assertSame(previousSessionVariable, context.getSessionVariable()),
                () -> Assertions.assertEquals(41, context.getSessionVariable().getQueryTimeoutS()),
                () -> Assertions.assertEquals(41, previousSessionVariable.getQueryTimeoutS()),
                () -> Assertions.assertEquals("original_resource_group", context.getSessionVariable().getResourceGroup()),
                () -> Assertions.assertEquals("original_warehouse", context.getCurrentWarehouseName()),
                () -> Assertions.assertEquals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, context.getCurrentCatalog()),
                () -> Assertions.assertEquals(previousModifiedSessionVariables, context.getModifiedSessionVariablesMap()),
                () -> Assertions.assertSame(previousComputeResource, context.getCurrentComputeResourceNoAcquire()));
    }

    private void assertOriginalAuthentication() {
        Assertions.assertAll(
                () -> Assertions.assertEquals(UserIdentity.ROOT, context.getCurrentUserIdentity()),
                () -> Assertions.assertEquals("root", context.getQualifiedUser()),
                () -> Assertions.assertEquals("root", context.getDistinguishedName()),
                () -> Assertions.assertEquals("native", context.getSecurityIntegration()),
                () -> Assertions.assertEquals("original_db", context.getDatabase()),
                () -> Assertions.assertEquals(Set.of("original_group"), context.getGroups()),
                () -> Assertions.assertEquals(previousRoles, context.getCurrentRoleIds()),
                () -> Assertions.assertEquals("original_token", context.getAuthToken()),
                () -> Assertions.assertSame(previousProvider, context.getAuthenticationProvider()),
                () -> Assertions.assertSame(previousIdentity, TaskExecutionIdentity.capture(context)));
    }

    private ByteBuffer changeUserPacket(String user, String password, String database) {
        byte[] authResponse = password.isEmpty() ? new byte[0] : MysqlPassword.scramble(SALT, password);
        MysqlSerializer packet = MysqlSerializer.newInstance();
        packet.writeInt1(MysqlCommand.COM_CHANGE_USER.getCommandCode());
        packet.writeNulTerminateString(user);
        packet.writeInt1(authResponse.length);
        packet.writeBytes(authResponse);
        packet.writeNulTerminateString(database);
        packet.writeInt2(33);
        packet.writeNulTerminateString(AuthPlugin.Client.MYSQL_NATIVE_PASSWORD.toString());
        return packet.toByteBuffer();
    }
}
