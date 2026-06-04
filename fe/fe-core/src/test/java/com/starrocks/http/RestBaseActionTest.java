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

package com.starrocks.http;

import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.http.rest.BootstrapFinishAction;
import com.starrocks.http.rest.ConnectionAction;
import com.starrocks.http.rest.FeatureAction;
import com.starrocks.http.rest.GetClusterSnapshotRestoreStateAction;
import com.starrocks.http.rest.GetSmallFileAction;
import com.starrocks.http.rest.HealthAction;
import com.starrocks.http.rest.IdleAction;
import com.starrocks.http.rest.MetricsAction;
import com.starrocks.http.rest.OAuth2Action;
import com.starrocks.http.rest.QueryProgressAction;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.ShowDataAction;
import com.starrocks.http.rest.ShowMetaInfoAction;
import com.starrocks.http.rest.ShowRuntimeInfoAction;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TNetworkAddress;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestBaseActionTest {

    private RestBaseAction restBaseAction;
    private BaseRequest mockRequest;
    private BaseResponse mockResponse;
    private TNetworkAddress mockAddr;

    static class TestableRestBaseAction extends RestBaseAction {
        boolean executed;
        UserIdentity observedUser;
        Set<Long> observedRoleIds;
        Set<String> observedGroups;
        String observedQualifiedUser;
        String observedRemoteIp;

        public TestableRestBaseAction() {
            super(null);
        }

        @Override
        protected void executeWithoutPassword(BaseRequest request, BaseResponse response)
                throws DdlException, AccessDeniedException {
            executed = true;
            ConnectContext ctx = ConnectContext.get();
            observedUser = ctx.getCurrentUserIdentity();
            observedRoleIds = ctx.getCurrentRoleIds();
            observedGroups = ctx.getGroups();
            observedQualifiedUser = ctx.getQualifiedUser();
            observedRemoteIp = ctx.getRemoteIP();
        }

        // Expose the protected enable_http_auth-gated helpers so tests in this package
        // can call them without reflection.
        public void callRequireOperate() throws AccessDeniedException {
            requireOperateIfHttpAuthEnabled();
        }

        public void callRequireDbInsert(String db) throws AccessDeniedException {
            requireDbInsertIfHttpAuthEnabled(db);
        }
    }

    static class NoAuthRestBaseAction extends TestableRestBaseAction {
        @Override
        public boolean needAuth() {
            return false;
        }
    }

    @BeforeEach
    public void setUp() {
        restBaseAction = spy(new TestableRestBaseAction());
        mockRequest = mock(BaseRequest.class);
        mockResponse = mock(BaseResponse.class);
        mockAddr = mock(TNetworkAddress.class);

        when(mockAddr.getHostname()).thenReturn("127.0.0.1");
        when(mockAddr.getPort()).thenReturn(8030);

        HttpRequest mockHttpRequest = mock(HttpRequest.class);
        when(mockHttpRequest.uri()).thenReturn("/api/mydb/testStreamLoad%E6%B5%8B%E8%AF%95/_stream_load");
        when(mockHttpRequest.method()).thenReturn(HttpMethod.GET);
        when(mockHttpRequest.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);

        HttpHeaders mockHeaders = mock(HttpHeaders.class);
        when(mockHttpRequest.headers()).thenReturn(mockHeaders);
        when(mockHeaders.containsValue(
            eq(HttpHeaderNames.CONNECTION),
            eq(HttpHeaderValues.KEEP_ALIVE),
            eq(true)
        )).thenReturn(false);  // or true

        when(mockRequest.getRequest()).thenReturn(mockHttpRequest);
        when(mockResponse.getContent()).thenReturn(new StringBuilder());

        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        when(mockRequest.getContext()).thenReturn(mockCtx);
    }

    @Test
    public void testRedirectTo() throws Exception {
        URI expectedUri = new URI("http", null, "127.0.0.1", 8030, "/api/mydb/testStreamLoad测试/_stream_load", null, null);
        String asciiUri = expectedUri.toASCIIString();

        restBaseAction.redirectTo(mockRequest, mockResponse, mockAddr);
        verify(mockResponse).updateHeader(HttpHeaderNames.LOCATION.toString(), asciiUri);
    }

    private static String basicAuth(String user, String password) {
        String raw = user + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    private static BaseRequest mockExecutableRequest(String authHeader, HttpConnectContext connectContext) {
        BaseRequest request = mock(BaseRequest.class);
        HttpRequest httpRequest = mock(HttpRequest.class);
        when(httpRequest.uri()).thenReturn("/api/auth_probe");
        when(request.getRequest()).thenReturn(httpRequest);
        when(request.getAuthorizationHeader()).thenReturn(authHeader);
        when(request.getHostString()).thenReturn("10.4.5.6");
        when(request.getConnectContext()).thenReturn(connectContext);

        ChannelHandlerContext channelContext = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("10.4.5.6", 9030));
        when(channelContext.channel()).thenReturn(channel);
        when(request.getContext()).thenReturn(channelContext);
        return request;
    }

    @Test
    public void testExecuteWithBasicAuthCopiesAuthenticatedContext() throws Exception {
        TestableRestBaseAction action = new TestableRestBaseAction();
        HttpConnectContext connectContext = new HttpConnectContext();
        BaseRequest request = mockExecutableRequest(basicAuth("ldap_user", "secret"), connectContext);
        UserIdentity authenticatedUser = UserIdentity.createAnalyzedUserIdentWithIp("ldap_user", "%");
        Set<Long> roleIds = Set.of(10L, 20L);
        Set<String> groups = Set.of("ldap_admins", "ldap_ops");

        try (MockedStatic<AuthenticationHandler> mocked = mockStatic(AuthenticationHandler.class)) {
            mocked.when(() -> AuthenticationHandler.authenticate(any(ConnectContext.class),
                            eq("ldap_user"), eq("10.4.5.6"), any(byte[].class)))
                    .thenAnswer(invocation -> {
                        ConnectContext authCtx = invocation.getArgument(0);
                        byte[] passwordBytes = invocation.getArgument(3);
                        Assertions.assertArrayEquals("secret".getBytes(StandardCharsets.UTF_8), passwordBytes);
                        authCtx.setCurrentUserIdentity(authenticatedUser);
                        authCtx.setCurrentRoleIds(roleIds);
                        authCtx.setGroups(groups);
                        return authenticatedUser;
                    });

            action.execute(request, new BaseResponse());
        }

        Assertions.assertTrue(action.executed);
        Assertions.assertEquals(authenticatedUser, action.observedUser);
        Assertions.assertEquals(roleIds, action.observedRoleIds);
        Assertions.assertEquals(groups, action.observedGroups);
        Assertions.assertEquals("ldap_user", action.observedQualifiedUser);
        Assertions.assertEquals("10.4.5.6", action.observedRemoteIp);
    }

    @Test
    public void testExecuteWithAuthDisabledByActionBypassesAuthenticationHandler() throws Exception {
        NoAuthRestBaseAction action = new NoAuthRestBaseAction();
        BaseRequest request = mockExecutableRequest(null, new HttpConnectContext());

        try (MockedStatic<AuthenticationHandler> mocked = mockStatic(AuthenticationHandler.class)) {
            action.execute(request, new BaseResponse());
            mocked.verifyNoInteractions();
        }

        Assertions.assertTrue(action.executed);
        Assertions.assertNull(action.observedUser);
    }

    @Test
    public void testExecuteWithInvalidCredentialsThrowsAccessDeniedBeforeDispatch() throws Exception {
        TestableRestBaseAction action = new TestableRestBaseAction();
        BaseRequest request = mockExecutableRequest(basicAuth("bad_user", "bad_pwd"), new HttpConnectContext());

        try (MockedStatic<AuthenticationHandler> mocked = mockStatic(AuthenticationHandler.class)) {
            mocked.when(() -> AuthenticationHandler.authenticate(any(ConnectContext.class),
                            eq("bad_user"), eq("10.4.5.6"), any(byte[].class)))
                    .thenThrow(new AuthenticationException("bad credentials"));

            Assertions.assertThrows(AccessDeniedException.class, () -> action.execute(request, new BaseResponse()));
        }

        Assertions.assertFalse(action.executed);
    }

    @Test
    public void testAlwaysAnonymousRestActionsBypassBasicAuth() {
        Config.enable_http_auth = true;
        Assertions.assertFalse(new BootstrapFinishAction(null).needAuth());
        Assertions.assertFalse(new FeatureAction(null).needAuth());
        Assertions.assertFalse(new HealthAction(null).needAuth());
        Assertions.assertFalse(new GetSmallFileAction(null).needAuth());
        Assertions.assertFalse(new OAuth2Action(null).needAuth());
        Assertions.assertFalse(new IdleAction(null).needAuth());
        Assertions.assertFalse(new MetricsAction(null).needAuth());
    }

    @Test
    public void testCompatibilityGatedRestActionsFollowHttpAuthFlag() {
        Config.enable_http_auth = false;
        Assertions.assertFalse(new ConnectionAction(null).needAuth());
        Assertions.assertFalse(new ShowDataAction(null).needAuth());
        Assertions.assertFalse(new GetClusterSnapshotRestoreStateAction(null).needAuth());
        Assertions.assertFalse(new QueryProgressAction(null).needAuth());
        Assertions.assertFalse(new ShowMetaInfoAction(null).needAuth());
        Assertions.assertFalse(new ShowRuntimeInfoAction(null).needAuth());

        Config.enable_http_auth = true;
        Assertions.assertTrue(new ConnectionAction(null).needAuth());
        Assertions.assertTrue(new ShowDataAction(null).needAuth());
        Assertions.assertTrue(new GetClusterSnapshotRestoreStateAction(null).needAuth());
        Assertions.assertTrue(new QueryProgressAction(null).needAuth());
        Assertions.assertTrue(new ShowMetaInfoAction(null).needAuth());
        Assertions.assertTrue(new ShowRuntimeInfoAction(null).needAuth());
    }

    // -------- enable_http_auth-gated helpers --------

    private boolean savedEnableHttpAuth;
    private ConnectContext savedCtx;

    @BeforeEach
    public void saveHttpAuthFlag() {
        savedEnableHttpAuth = Config.enable_http_auth;
        savedCtx = ConnectContext.get();
    }

    @AfterEach
    public void restoreHttpAuthFlag() {
        Config.enable_http_auth = savedEnableHttpAuth;
        if (savedCtx == null) {
            ConnectContext.remove();
        } else {
            savedCtx.setThreadLocalInfo();
        }
    }

    private TestableRestBaseAction newTestableAction() {
        return new TestableRestBaseAction();
    }

    @Test
    public void testRequireOperate_disabled_isNoop() throws Exception {
        Config.enable_http_auth = false;
        ConnectContext.remove();
        newTestableAction().callRequireOperate();
    }

    @Test
    public void testRequireDbInsert_disabled_isNoop() throws Exception {
        Config.enable_http_auth = false;
        ConnectContext.remove();
        newTestableAction().callRequireDbInsert("any_db");
    }

    @Test
    public void testRequireOperate_enabled_callsAuthorizer() throws Exception {
        Config.enable_http_auth = true;
        new ConnectContext().setThreadLocalInfo();

        try (MockedStatic<Authorizer> mocked = mockStatic(Authorizer.class)) {
            // default no-throw = authorized
            newTestableAction().callRequireOperate();
            mocked.verify(() -> Authorizer.checkSystemAction(any(ConnectContext.class),
                    org.mockito.ArgumentMatchers.eq(PrivilegeType.OPERATE)));
        }
    }

    @Test
    public void testRequireOperate_enabled_denied_propagatesAccessDenied() throws Exception {
        Config.enable_http_auth = true;
        new ConnectContext().setThreadLocalInfo();

        try (MockedStatic<Authorizer> mocked = mockStatic(Authorizer.class)) {
            mocked.when(() -> Authorizer.checkSystemAction(any(ConnectContext.class),
                            org.mockito.ArgumentMatchers.eq(PrivilegeType.OPERATE)))
                    .thenThrow(new AccessDeniedException("operate denied"));

            Assertions.assertThrows(AccessDeniedException.class,
                    () -> newTestableAction().callRequireOperate());
        }
    }

    @Test
    public void testRequireDbInsert_enabled_denied_propagatesAccessDenied() throws Exception {
        Config.enable_http_auth = true;
        new ConnectContext().setThreadLocalInfo();

        try (MockedStatic<Authorizer> mocked = mockStatic(Authorizer.class)) {
            mocked.when(() -> Authorizer.checkActionInDb(any(ConnectContext.class),
                            org.mockito.ArgumentMatchers.eq("test_db"),
                            org.mockito.ArgumentMatchers.eq(PrivilegeType.INSERT)))
                    .thenThrow(new AccessDeniedException("db insert denied"));

            Assertions.assertThrows(AccessDeniedException.class,
                    () -> newTestableAction().callRequireDbInsert("test_db"));
        }
    }

    @Test
    public void testRequireOperate_enabled_checksOperateOnly() throws Exception {
        // Asserts requireOperateIfHttpAuthEnabled checks SYSTEM.OPERATE and
        // nothing else — in particular, the helper must not also try NODE as
        // a fallback. The NODE-denied stub would surface any such regression.
        Config.enable_http_auth = true;
        new ConnectContext().setThreadLocalInfo();

        try (MockedStatic<Authorizer> mocked = mockStatic(Authorizer.class)) {
            mocked.when(() -> Authorizer.checkSystemAction(any(ConnectContext.class),
                            org.mockito.ArgumentMatchers.eq(PrivilegeType.NODE)))
                    .thenThrow(new AccessDeniedException("must not be called"));

            newTestableAction().callRequireOperate();
            mocked.verify(() -> Authorizer.checkSystemAction(any(ConnectContext.class),
                    org.mockito.ArgumentMatchers.eq(PrivilegeType.OPERATE)));
            mocked.verify(() -> Authorizer.checkSystemAction(any(ConnectContext.class),
                    org.mockito.ArgumentMatchers.eq(PrivilegeType.NODE)), never());
        }
    }

    @Test
    public void testRequireDbInsert_enabled_callsCheckActionInDb() throws Exception {
        Config.enable_http_auth = true;
        new ConnectContext().setThreadLocalInfo();

        try (MockedStatic<Authorizer> mocked = mockStatic(Authorizer.class)) {
            newTestableAction().callRequireDbInsert("test_db");
            mocked.verify(() -> Authorizer.checkActionInDb(any(ConnectContext.class),
                    org.mockito.ArgumentMatchers.eq("test_db"),
                    org.mockito.ArgumentMatchers.eq(PrivilegeType.INSERT)));
        }
    }
}
