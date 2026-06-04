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

import com.starrocks.common.Config;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.rest.BootstrapFinishAction;
import com.starrocks.http.rest.ConnectionAction;
import com.starrocks.http.rest.FeatureAction;
import com.starrocks.http.rest.GetSmallFileAction;
import com.starrocks.http.rest.HealthAction;
import com.starrocks.http.rest.IdleAction;
import com.starrocks.http.rest.MetricsAction;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.ShowDataAction;
import com.starrocks.http.rest.ShowMetaInfoAction;
import com.starrocks.http.rest.ShowRuntimeInfoAction;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TNetworkAddress;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.net.URI;

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

    private boolean savedEnableHttpAuth;
    private ConnectContext savedCtx;

    static class TestableRestBaseAction extends RestBaseAction {
        public TestableRestBaseAction() {
            super(null);
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

    @Before
    public void setUp() {
        savedEnableHttpAuth = Config.enable_http_auth;
        savedCtx = ConnectContext.get();

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

    @After
    public void tearDown() {
        Config.enable_http_auth = savedEnableHttpAuth;
        if (savedCtx == null) {
            ConnectContext.remove();
        } else {
            savedCtx.setThreadLocalInfo();
        }
    }

    @Test
    public void testRedirectTo() throws Exception {
        URI expectedUri = new URI("http", null, "127.0.0.1", 8030, "/api/mydb/testStreamLoad测试/_stream_load", null, null);
        String asciiUri = expectedUri.toASCIIString();

        restBaseAction.redirectTo(mockRequest, mockResponse, mockAddr);
        verify(mockResponse).updateHeader(HttpHeaderNames.LOCATION.toString(), asciiUri);
    }

    @Test
    public void testAlwaysAnonymousRestActionsBypassBasicAuth() {
        // These endpoints are never gated by enable_http_auth (probes / internal token-protected).
        Config.enable_http_auth = true;
        Assert.assertFalse(new BootstrapFinishAction(null).needAuth());
        Assert.assertFalse(new FeatureAction(null).needAuth());
        Assert.assertFalse(new HealthAction(null).needAuth());
        Assert.assertFalse(new GetSmallFileAction(null).needAuth());
        Assert.assertFalse(new IdleAction(null).needAuth());
        Assert.assertFalse(new MetricsAction(null).needAuth());
    }

    @Test
    public void testCompatibilityGatedRestActionsFollowHttpAuthFlag() {
        // Historically-anonymous endpoints: require Basic only when the operator opts in.
        Config.enable_http_auth = false;
        Assert.assertFalse(new ConnectionAction(null).needAuth());
        Assert.assertFalse(new ShowDataAction(null).needAuth());
        Assert.assertFalse(new ShowMetaInfoAction(null).needAuth());
        Assert.assertFalse(new ShowRuntimeInfoAction(null).needAuth());

        Config.enable_http_auth = true;
        Assert.assertTrue(new ConnectionAction(null).needAuth());
        Assert.assertTrue(new ShowDataAction(null).needAuth());
        Assert.assertTrue(new ShowMetaInfoAction(null).needAuth());
        Assert.assertTrue(new ShowRuntimeInfoAction(null).needAuth());
    }

    // -------- enable_http_auth-gated helpers --------

    private TestableRestBaseAction newTestableAction() {
        return new TestableRestBaseAction();
    }

    // The enable_http_auth-gated helpers read the current user identity from the thread-local
    // ConnectContext; install a real identity so Authorizer argument matchers (any(UserIdentity))
    // match (any(UserIdentity.class) does not match a null first argument).
    private void setUpAuthedContext() {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setCurrentRoleIds(UserIdentity.ROOT);
        context.setThreadLocalInfo();
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
        setUpAuthedContext();

        try (MockedStatic<Authorizer> mocked = mockStatic(Authorizer.class)) {
            // default no-throw = authorized
            newTestableAction().callRequireOperate();
            mocked.verify(() -> Authorizer.checkSystemAction(any(UserIdentity.class), any(),
                    eq(PrivilegeType.OPERATE)));
        }
    }

    @Test
    public void testRequireOperate_enabled_denied_propagatesAccessDenied() throws Exception {
        Config.enable_http_auth = true;
        setUpAuthedContext();

        try (MockedStatic<Authorizer> mocked = mockStatic(Authorizer.class)) {
            mocked.when(() -> Authorizer.checkSystemAction(any(UserIdentity.class), any(),
                            eq(PrivilegeType.OPERATE)))
                    .thenThrow(new AccessDeniedException("operate denied"));

            Assert.assertThrows(AccessDeniedException.class,
                    () -> newTestableAction().callRequireOperate());
        }
    }

    @Test
    public void testRequireOperate_enabled_checksOperateOnly() throws Exception {
        // requireOperateIfHttpAuthEnabled checks SYSTEM.OPERATE and nothing else (must not also try NODE).
        Config.enable_http_auth = true;
        setUpAuthedContext();

        try (MockedStatic<Authorizer> mocked = mockStatic(Authorizer.class)) {
            mocked.when(() -> Authorizer.checkSystemAction(any(UserIdentity.class), any(),
                            eq(PrivilegeType.NODE)))
                    .thenThrow(new AccessDeniedException("must not be called"));

            newTestableAction().callRequireOperate();
            mocked.verify(() -> Authorizer.checkSystemAction(any(UserIdentity.class), any(),
                    eq(PrivilegeType.OPERATE)));
            mocked.verify(() -> Authorizer.checkSystemAction(any(UserIdentity.class), any(),
                    eq(PrivilegeType.NODE)), never());
        }
    }

    @Test
    public void testRequireDbInsert_enabled_denied_propagatesAccessDenied() throws Exception {
        Config.enable_http_auth = true;
        setUpAuthedContext();

        try (MockedStatic<Authorizer> mocked = mockStatic(Authorizer.class)) {
            mocked.when(() -> Authorizer.checkActionInDb(any(UserIdentity.class), any(),
                            eq("test_db"), eq(PrivilegeType.INSERT)))
                    .thenThrow(new AccessDeniedException("db insert denied"));

            Assert.assertThrows(AccessDeniedException.class,
                    () -> newTestableAction().callRequireDbInsert("test_db"));
        }
    }

    @Test
    public void testRequireDbInsert_enabled_callsCheckActionInDb() throws Exception {
        Config.enable_http_auth = true;
        setUpAuthedContext();

        try (MockedStatic<Authorizer> mocked = mockStatic(Authorizer.class)) {
            newTestableAction().callRequireDbInsert("test_db");
            mocked.verify(() -> Authorizer.checkActionInDb(any(UserIdentity.class), any(),
                    eq("test_db"), eq(PrivilegeType.INSERT)));
        }
    }
}
