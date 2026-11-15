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

import com.starrocks.authentication.AuthCredentialKind;
import com.starrocks.authentication.AuthCredentials;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.http.rest.MetricsAction;
import com.starrocks.qe.ConnectContext;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.Credentials;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class MetricsActionTest {
    public static class MockMetricsAction extends MetricsAction {
        public MockMetricsAction(ActionController controller) {
            super(controller);
        }

        public MetricsAction.RequestParams  callParseRequestParams(BaseRequest request) {
            return this.parseRequestParams(request);
        }
    }

    private BaseRequest buildBaseRequest(String uri, boolean includeAuth) {
        DefaultHttpHeaders headers = new DefaultHttpHeaders();
        if (includeAuth) {
            headers.add("Authorization", Credentials.basic("root", ""));
        }
        DefaultHttpRequest rawRequest =
                new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri, headers);
        return new BaseRequest(null, rawRequest);
    }

    @Test
    public void testParseRequestParams() {
        ActionController controller = new ActionController();
        MockMetricsAction action = new MockMetricsAction(controller);

        // All default, no parameter
        {
            BaseRequest request = buildBaseRequest("/metrics", true);
            new Expectations(request) {
                {
                    request.getAuthorizationHeader();
                    result = new Exception("Don't expect check auth header");
                    minTimes = 0;
                    maxTimes = 0;
                }
            };
            MetricsAction.RequestParams params = action.callParseRequestParams(request);
            Assertions.assertNotNull(params);
            Assertions.assertFalse(params.isCollectMVMetrics());
            Assertions.assertFalse(params.isMinifyMVMetrics());
            Assertions.assertFalse(params.isCollectTableMetrics());
            Assertions.assertFalse(params.isMinifyTableMetrics());
        }

        // All default, want table_metrics, but no auth
        {
            BaseRequest request = buildBaseRequest("/metrics?with_table_metrics=all", false);
            // expect check auth
            new Expectations(request) {
                {
                    request.getAuthorizationHeader();
                    minTimes = 1;
                }
            };
            MetricsAction.RequestParams params = action.callParseRequestParams(request);
            Assertions.assertNotNull(params);
            // no auth should not access mv metrics
            Assertions.assertFalse(params.isCollectMVMetrics());
            Assertions.assertFalse(params.isMinifyMVMetrics());
            // no auth should not access table metrics
            Assertions.assertFalse(params.isCollectTableMetrics());
            Assertions.assertFalse(params.isMinifyTableMetrics());
        }

        // All default, has query parameter, has auth, only with_table_metrics
        {
            BaseRequest request = buildBaseRequest("/metrics?with_table_metrics=all", true);
            // expect check auth
            new Expectations(request) {
                {
                    // auth passed, retrieve the remote host from the request context
                    request.getHostString();
                    result = "127.0.0.1";
                }
            };
            MetricsAction.RequestParams params = action.callParseRequestParams(request);
            Assertions.assertNotNull(params);
            Assertions.assertFalse(params.isCollectMVMetrics());
            Assertions.assertFalse(params.isMinifyMVMetrics());
            Assertions.assertTrue(params.isCollectTableMetrics());
            Assertions.assertFalse(params.isMinifyTableMetrics());
        }

        // All default, has query parameter, has auth, both with_table_metrics and with_materialized_view_metrics
        {
            BaseRequest request =
                    buildBaseRequest("/metrics?with_table_metrics=all&with_materialized_view_metrics=all", true);
            new Expectations(request) {
                {
                    // auth passed, retrieve the remote host from the request context
                    request.getHostString();
                    result = "127.0.0.1";
                }
            };
            MetricsAction.RequestParams params = action.callParseRequestParams(request);
            Assertions.assertNotNull(params);
            Assertions.assertTrue(params.isCollectMVMetrics());
            Assertions.assertFalse(params.isMinifyMVMetrics());
            Assertions.assertTrue(params.isCollectTableMetrics());
            Assertions.assertFalse(params.isMinifyTableMetrics());
        }

        // All default, want table_metrics, but no auth
        {
            BaseRequest request = buildBaseRequest("/metrics?with_materialized_view_metrics=all", false);
            // expect check auth
            new Expectations(request) {
                {
                    request.getAuthorizationHeader();
                    minTimes = 1;
                }
            };
            MetricsAction.RequestParams params = action.callParseRequestParams(request);
            Assertions.assertNotNull(params);
            // no auth should not access mv metrics
            Assertions.assertFalse(params.isCollectMVMetrics());
            Assertions.assertFalse(params.isMinifyMVMetrics());
            // no auth should not access table metrics
            Assertions.assertFalse(params.isCollectTableMetrics());
            Assertions.assertFalse(params.isMinifyTableMetrics());
        }

        // All default, has query parameter, has auth, only with_materialized_view_metrics
        {
            BaseRequest request = buildBaseRequest("/metrics?with_materialized_view_metrics=all", true);
            // expect check auth
            new Expectations(request) {
                {
                    // auth passed, retrieve the remote host from the request context
                    request.getHostString();
                    result = "127.0.0.1";
                }
            };
            MetricsAction.RequestParams params = action.callParseRequestParams(request);
            Assertions.assertNotNull(params);
            Assertions.assertTrue(params.isCollectMVMetrics());
            Assertions.assertFalse(params.isMinifyMVMetrics());
            Assertions.assertFalse(params.isCollectTableMetrics());
            Assertions.assertFalse(params.isMinifyTableMetrics());
        }

        // All default, has query parameter, has auth, only with_materialized_view_metrics
        {
            BaseRequest request = buildBaseRequest("/metrics?with_materialized_view_metrics=minified", true);
            // expect check auth
            new Expectations(request) {
                {
                    // auth passed, retrieve the remote host from the request context
                    request.getHostString();
                    result = "127.0.0.1";
                }
            };
            MetricsAction.RequestParams params = action.callParseRequestParams(request);
            Assertions.assertNotNull(params);
            Assertions.assertTrue(params.isCollectMVMetrics());
            Assertions.assertTrue(params.isMinifyMVMetrics());
            Assertions.assertFalse(params.isCollectTableMetrics());
            Assertions.assertFalse(params.isMinifyTableMetrics());
        }

        // All default, has query parameter, has auth, both with_table_metrics and with_materialized_view_metrics
        {
            BaseRequest request =
                    buildBaseRequest("/metrics?with_table_metrics=minified&with_materialized_view_metrics=minified", true);
            new Expectations(request) {
                {
                    // auth passed, retrieve the remote host from the request context
                    request.getHostString();
                    result = "127.0.0.1";
                }
            };
            MetricsAction.RequestParams params = action.callParseRequestParams(request);
            Assertions.assertNotNull(params);
            Assertions.assertTrue(params.isCollectMVMetrics());
            Assertions.assertTrue(params.isMinifyMVMetrics());
            Assertions.assertTrue(params.isCollectTableMetrics());
            Assertions.assertTrue(params.isMinifyTableMetrics());
        }
    }

    @Test
    public void testParseRequestParamsWithUserConnections() {
        ActionController controller = new ActionController();
        MockMetricsAction action = new MockMetricsAction(controller);

        // Test with user connections parameter but no auth
        {
            BaseRequest request = buildBaseRequest("/metrics?with_user_connections=all", false);
            new Expectations(request) {
                {
                    request.getAuthorizationHeader();
                    minTimes = 1;
                }
            };
            MetricsAction.RequestParams params = action.callParseRequestParams(request);
            Assertions.assertNotNull(params);
            Assertions.assertFalse(params.isCollectUserConnMetrics());
        }

        // Test with user connections parameter and auth
        {
            BaseRequest request = buildBaseRequest("/metrics?with_user_connections=all", true);
            new Expectations(request) {
                {
                    request.getHostString();
                    result = "127.0.0.1";
                }
            };
            MetricsAction.RequestParams params = action.callParseRequestParams(request);
            Assertions.assertNotNull(params);
            Assertions.assertTrue(params.isCollectUserConnMetrics());
        }

        // Test combined parameters
        {
            BaseRequest request = buildBaseRequest("/metrics?with_table_metrics=all&with_user_connections=all", true);
            new Expectations(request) {
                {
                    request.getHostString();
                    result = "127.0.0.1";
                }
            };
            MetricsAction.RequestParams params = action.callParseRequestParams(request);
            Assertions.assertNotNull(params);
            Assertions.assertTrue(params.isCollectTableMetrics());
            Assertions.assertFalse(params.isMinifyTableMetrics());
            Assertions.assertTrue(params.isCollectUserConnMetrics());
        }
    }

    /**
     * A security-integration user owns no roles of its own: db_admin and user_admin arrive through
     * its groups and sit on the context authentication wrote to. /metrics must authorize on that
     * context; the user-owned role table does not even know the user.
     */
    @Test
    public void testAnAdminRoleMappedFromAGroupUnlocksTheTableMetrics() {
        MockMetricsAction action = new MockMetricsAction(new ActionController());
        new MockUp<AuthenticationHandler>() {
            @Mock
            public UserIdentity authenticate(ConnectContext ctx, AuthCredentials cred) {
                UserIdentity siUser = UserIdentity.createEphemeralUserIdent("si_admin", "%");
                ctx.setCurrentUserIdentity(siUser);
                ctx.setQualifiedUser("si_admin");
                ctx.setCurrentRoleIds(Set.of(PrivilegeBuiltinConstants.DB_ADMIN_ROLE_ID,
                        PrivilegeBuiltinConstants.USER_ADMIN_ROLE_ID));
                return siUser;
            }
        };

        DefaultHttpHeaders headers = new DefaultHttpHeaders();
        headers.add("Authorization", "Bearer good.jwt.token");
        BaseRequest request = new BaseRequest(null, new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/metrics?with_table_metrics=all", headers));
        new Expectations(request) {
            {
                request.getHostString();
                result = "127.0.0.1";
                minTimes = 0;
            }
        };
        Assertions.assertTrue(action.callParseRequestParams(request).isCollectTableMetrics(),
                "the roles on the authenticated context decide, not the user-owned role table");
    }

    @Test
    public void testBearerTokenUnlocksTableMetricsAndABadOneDegradesSilently() {
        // TC-L3-46-08: /metrics reads Basic or Bearer through the same parser as every other FE
        // endpoint. A verified token grants the table metrics; a refused one falls back to the base
        // metrics without an error, exactly as a wrong Basic password always has.
        ActionController controller = new ActionController();
        MockMetricsAction action = new MockMetricsAction(controller);
        new MockUp<AuthenticationHandler>() {
            @Mock
            public UserIdentity authenticate(ConnectContext ctx, AuthCredentials cred) throws AuthenticationException {
                if (cred.kind() != AuthCredentialKind.BEARER_TOKEN || !"good.jwt.token".equals(cred.tokenString())) {
                    throw new AuthenticationException("refused");
                }
                ctx.setCurrentUserIdentity(UserIdentity.ROOT);
                ctx.setQualifiedUser("root");
                return UserIdentity.ROOT;
            }
        };
        new MockUp<MetricsAction>() {
            @Mock
            protected void checkUserOwnsAdminRole(UserIdentity currentUser) {
            }
        };

        DefaultHttpHeaders good = new DefaultHttpHeaders();
        good.add("Authorization", "Bearer good.jwt.token");
        BaseRequest okRequest = new BaseRequest(null, new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/metrics?with_table_metrics=all", good));
        new Expectations(okRequest) {
            {
                okRequest.getHostString();
                result = "127.0.0.1";
                minTimes = 0;
            }
        };
        Assertions.assertTrue(action.callParseRequestParams(okRequest).isCollectTableMetrics());

        DefaultHttpHeaders bad = new DefaultHttpHeaders();
        bad.add("Authorization", "Bearer forged.jwt.token");
        BaseRequest badRequest = new BaseRequest(null, new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/metrics?with_table_metrics=all", bad));
        new Expectations(badRequest) {
            {
                badRequest.getHostString();
                result = "127.0.0.1";
                minTimes = 0;
            }
        };
        Assertions.assertFalse(action.callParseRequestParams(badRequest).isCollectTableMetrics(),
                "a refused token degrades to the base metrics, it does not fail the scrape");
    }
}
