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

import com.starrocks.http.rest.MetricsAction;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import mockit.Expectations;
import okhttp3.Credentials;
import org.junit.Assert;
import org.junit.Test;

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
        return new BaseRequest(null, rawRequest, null);
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
            Assert.assertNotNull(params);
            Assert.assertFalse(params.isCollectMVMetrics());
            Assert.assertTrue(params.isMinifyMVMetrics());
            Assert.assertFalse(params.isCollectTableMetrics());
            Assert.assertTrue(params.isMinifyTableMetrics());
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
            Assert.assertNotNull(params);
            Assert.assertFalse(params.isCollectMVMetrics());
            Assert.assertTrue(params.isMinifyMVMetrics());
            // still minified way collecting table metrics
            Assert.assertFalse(params.isCollectTableMetrics());
            Assert.assertTrue(params.isMinifyTableMetrics());
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
            Assert.assertNotNull(params);
            Assert.assertFalse(params.isCollectMVMetrics());
            Assert.assertTrue(params.isMinifyMVMetrics());
            Assert.assertTrue(params.isCollectTableMetrics());
            Assert.assertFalse(params.isMinifyTableMetrics());
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
            Assert.assertNotNull(params);
            Assert.assertTrue(params.isCollectMVMetrics());
            Assert.assertFalse(params.isMinifyMVMetrics());
            Assert.assertTrue(params.isCollectTableMetrics());
            Assert.assertFalse(params.isMinifyTableMetrics());
        }
    }
}
