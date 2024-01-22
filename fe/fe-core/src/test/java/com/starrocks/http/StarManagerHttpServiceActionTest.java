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

import com.staros.manager.HttpService;
import com.staros.manager.StarManager;
import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.rest.StarManagerHttpServiceAction;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.staros.StarMgrServer;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

public class StarManagerHttpServiceActionTest {
    @Test
    public void testexecuteWithoutPassword() throws Exception {
        ActionController controller = new ActionController();
        StarManagerHttpServiceAction starManagerHttpServiceAction = new StarManagerHttpServiceAction(controller);
        starManagerHttpServiceAction.registerAction(controller);

        BaseRequest request = new BaseRequest(null, null, null);
        BaseResponse response = new BaseResponse();

        ConnectContext connectContext = new ConnectContext();
        UserIdentity userIdentity = new UserIdentity();
        StarMgrServer starMgrServer = new StarMgrServer();
        StarManager starManager = new StarManager();
        HttpService httpService = new HttpService(starManager);

        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                return connectContext;
            }
            public UserIdentity getCurrentUserIdentity() {
                return userIdentity;
            }
        };
        new MockUp<StarMgrServer>() {
            @Mock
            public StarMgrServer getCurrentState() {
                return starMgrServer;
            }
            @Mock
            public HttpService getHttpService() {
                return httpService;
            }
        };
        new MockUp<StarManagerHttpServiceAction>() {
            @Mock
            public void sendResult(BaseRequest request, BaseResponse response) {}

            @Mock
            void checkUserOwnsAdminRole(UserIdentity currentUser) {}

            @Mock
            void writeResponse(BaseRequest request, BaseResponse response, HttpResponseStatus status) {}
        };

        { // normal case
            new MockUp<HttpService>() {
                @Mock
                public HttpResponse starmgrHttpService(HttpRequest request) {
                    String str = "content";
                    FullHttpResponse response = new DefaultFullHttpResponse(
                            HttpVersion.HTTP_1_1,
                            HttpResponseStatus.OK,
                            Unpooled.copiedBuffer(str.getBytes()));

                    response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN);
                    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
                    return response;
                }
                @Mock
                void writeResponse(BaseRequest request, BaseResponse response, HttpResponseStatus status) {}
            };
            starManagerHttpServiceAction.executeWithoutPassword(request, response);
        }
        { // Internal error case
            new MockUp<HttpService>() {
                @Mock
                public HttpResponse starmgrHttpService(HttpRequest request) {
                    HttpResponse response = new DefaultHttpResponse(
                            HttpVersion.HTTP_1_1,
                            HttpResponseStatus.OK);
                    response.headers().set("Content-Type", "text/plain; charset=UTF-8");
                    return response;
                }
            };
            DdlException ddlException = Assert.assertThrows(DdlException.class, () ->
                    starManagerHttpServiceAction.executeWithoutPassword(request, response));
            Assert.assertEquals(ddlException.getMessage(), "Inernal Error");
        }
    }
}