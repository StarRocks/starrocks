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

import com.starrocks.http.rest.HealthAction;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class HealthActionTest extends HttpServerTestUtils {

    @Test
    public void testAsyncHandleHealth() throws Exception {
        String uri = "/api/health";
        ActionController controller = new ActionController();
        MockHealthAction action = new MockHealthAction(controller);
        controller.registerHandler(HttpMethod.GET, uri, action);
        HttpServerTestUtils.MockExecutor executor = new HttpServerTestUtils.MockExecutor();

        executor.setRejectExecute(false);
        MockChannelHandlerContext context = createChannelHandlerContext();
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
        HttpServerHandler handler = new HttpServerHandler(controller, executor);
        assertEquals(1, ReferenceCountUtil.refCnt(request));
        assertEquals(0, action.executeCount());
        handler.channelRead(context, request);
        assertEquals(1, action.executeCount());
        assertEquals(0, ReferenceCountUtil.refCnt(request));
        assertEquals(0, executor.pendingTaskCount());
        assertEquals(0, context.numResponses());
        assertFalse(context.isFlushed());
    }

    private static class MockHealthAction extends HealthAction {
        private final AtomicInteger executeCount = new AtomicInteger(0);
        public MockHealthAction(ActionController controller) {
            super(controller);
        }

        @Override
        public void handleRequest(BaseRequest request) {
            executeCount.incrementAndGet();
        }

        int executeCount() {
            return executeCount.get();
        }
    }

}
