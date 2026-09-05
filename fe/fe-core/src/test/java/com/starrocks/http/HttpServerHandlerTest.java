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

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HttpServerHandlerTest extends HttpServerTestUtils {

    @Test
    public void testSyncHandle() throws Exception {
        String uri = "/test/handler";
        ActionController controller = new ActionController();
        MockAction action = new MockAction(controller, false);
        controller.registerHandler(HttpMethod.GET, uri, action);
        MockExecutor executor = new MockExecutor();

        // handle successfully
        {
            action.setException(false);
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

        // handle failed
        {
            action.setException(true);
            MockChannelHandlerContext context = createChannelHandlerContext();
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
            HttpServerHandler handler = new HttpServerHandler(controller, executor);
            assertEquals(1, ReferenceCountUtil.refCnt(request));
            assertEquals(1, action.executeCount());
            handler.channelRead(context, request);
            assertEquals(2, action.executeCount());
            assertEquals(0, ReferenceCountUtil.refCnt(request));
            assertEquals(0, executor.pendingTaskCount());
            assertEquals(1, context.numResponses());
            assertFalse(context.isFlushed());
            verifyResponse(context.pollResponse(), HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    "mock exception");
        }
    }

    @Test
    public void testAsyncHandle() throws Exception {
        String uri = "/test/handler";
        ActionController controller = new ActionController();
        MockAction action = new MockAction(controller, true);
        controller.registerHandler(HttpMethod.GET, uri, action);
        MockExecutor executor = new MockExecutor();

        // handle successfully
        {
            action.setException(false);
            executor.setRejectExecute(false);
            MockChannelHandlerContext context = createChannelHandlerContext();
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
            HttpServerHandler handler = new HttpServerHandler(controller, executor);
            assertEquals(0, action.executeCount());
            assertEquals(1, ReferenceCountUtil.refCnt(request));
            handler.channelRead(context, request);
            assertEquals(0, action.executeCount());
            assertEquals(1, executor.pendingTaskCount());
            assertEquals(0, context.numResponses());
            assertFalse(context.isFlushed());
            assertEquals(1, ReferenceCountUtil.refCnt(request));

            // run the async task
            executor.runOneTask();
            assertEquals(1, action.executeCount());
            assertEquals(0, executor.pendingTaskCount());
            assertEquals(0, context.numResponses());
            assertTrue(context.isFlushed());
            assertEquals(0, ReferenceCountUtil.refCnt(request));
        }

        // handle failed
        {
            action.setException(true);
            executor.setRejectExecute(false);
            MockChannelHandlerContext context = createChannelHandlerContext();
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
            HttpServerHandler handler = new HttpServerHandler(controller, executor);
            assertEquals(1, action.executeCount());
            assertEquals(1, ReferenceCountUtil.refCnt(request));
            handler.channelRead(context, request);
            assertEquals(1, action.executeCount());
            assertEquals(1, executor.pendingTaskCount());
            assertEquals(0, context.numResponses());
            assertFalse(context.isFlushed());
            assertEquals(1, ReferenceCountUtil.refCnt(request));

            // run the async task
            executor.runOneTask();
            assertEquals(2, action.executeCount());
            assertEquals(0, executor.pendingTaskCount());
            assertEquals(1, context.numResponses());
            assertTrue(context.isFlushed());
            assertEquals(0, ReferenceCountUtil.refCnt(request));
            verifyResponse(context.pollResponse(), HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    "mock exception");
        }

        // submit async task failed
        {
            action.setException(false);
            executor.setRejectExecute(true);
            MockChannelHandlerContext context = createChannelHandlerContext();
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
            HttpServerHandler handler = new HttpServerHandler(controller, executor);
            assertEquals(2, action.executeCount());
            assertEquals(1, ReferenceCountUtil.refCnt(request));
            handler.channelRead(context, request);
            assertEquals(2, action.executeCount());
            assertEquals(0, executor.pendingTaskCount());
            assertEquals(1, context.numResponses());
            assertFalse(context.isFlushed());
            assertEquals(0, ReferenceCountUtil.refCnt(request));
            verifyResponse(context.pollResponse(), HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    "mock reject");
        }
    }

    private void verifyResponse(Object response, HttpResponseStatus expectStatus, String expectContent) {
        assertInstanceOf(DefaultFullHttpResponse.class, response);
        DefaultFullHttpResponse httpResponse = (DefaultFullHttpResponse) response;
        assertEquals(expectStatus, httpResponse.status());
        assertTrue(httpResponse.content().toString(StandardCharsets.UTF_8).contains(expectContent));
    }



    private static class MockAction extends BaseAction {

        private final boolean async;
        private boolean exception = false;
        private final AtomicInteger executeCount = new AtomicInteger(0);

        public MockAction(ActionController controller, boolean async) {
            super(controller);
            this.async = async;
        }

        public void setException(boolean exception) {
            this.exception = exception;
        }

        @Override
        public boolean supportAsyncHandler() {
            return async;
        }

        @Override
        public void handleRequest(BaseRequest request) {
            executeCount.incrementAndGet();
            if (exception) {
                throw new RuntimeException("mock exception");
            }
        }

        @Override
        public void execute(BaseRequest request, BaseResponse response) {
            throw new UnsupportedOperationException();
        }

        int executeCount() {
            return executeCount.get();
        }
    }




}
