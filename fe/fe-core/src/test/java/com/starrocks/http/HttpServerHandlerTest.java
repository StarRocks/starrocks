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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.Attribute;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

    /**
     * The SQL actions on a channel share its ConnectContext. Two pipelined requests must not run on it at
     * once: the second starts only after the first has finished, whatever the executor's parallelism, since the
     * requests of one connection are handled one at a time.
     */
    @Test
    public void testSqlRequestsOnOneChannelRunOneAtATime() throws Exception {
        String uri = "/test/sql";
        ActionController controller = new ActionController();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicInteger running = new AtomicInteger();
        AtomicInteger overlapped = new AtomicInteger();
        CountDownLatch bothHandled = new CountDownLatch(2);
        BaseAction sqlAction = new BaseAction(controller) {
            @Override
            public boolean isSqlAction() {
                return true;
            }

            @Override
            public boolean supportAsyncHandler() {
                return true;
            }

            @Override
            public void handleRequest(BaseRequest request) {
                if (running.incrementAndGet() > 1) {
                    overlapped.incrementAndGet();
                }
                try {
                    firstStarted.countDown();
                    releaseFirst.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    running.decrementAndGet();
                    bothHandled.countDown();
                }
            }

            @Override
            public void execute(BaseRequest request, BaseResponse response) {
            }
        };
        controller.registerHandler(HttpMethod.GET, uri, sqlAction);

        HttpConnectContext shared = new HttpConnectContext();
        Attribute<HttpConnectContext> attribute = Mockito.mock(Attribute.class);
        Mockito.when(attribute.get()).thenReturn(shared);
        Channel channel = Mockito.mock(Channel.class);
        Mockito.when(channel.attr(Mockito.same(HttpServerHandler.HTTP_SQL_CONNECT_CONTEXT_ATTRIBUTE_KEY))).thenReturn(attribute);
        Mockito.when(channel.isActive()).thenReturn(true);
        ChannelHandlerContext context = Mockito.mock(ChannelHandlerContext.class);
        Mockito.when(context.channel()).thenReturn(channel);
        Mockito.when(context.executor()).thenReturn(ImmediateEventExecutor.INSTANCE);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            HttpServerHandler handler = new HttpServerHandler(controller, pool);
            handler.channelRead(context, new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri));
            assertTrue(firstStarted.await(5, TimeUnit.SECONDS));
            handler.channelRead(context, new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri));
            Thread.sleep(300);
            assertEquals(1, running.get(), "the second request waits for the first");
            releaseFirst.countDown();
            // The second request goes to the executor only once the first is done.
            assertTrue(bothHandled.await(5, TimeUnit.SECONDS));
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
            assertEquals(0, overlapped.get(), "no two SQL requests of one channel ran at the same time");
        } finally {
            pool.shutdownNow();
        }
    }

    private static DefaultFullHttpRequest get(String uri) {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    }

    /**
     * A client that pipelines requests gets them handled one at a time, in the order sent. A synchronous action
     * pipelined behind one still running on the executor used to answer at once on the event loop, inside the
     * first response; it now waits until the first is done.
     */
    @Test
    public void testPipelinedRequestWaitsForTheOneBefore() throws Exception {
        ActionController controller = new ActionController();
        MockAction streaming = new MockAction(controller, true);
        MockAction health = new MockAction(controller, false);
        controller.registerHandler(HttpMethod.GET, "/test/stream", streaming);
        controller.registerHandler(HttpMethod.GET, "/test/health", health);
        MockExecutor executor = new MockExecutor();
        MockChannelHandlerContext context = createChannelHandlerContext();
        HttpServerHandler handler = new HttpServerHandler(controller, executor);
        DefaultFullHttpRequest first = get("/test/stream");
        DefaultFullHttpRequest second = get("/test/health");

        handler.channelRead(context, first);
        handler.channelRead(context, second);

        assertEquals(0, streaming.executeCount());
        assertEquals(0, health.executeCount(), "the second request waits for the first");
        assertEquals(1, ReferenceCountUtil.refCnt(second), "a waiting request is kept until it is handled");

        executor.runOneTask();

        assertEquals(1, streaming.executeCount());
        assertEquals(1, health.executeCount(), "the second request runs once the first is done");
        assertEquals(0, executor.pendingTaskCount());
        assertEquals(0, ReferenceCountUtil.refCnt(first));
        assertEquals(0, ReferenceCountUtil.refCnt(second));
    }

    /**
     * Each pipelined request is handled by the action its own uri maps to, even when the next request on the
     * connection is read before the executor gets to it.
     */
    @Test
    public void testPipelinedAsyncRequestsAreEachHandledByTheirOwnAction() throws Exception {
        ActionController controller = new ActionController();
        MockAction first = new MockAction(controller, true);
        MockAction second = new MockAction(controller, true);
        controller.registerHandler(HttpMethod.GET, "/test/first", first);
        controller.registerHandler(HttpMethod.GET, "/test/second", second);
        MockExecutor executor = new MockExecutor();
        MockChannelHandlerContext context = createChannelHandlerContext();
        HttpServerHandler handler = new HttpServerHandler(controller, executor);

        handler.channelRead(context, get("/test/first"));
        handler.channelRead(context, get("/test/second"));
        executor.runOneTask();

        assertEquals(1, first.executeCount());
        assertEquals(0, second.executeCount());

        executor.runOneTask();

        assertEquals(1, first.executeCount());
        assertEquals(1, second.executeCount());
    }

    @Test
    public void testRequestsWaitingWhenTheConnectionClosesAreReleased() throws Exception {
        ActionController controller = new ActionController();
        MockAction streaming = new MockAction(controller, true);
        MockAction health = new MockAction(controller, false);
        controller.registerHandler(HttpMethod.GET, "/test/stream", streaming);
        controller.registerHandler(HttpMethod.GET, "/test/health", health);
        MockExecutor executor = new MockExecutor();
        MockChannelHandlerContext context = createChannelHandlerContext();
        HttpServerHandler handler = new HttpServerHandler(controller, executor);
        DefaultFullHttpRequest second = get("/test/health");

        handler.channelActive(context);
        handler.channelRead(context, get("/test/stream"));
        handler.channelRead(context, second);
        handler.channelInactive(context);

        assertEquals(0, ReferenceCountUtil.refCnt(second));
        executor.runOneTask();
        assertEquals(1, streaming.executeCount());
        assertEquals(0, health.executeCount(), "nothing is handled for a connection that is gone");
    }

    /**
     * A client cannot pile requests up in memory behind a slow one: past a few waiting requests it loses the
     * connection.
     */
    @Test
    public void testTooManyWaitingRequestsCloseTheConnection() throws Exception {
        ActionController controller = new ActionController();
        MockAction streaming = new MockAction(controller, true);
        MockAction health = new MockAction(controller, false);
        controller.registerHandler(HttpMethod.GET, "/test/stream", streaming);
        controller.registerHandler(HttpMethod.GET, "/test/health", health);
        MockExecutor executor = new MockExecutor();
        MockChannelHandlerContext context = createChannelHandlerContext();
        HttpServerHandler handler = new HttpServerHandler(controller, executor);

        handler.channelRead(context, get("/test/stream"));
        for (int i = 0; i < 8; i++) {
            handler.channelRead(context, get("/test/health"));
        }
        Mockito.verify(context, Mockito.never()).close();
        DefaultFullHttpRequest ninth = get("/test/health");
        handler.channelRead(context, ninth);

        Mockito.verify(context).close();
        assertEquals(0, ReferenceCountUtil.refCnt(ninth));
        assertEquals(0, health.executeCount());
    }

    @Test
    public void testWaitingRequestsOfAClosedConnectionAreNotHandled() throws Exception {
        ActionController controller = new ActionController();
        MockAction streaming = new MockAction(controller, true);
        MockAction health = new MockAction(controller, false);
        controller.registerHandler(HttpMethod.GET, "/test/stream", streaming);
        controller.registerHandler(HttpMethod.GET, "/test/health", health);
        MockExecutor executor = new MockExecutor();
        MockChannelHandlerContext context = createChannelHandlerContext();
        HttpServerHandler handler = new HttpServerHandler(controller, executor);
        DefaultFullHttpRequest second = get("/test/health");

        handler.channelRead(context, get("/test/stream"));
        handler.channelRead(context, second);
        // The connection goes away while the first request runs, before netty reports it inactive.
        Mockito.when(context.channel().isActive()).thenReturn(false);
        executor.runOneTask();

        assertEquals(1, streaming.executeCount());
        assertEquals(0, health.executeCount());
        assertEquals(0, ReferenceCountUtil.refCnt(second));
    }

    @Test
    public void testErrorInAWaitingRequestClosesTheConnection() throws Exception {
        ActionController controller = new ActionController();
        MockAction streaming = new MockAction(controller, true);
        MockAction health = new MockAction(controller, false);
        BaseAction broken = new BaseAction(controller) {
            @Override
            public boolean supportAsyncHandler() {
                return false;
            }

            @Override
            public void handleRequest(BaseRequest request) {
                throw new AssertionError("broken action");
            }

            @Override
            public void execute(BaseRequest request, BaseResponse response) {
            }
        };
        controller.registerHandler(HttpMethod.GET, "/test/stream", streaming);
        controller.registerHandler(HttpMethod.GET, "/test/broken", broken);
        controller.registerHandler(HttpMethod.GET, "/test/health", health);
        MockExecutor executor = new MockExecutor();
        MockChannelHandlerContext context = createChannelHandlerContext();
        HttpServerHandler handler = new HttpServerHandler(controller, executor);
        DefaultFullHttpRequest third = get("/test/health");

        handler.channelRead(context, get("/test/stream"));
        handler.channelRead(context, get("/test/broken"));
        handler.channelRead(context, third);
        executor.runOneTask();

        Mockito.verify(context).close();
        assertEquals(0, health.executeCount(), "nothing more is handled on a connection being closed");
        handler.channelInactive(context);
        assertEquals(0, ReferenceCountUtil.refCnt(third));
    }

    @Test
    public void testWaitingRequestWithABadUriIsAnswered() throws Exception {
        ActionController controller = new ActionController();
        MockAction streaming = new MockAction(controller, true);
        controller.registerHandler(HttpMethod.GET, "/test/stream", streaming);
        MockExecutor executor = new MockExecutor();
        MockChannelHandlerContext context = createChannelHandlerContext();
        HttpServerHandler handler = new HttpServerHandler(controller, executor);
        DefaultFullHttpRequest bad = get("/test/%ZZ");

        handler.channelRead(context, get("/test/stream"));
        handler.channelRead(context, bad);
        assertEquals(0, context.numResponses());
        executor.runOneTask();

        verifyResponse(context.pollResponse(), HttpResponseStatus.BAD_REQUEST, "Invalid request uri");
        assertEquals(0, ReferenceCountUtil.refCnt(bad));
    }

    @Test
    public void testWaitingRequestTheExecutorRejectsIsAnswered() throws Exception {
        ActionController controller = new ActionController();
        MockAction streaming = new MockAction(controller, true);
        MockAction health = new MockAction(controller, false);
        controller.registerHandler(HttpMethod.GET, "/test/stream", streaming);
        controller.registerHandler(HttpMethod.GET, "/test/health", health);
        MockExecutor executor = new MockExecutor();
        MockChannelHandlerContext context = createChannelHandlerContext();
        HttpServerHandler handler = new HttpServerHandler(controller, executor);

        handler.channelRead(context, get("/test/stream"));
        handler.channelRead(context, get("/test/stream"));
        handler.channelRead(context, get("/test/health"));
        executor.setRejectExecute(true);
        executor.runOneTask();

        // The second request cannot go to the executor: it is answered with an error, and the third still runs.
        verifyResponse(context.pollResponse(), HttpResponseStatus.INTERNAL_SERVER_ERROR, "mock reject");
        assertEquals(1, streaming.executeCount());
        assertEquals(1, health.executeCount());
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
