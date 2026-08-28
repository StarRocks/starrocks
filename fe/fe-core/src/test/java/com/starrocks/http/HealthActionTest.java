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

import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.http.rest.HealthAction;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.GracefulExitFlag;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.SystemInfoService;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    @Test
    public void testHealthProbeDuringGracefulExitReturns500(@Mocked GracefulExitFlag gracefulExitFlag)
            throws Exception {
        // While graceful exit is in progress, a health probe must be answered with 500 so the
        // upstream Load Balancer stops routing new connections (and logs a probe sample).
        new Expectations() {
            {
                GracefulExitFlag.isGracefulExit();
                result = true;
                GracefulExitFlag.shouldAcceptNewRequest();
                result = false;
            }
        };

        ChannelFuture channelFuture = mock(ChannelFuture.class);
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        when(context.write(any())).thenReturn(channelFuture);
        BaseRequest request = mock(BaseRequest.class);
        when(request.getHostString()).thenReturn("127.0.0.1");
        when(request.getRequest())
                .thenReturn(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/api/health"));
        when(request.getContext()).thenReturn(context);

        HealthAction action = new HealthAction(new ActionController());
        Deencapsulation.invoke(action, "executeWithoutPassword", request, new BaseResponse());
    }

    @Test
    public void testHealthProbeNormalPathReturnsBackendInfo() throws Exception {
        // Outside graceful exit, the probe returns backend counts; the summary counter
        // aggregates non-graceful probes and emits a log once per interval.
        resetGracefulCounters();
        new MockUp<GracefulExitFlag>() {
            @Mock
            public static boolean isGracefulExit() {
                return false;
            }
        };

        GlobalStateMgr globalStateMgr = mock(GlobalStateMgr.class);
        NodeMgr nodeMgr = mock(NodeMgr.class);
        SystemInfoService systemInfo = mock(SystemInfoService.class);
        when(systemInfo.getTotalBackendNumber()).thenReturn(2);
        when(systemInfo.getAliveBackendNumber()).thenReturn(1);
        when(nodeMgr.getClusterInfo()).thenReturn(systemInfo);
        when(globalStateMgr.getNodeMgr()).thenReturn(nodeMgr);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }
        };

        ChannelFuture channelFuture = mock(ChannelFuture.class);
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        when(context.write(any())).thenReturn(channelFuture);
        BaseRequest request = mock(BaseRequest.class);
        when(request.getHostString()).thenReturn("127.0.0.1");
        when(request.getRequest())
                .thenReturn(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/api/health"));
        when(request.getContext()).thenReturn(context);

        HealthAction action = new HealthAction(new ActionController());
        // Two probes: the first seeds the summary timestamp, the second is aggregated.
        Deencapsulation.invoke(action, "executeWithoutPassword", request, new BaseResponse());
        Deencapsulation.invoke(action, "executeWithoutPassword", request, new BaseResponse());
    }

    private void resetGracefulCounters() throws Exception {
        setAtomic(HealthAction.class, "PROBE_COUNT", 0);
        setAtomic(HealthAction.class, "LAST_SUMMARY_TS", 0);
        setAtomic(HealthAction.class, "GRACEFUL_PROBE_COUNT", 0);
        setAtomic(HealthAction.class, "LAST_GRACEFUL_LOG_TS", 0);
    }

    private void setAtomic(Class<?> clazz, String fieldName, long value) throws Exception {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        ((AtomicLong) field.get(null)).set(value);
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
