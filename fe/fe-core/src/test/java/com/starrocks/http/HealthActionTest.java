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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HealthActionTest extends StarRocksHttpTestCase {

    private static final long TEST_CN_ID = 9001;

    @Override
    protected void doSetUp() {
        HealthAction.resetNodesReadyForTest();
    }

    @AfterEach
    public void resetNodesReadyLatch() {
        HealthAction.resetNodesReadyForTest();
    }

    @Test
    public void testAsyncHandleHealth() throws Exception {
        String uri = "/api/health";
        ActionController controller = new ActionController();
        MockHealthAction action = new MockHealthAction(controller);
        controller.registerHandler(HttpMethod.GET, uri, action);
        HttpServerTestUtils.MockExecutor executor = new HttpServerTestUtils.MockExecutor();

        executor.setRejectExecute(false);
        HttpServerTestUtils.MockChannelHandlerContext context = new HttpServerTestUtils().createChannelHandlerContext();
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
    public void testNodesReadyLatch() {
        // nothing registered: new cluster, must not block
        assertTrue(HealthAction.isNodesReady(0, 0));

        HealthAction.resetNodesReadyForTest();
        // registered but none alive: not ready until a node is alive
        assertFalse(HealthAction.isNodesReady(3, 0));
        assertFalse(HealthAction.isNodesReady(3, 0));
        assertTrue(HealthAction.isNodesReady(3, 1));
        // latched: a later outage does not flip it back
        assertTrue(HealthAction.isNodesReady(3, 0));
    }

    @Test
    public void testPlainHealthIgnoresNodeState() throws IOException {
        setAllBackendsAlive(false);
        assertEquals(200, get("/api/health"));
        assertEquals(200, get("/api/health?check_nodes=false"));
    }

    @Test
    public void testCheckNodesWaitsForAliveBackend() throws IOException {
        setAllBackendsAlive(false);
        assertEquals(503, get("/api/health?check_nodes=true"));

        clusterInfo().getBackends().get(0).setAlive(true);
        assertEquals(200, get("/api/health?check_nodes=true"));

        // once ready, stays ready even if every node goes down
        setAllBackendsAlive(false);
        assertEquals(200, get("/api/health?check_nodes=true"));
    }

    @Test
    public void testCheckNodesCountsComputeNodes() throws IOException {
        setAllBackendsAlive(false);
        ComputeNode cn = new ComputeNode(TEST_CN_ID, "cn-1", 9050);
        clusterInfo().addComputeNode(cn);
        try {
            assertEquals(503, get("/api/health?check_nodes=true"));
            cn.setAlive(true);
            assertEquals(200, get("/api/health?check_nodes=true"));
        } finally {
            clusterInfo().dropComputeNode(cn);
        }
    }

    private int get(String path) throws IOException {
        Request request = new Request.Builder().url(BASE_URL + path).get().build();
        try (Response response = networkClient.newCall(request).execute()) {
            return response.code();
        }
    }

    private static SystemInfoService clusterInfo() {
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
    }

    private static void setAllBackendsAlive(boolean alive) {
        for (Backend backend : clusterInfo().getBackends()) {
            backend.setAlive(alive);
        }
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
