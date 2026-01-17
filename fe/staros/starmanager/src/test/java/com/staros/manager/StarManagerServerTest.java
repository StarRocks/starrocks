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

package com.staros.manager;

import com.staros.util.Utils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StarManagerServerTest {

    public static int detectUsableSocketPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (Exception e) {
            throw new IllegalStateException("Could not find a free TCP/IP port");
        }
    }

    public static ExecutorService getExecutorServiceFromServer(StarManagerServer server) {
        try {
            Object executorService = FieldUtils.readField(server, "executorService", true);
            return (ExecutorService) executorService;
        } catch (IllegalAccessException e) {
            return null;
        }
    }

    @Test
    public void testUseDefaultExecutorService() throws IOException {
        int port = detectUsableSocketPort();
        StarManagerServer server = new StarManagerServer();
        server.start(port);
        ExecutorService executorService = getExecutorServiceFromServer(server);
        Assert.assertNotNull(executorService);
        Assert.assertFalse(executorService.isShutdown());
        Assert.assertFalse(executorService.isTerminated());
        server.shutdown();
        Assert.assertTrue(executorService.isShutdown());
        Assert.assertTrue(executorService.isTerminated());
    }

    @Test
    public void testUseCustomizedExecutorService() throws IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        int port = detectUsableSocketPort();
        StarManagerServer server = new StarManagerServer();
        server.start("127.0.0.1", port, executorService);
        ExecutorService es = getExecutorServiceFromServer(server);

        // null executor service
        Assert.assertNull(es);

        Assert.assertFalse(executorService.isShutdown());
        Assert.assertFalse(executorService.isTerminated());
        server.shutdown();

        // starmgr server will not shut down the executor
        Assert.assertFalse(executorService.isShutdown());
        Assert.assertFalse(executorService.isTerminated());

        Utils.shutdownExecutorService(executorService);
    }
}
