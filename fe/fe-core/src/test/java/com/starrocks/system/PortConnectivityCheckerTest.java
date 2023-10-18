// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/system/HeartbeatMgrTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.system;

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.server.NodeMgr;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

public class PortConnectivityCheckerTest {
    @Before
    public void setup() throws Exception {
        Config.port_connectivity_check_timeout_ms = 500;
    }

    private static class PortListenerThread extends Thread {
        private volatile boolean running = true;
        private final int port;
        ServerSocket serverSocket = null;

        public PortListenerThread(int port) {
            this.port = port;
        }

        @Override
        public void run() {
            try {
                // Create a ServerSocket bound to the specified port
                serverSocket = new ServerSocket(port);
                System.out.println("Listening on port " + port);

                while (running) {
                    // Accept incoming connections
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Accepted connection from " +
                            clientSocket.getInetAddress() + ":" + clientSocket.getPort());
                }
                System.out.println("Stopped listening on port " + port);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (serverSocket != null) {
                        serverSocket.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        public void stopListening() throws IOException {
            running = false;
            serverSocket.close();
            System.out.println("Stop listening on port " + port);
        }
    }

    @Test
    public void testPortConnectivity() throws InterruptedException, IOException {
        int editLogPort1 = UtFrameUtils.findValidPort();
        Config.rpc_port = UtFrameUtils.findValidPort();
        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getMySelf() {
                return new Frontend(FrontendNodeType.FOLLOWER, "F1", "192.168.10.5", 9010);
            }

            @Mock
            public List<Frontend> getFrontends(FrontendNodeType nodeType) {
                List<Frontend> result = Lists.newArrayList();
                result.add(new Frontend(FrontendNodeType.FOLLOWER, "F1", "127.0.0.1", editLogPort1));
                return result;
            }
        };

        PortListenerThread listenerThread = new PortListenerThread(editLogPort1);
        listenerThread.start();

        Config.edit_log_port = editLogPort1;
        PortConnectivityChecker portConnectivityChecker = new PortConnectivityChecker();
        portConnectivityChecker.runAfterCatalogReady();

        Assert.assertTrue(portConnectivityChecker.getCurrentPortStates().get(new Pair<>("127.0.0.1", editLogPort1)));
        Assert.assertFalse(portConnectivityChecker.getCurrentPortStates().get(new Pair<>("127.0.0.1", Config.rpc_port)));

        listenerThread.stopListening();
        listenerThread.join();
    }
}
