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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/mysql/MysqlServerTest.java

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

package com.starrocks.mysql;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectScheduler;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

public class MysqlServerTest {
    private final AtomicInteger submitNum = new AtomicInteger(0);
    private final AtomicInteger submitFailNum = new AtomicInteger(0);
    @Mocked
    private final ConnectScheduler scheduler = new ConnectScheduler(10) {
        @Override
        public boolean submit(ConnectContext context) {
            submitNum.getAndIncrement();
            try {
                ByteBuffer writer = ByteBuffer.allocate(10);
                writer.put((byte) 1);
                context.getMysqlChannel().sendAndFlush(writer);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return true;
        }
    };

    @Mocked
    private final ConnectScheduler badScheduler = new ConnectScheduler(10) {
        @Override
        public boolean submit(ConnectContext context) {
            submitFailNum.getAndIncrement();
            try {
                ByteBuffer writer = ByteBuffer.allocate(10);
                writer.put((byte) 1);
                context.getMysqlChannel().sendAndFlush(writer);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return false;
        }
    };

    @Before
    public void setUp() {
        submitNum.set(0);
        submitFailNum.set(0);
    }

    @Test
    public void testInvalidParam() throws IOException {
        ServerSocket socket = new ServerSocket(0);
        int port = socket.getLocalPort();
        socket.close();
        MysqlServer server = new MysqlServer(port, null, null);
        Assert.assertFalse(server.start());
    }

    @Test
    public void testBindFail() throws IOException {
        ServerSocket socket = new ServerSocket(0);
        int port = socket.getLocalPort();
        socket.close();
        MysqlServer server = new MysqlServer(port, scheduler, null);
        Assert.assertTrue(server.start());
        MysqlServer server1 = new MysqlServer(port, scheduler, null);
        Assert.assertFalse(server1.start());

        server.stop();
        server.join();
    }

    @Test
    public void testSubFail() throws IOException, InterruptedException {
        ServerSocket socket = new ServerSocket(0);
        int port = socket.getLocalPort();
        socket.close();
        MysqlServer server = new MysqlServer(port, badScheduler, null);
        Assert.assertTrue(server.start());

        // submit
        SocketChannel channel = SocketChannel.open();
        channel.connect(new InetSocketAddress("127.0.0.1", port));
        // sleep to wait mock process
        ByteBuffer reader = ByteBuffer.allocate(10);
        while (channel.read(reader) < 0) {
            Thread.sleep(100);
        }
        channel.close();

        // submit twice
        channel = SocketChannel.open();
        channel.connect(new InetSocketAddress("127.0.0.1", port));
        // sleep to wait mock process
        reader = ByteBuffer.allocate(10);
        while (channel.read(reader) < 0) {
            Thread.sleep(100);
        }
        channel.close();

        // stop and join
        server.stop();
        server.join();

        Assert.assertEquals(2, submitFailNum.get());
    }

}