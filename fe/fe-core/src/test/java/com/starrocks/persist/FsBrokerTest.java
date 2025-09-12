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

package com.starrocks.persist;

import com.starrocks.catalog.FsBroker;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.system.BrokerHbResponse;
import com.starrocks.system.HeartbeatResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class FsBrokerTest {

    private static String fileName1 = "./FsBrokerTest1";
    private static String fileName2 = "./FsBrokerTest2";

    @AfterAll
    public static void tear() {
        new File(fileName1).delete();
        new File(fileName2).delete();
    }

    @Test
    public void testHeartbeatOk() throws Exception {
        // 1. Write objects to file
        File file = new File(fileName1);
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        FsBroker fsBroker = new FsBroker("127.0.0.1", 8118);
        long time = System.currentTimeMillis();
        BrokerHbResponse hbResponse = new BrokerHbResponse("broker", "127.0.0.1", 8118, time);
        fsBroker.handleHbResponse(hbResponse, false);
        Text.writeString(dos, GsonUtils.GSON.toJson(fsBroker, FsBroker.class));
        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        FsBroker readBroker = FsBroker.readIn(dis);
        Assertions.assertEquals(fsBroker.ip, readBroker.ip);
        Assertions.assertEquals(fsBroker.port, readBroker.port);
        Assertions.assertEquals(fsBroker.isAlive, readBroker.isAlive);
        Assertions.assertTrue(fsBroker.isAlive);
        Assertions.assertEquals(time, readBroker.lastStartTime);
        Assertions.assertEquals(-1, readBroker.lastUpdateTime);
        dis.close();
    }

    @Test
    public void testHeartbeatFailed() throws Exception {
        // 1. Write objects to file
        File file = new File(fileName2);
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        FsBroker fsBroker = new FsBroker("127.0.0.1", 8118);
        long time = System.currentTimeMillis();
        BrokerHbResponse hbResponse = new BrokerHbResponse("broker", "127.0.0.1", 8118, "got exception");
        fsBroker.handleHbResponse(hbResponse, false);
        Text.writeString(dos, GsonUtils.GSON.toJson(fsBroker, FsBroker.class));
        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        FsBroker readBroker = FsBroker.readIn(dis);
        Assertions.assertEquals(fsBroker.ip, readBroker.ip);
        Assertions.assertEquals(fsBroker.port, readBroker.port);
        Assertions.assertEquals(fsBroker.isAlive, readBroker.isAlive);
        Assertions.assertFalse(fsBroker.isAlive);
        Assertions.assertEquals(-1, readBroker.lastStartTime);
        Assertions.assertEquals(-1, readBroker.lastUpdateTime);
        dis.close();
    }

    @Test
    public void testBrokerAlive() throws Exception {

        FsBroker fsBroker = new FsBroker("127.0.0.1", 8118);
        long time = System.currentTimeMillis();
        BrokerHbResponse hbResponse = new BrokerHbResponse("broker", "127.0.0.1", 8118, "got exception");

        hbResponse.aliveStatus = HeartbeatResponse.AliveStatus.ALIVE;
        fsBroker.handleHbResponse(hbResponse, true);
        Assertions.assertTrue(fsBroker.isAlive);
        hbResponse.aliveStatus = HeartbeatResponse.AliveStatus.NOT_ALIVE;
        fsBroker.handleHbResponse(hbResponse, true);
        Assertions.assertFalse(fsBroker.isAlive);
    }
}
