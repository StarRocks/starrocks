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

package com.starrocks.system;

import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.journal.JournalEntity;
import com.starrocks.persist.EditLogDeserializer;
import com.starrocks.persist.HbPackage;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TStatusCode;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class BackendHbResponseTest {
    @Test
    public void testSerializeHbResponse() {
        int beId = 1;
        int bePort = 59000;
        int httpPort = 59001;
        int brpcPort = 59002;
        int starletPort = 59003;
        long hbTime = System.currentTimeMillis();
        String version = "version1";
        int cpuCores = 10;
        long memLimitBytes = 20;
        BackendHbResponse resp =
                new BackendHbResponse(beId, bePort, httpPort, brpcPort, starletPort, hbTime, version, cpuCores, memLimitBytes);

        Assert.assertEquals(beId, resp.getBeId());
        Assert.assertEquals(bePort, resp.getBePort());
        Assert.assertEquals(httpPort, resp.getHttpPort());
        Assert.assertEquals(brpcPort, resp.getBrpcPort());
        Assert.assertEquals(starletPort, resp.getStarletPort());
        Assert.assertEquals(version, resp.getVersion());
        Assert.assertEquals(cpuCores, resp.getCpuCores());
        Assert.assertEquals(memLimitBytes, resp.getMemLimitBytes());
        Assert.assertEquals(TStatusCode.OK, resp.getStatusCode());

        // json serialize
        String json = GsonUtils.GSON.toJson(resp);
        BackendHbResponse respJson = GsonUtils.GSON.fromJson(json, BackendHbResponse.class);
        Assert.assertEquals(beId, respJson.getBeId());
        Assert.assertEquals(bePort, respJson.getBePort());
        Assert.assertEquals(httpPort, respJson.getHttpPort());
        Assert.assertEquals(brpcPort, respJson.getBrpcPort());
        Assert.assertEquals(starletPort, respJson.getStarletPort());
        Assert.assertEquals(version, respJson.getVersion());
        Assert.assertEquals(cpuCores, respJson.getCpuCores());
        Assert.assertEquals(memLimitBytes, respJson.getMemLimitBytes());
        Assert.assertEquals(TStatusCode.OK, respJson.getStatusCode());
    }

    @Test
    public void testSerializeHbResponseStatusCode() throws IOException {
        HbPackage hbPackage = new HbPackage();
        BackendHbResponse hbResponse = new BackendHbResponse(1, TStatusCode.SHUTDOWN, "Shutdown");
        Assert.assertEquals(TStatusCode.SHUTDOWN, hbResponse.getStatusCode());
        hbPackage.addHbResponse(hbResponse);

        DataOutputBuffer buffer = new DataOutputBuffer(1024);
        JournalEntity entity = new JournalEntity(OperationType.OP_HEARTBEAT_V2, hbPackage);
        buffer.writeShort(entity.opCode());
        entity.data().write(buffer);

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer.getData()));
        short opCode = in.readShort();
        JournalEntity replayEntry = new JournalEntity(opCode, EditLogDeserializer.deserialize(opCode, in));

        Assert.assertEquals(OperationType.OP_HEARTBEAT_V2, replayEntry.opCode());
        HbPackage replayHbPackage = (HbPackage) replayEntry.data();
        Assert.assertEquals(1, replayHbPackage.getHbResults().size());
        HeartbeatResponse replayHbResponse = replayHbPackage.getHbResults().get(0);
        Assert.assertEquals(HeartbeatResponse.Type.BACKEND, replayHbResponse.getType());
        Assert.assertTrue(replayHbResponse instanceof BackendHbResponse);

        // ensure the status code can be replayed through the edit log, so the follower can be synced with the leader
        BackendHbResponse replayBackendResponse = (BackendHbResponse) replayHbResponse;
        Assert.assertEquals(TStatusCode.SHUTDOWN, replayBackendResponse.getStatusCode());
    }
}
