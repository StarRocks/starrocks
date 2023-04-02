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

import com.starrocks.persist.gson.GsonUtils;
import org.junit.Assert;
import org.junit.Test;

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
        BackendHbResponse resp = new BackendHbResponse(beId, bePort, httpPort, brpcPort, starletPort, hbTime, version, cpuCores);

        Assert.assertEquals(beId, resp.getBeId());
        Assert.assertEquals(bePort, resp.getBePort());
        Assert.assertEquals(httpPort, resp.getHttpPort());
        Assert.assertEquals(brpcPort, resp.getBrpcPort());
        Assert.assertEquals(starletPort, resp.getStarletPort());
        Assert.assertEquals(version, resp.getVersion());
        Assert.assertEquals(cpuCores, resp.getCpuCores());

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
    }
}
