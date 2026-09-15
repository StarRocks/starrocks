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

import com.starrocks.persist.HbPackage;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.HeartbeatServiceConstants;
import com.starrocks.thrift.TBackendInfo;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NativeGeoHeartbeatTest {
    @Test
    public void testOptionalWireField() throws Exception {
        TBackendInfo source = new TBackendInfo(9060, 8040);
        TBackendInfo restored = new TBackendInfo();
        new TDeserializer().deserialize(restored, new TSerializer().serialize(source));
        Assertions.assertFalse(restored.isSetNative_geo_capabilities());
        Assertions.assertEquals(0, restored.getNative_geo_capabilities());
        source.setNative_geo_capabilities(HeartbeatServiceConstants.NATIVE_GEOGRAPHY_TRANSPORT);
        new TDeserializer().deserialize(restored, new TSerializer().serialize(source));
        Assertions.assertTrue(restored.isSetNative_geo_capabilities());
        Assertions.assertEquals(HeartbeatServiceConstants.NATIVE_GEOGRAPHY_TRANSPORT,
                restored.getNative_geo_capabilities());
    }

    @Test
    public void testReplayAndDowngrade() {
        ComputeNode leader = new ComputeNode();
        leader.setAlive(true);
        ComputeNode follower = new ComputeNode();
        follower.setAlive(true);
        BackendHbResponse response = new BackendHbResponse(0, 0, 0, 0, 0, 1, "", 0, 0);
        response.setNativeGeoCapabilities(HeartbeatServiceConstants.NATIVE_GEOGRAPHY_TRANSPORT);
        Assertions.assertTrue(leader.handleHbResponse(response, false));
        HbPackage pkg = new HbPackage();
        pkg.addHbResponse(response);
        HbPackage replay = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(pkg), HbPackage.class);
        follower.handleHbResponse((BackendHbResponse) replay.getHbResults().get(0), true);
        Assertions.assertEquals(leader.getNativeGeoCapabilities(), follower.getNativeGeoCapabilities());
        Assertions.assertFalse(leader.handleHbResponse(response, false));
        BackendHbResponse oldNode = new BackendHbResponse(0, 0, 0, 0, 0, 2, "", 0, 0);
        Assertions.assertTrue(leader.handleHbResponse(oldNode, false));
        follower.handleHbResponse(oldNode, true);
        Assertions.assertEquals(0, leader.getNativeGeoCapabilities());
        Assertions.assertEquals(0, follower.getNativeGeoCapabilities());
    }
}
