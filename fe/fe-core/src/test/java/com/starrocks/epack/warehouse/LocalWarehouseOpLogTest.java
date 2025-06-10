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

package com.starrocks.epack.warehouse;

import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

public class LocalWarehouseOpLogTest {

    void validateSerializeAndDeserialize(LocalWarehouseOpLog opLog) {
        ExceptionChecker.expectThrowsNoException(() -> {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(out);
            opLog.write(dataOut);

            byte[] bytes = out.toByteArray();
            DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(bytes));
            String payload = Text.readString(dataIn);
            LocalWarehouseOpLog deserializeOpLog = LocalWarehouseOpLog.fromJson(payload);
            Assert.assertEquals(opLog.toJson(), deserializeOpLog.toJson());
        });
    }

    @Test
    public void testCreateCNGroupOpLog() {
        Cluster cluster = new Cluster(2L, "test-cngroup", 3L);
        LocalWarehouseOpLog log = LocalWarehouseOpLog.createCNGroupOpLog(cluster);

        Assert.assertEquals(LocalWarehouseOpLog.CREATE_CNGROUP, log.getOp());
        Assert.assertNull(log.getCNGroupName());
        Assert.assertEquals(cluster.toJson(), log.getCluster().toJson());
        validateSerializeAndDeserialize(log);
    }

    @Test
    public void testDropCNGroupOpLog() {
        String cngroupName = "test-cg";
        LocalWarehouseOpLog log = LocalWarehouseOpLog.dropCNGroupOpLog(cngroupName);

        Assert.assertEquals(LocalWarehouseOpLog.DROP_CNGROUP, log.getOp());
        Assert.assertEquals(cngroupName, log.getCNGroupName());
        Assert.assertNull(log.getCluster());
        validateSerializeAndDeserialize(log);
    }

    @Test
    public void testEnableCNGroupOpLog() {
        String cngroupName = "enabled-cg";
        LocalWarehouseOpLog log = LocalWarehouseOpLog.enableCNGroupOpLog(cngroupName);

        Assert.assertEquals(LocalWarehouseOpLog.ENABLE_CNGROUP, log.getOp());
        Assert.assertEquals(cngroupName, log.getCNGroupName());
        validateSerializeAndDeserialize(log);
    }

    @Test
    public void testDisableCNGroupOpLog() {
        String cngroupName = "disabled-cg";
        LocalWarehouseOpLog log = LocalWarehouseOpLog.disableCNGroupOpLog(cngroupName);

        Assert.assertEquals(LocalWarehouseOpLog.DISABLE_CNGROUP, log.getOp());
        Assert.assertEquals(cngroupName, log.getCNGroupName());
        validateSerializeAndDeserialize(log);
    }
}
