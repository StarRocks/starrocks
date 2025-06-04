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

package com.starrocks.persist;

import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Writable;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class EditLogDeserializerTest {

    @Test
    public void testEditLogDeserializerWarehouseInternalOpLog() throws IOException {
        DataOutputBuffer buffer = new DataOutputBuffer();

        String warehouseName = "new_warehouse";
        String payload = "{'a':'b'}";
        WarehouseInternalOpLog log = new WarehouseInternalOpLog(warehouseName, payload);

        buffer.writeShort(OperationType.OP_WAREHOUSE_INTERNAL_OP);
        log.write(buffer);

        DataInputStream inStream = new DataInputStream(new ByteArrayInputStream(buffer.getData()));

        short opCode = inStream.readShort();
        Assert.assertEquals(OperationType.OP_WAREHOUSE_INTERNAL_OP, opCode);
        Writable w = EditLogDeserializer.deserialize(opCode, inStream);
        Assert.assertTrue(w instanceof WarehouseInternalOpLog);
        WarehouseInternalOpLog readLog = (WarehouseInternalOpLog) w;
        Assert.assertEquals(warehouseName, readLog.getWarehouseName());
        Assert.assertEquals(payload, readLog.getPayload());
    }
}
