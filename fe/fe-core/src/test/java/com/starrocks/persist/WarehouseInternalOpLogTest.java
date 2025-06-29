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

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class WarehouseInternalOpLogTest {

    @Test
    public void testWarehouseInternalOpLog() throws IOException {
        String warehouseName = "new_warehouse";
        String payload = "{'a':'b'}";

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        WarehouseInternalOpLog log = new WarehouseInternalOpLog(warehouseName, payload);

        DataOutputStream dataOutStream = new DataOutputStream(os);
        log.write(dataOutStream);

        DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(os.toByteArray()));
        WarehouseInternalOpLog readLog = WarehouseInternalOpLog.read(dataInputStream);

        Assert.assertEquals(warehouseName, readLog.getWarehouseName());
        Assert.assertEquals(payload, readLog.getPayload());
    }
}
