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

import com.starrocks.common.io.Writable;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.journal.JournalEntity;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;

import static com.starrocks.persist.OperationType.OP_MODIFY_EXTERNAL_COOLDOWN_CONFIG;

public class ModifyExternalCooldownConfigTest {
    private String fileName = "./ModifyExternalCooldownConfigTest";

    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testNormal() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_TARGET, "iceberg.db1.tbl1");
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_WAIT_SECOND, "3600");
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_SCHEDULE,
                "START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE");
        ModifyTablePropertyOperationLog modifyTablePropertyOperationLog =
                new ModifyTablePropertyOperationLog(100L, 200L, properties);
        out.writeShort(OP_MODIFY_EXTERNAL_COOLDOWN_CONFIG);
        modifyTablePropertyOperationLog.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        JournalEntity journalEntity = new JournalEntity();
        journalEntity.readFields(in);
        Writable data = journalEntity.getData();
        Assert.assertEquals(OP_MODIFY_EXTERNAL_COOLDOWN_CONFIG, journalEntity.getOpCode());
        Assert.assertTrue(data instanceof ModifyTablePropertyOperationLog);

        ModifyTablePropertyOperationLog readModifyExternalCooldownConfigInfo = (ModifyTablePropertyOperationLog) data;
        Assert.assertEquals(readModifyExternalCooldownConfigInfo.getDbId(), 100L);
        Assert.assertEquals(readModifyExternalCooldownConfigInfo.getTableId(), 200L);
        Assert.assertEquals(readModifyExternalCooldownConfigInfo.getProperties(), properties);
        in.close();
    }
}
