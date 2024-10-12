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

import com.starrocks.externalcooldown.ExternalCooldownConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class ExternalCooldownConfigTest {
    private String fileName = "./ExternalCooldownConfigTest";
    private String fileName1 = "./ExternalCooldownConfigTest1";

    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();

        File file1 = new File(fileName1);
        file1.delete();
    }

    @Test
    public void testNormal() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        ExternalCooldownConfig config = new ExternalCooldownConfig(
                "iceberg.db1.tbl1", "START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE", 3600L);

        config.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        ExternalCooldownConfig config2 = ExternalCooldownConfig.read(in);
        Assert.assertEquals(config, config2);

        in.close();

        Assert.assertNotEquals(config, null);
        Assert.assertFalse(config.equals(0));
    }
}