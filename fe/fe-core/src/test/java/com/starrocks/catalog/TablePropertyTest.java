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

package com.starrocks.catalog;

import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.OperationType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.threeten.extra.PeriodDuration;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;

public class TablePropertyTest {
    private static String fileName = "./TablePropertyTest";

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
        properties.put(DynamicPartitionProperty.ENABLE, "true");
        properties.put(DynamicPartitionProperty.TIME_UNIT, "day");
        properties.put(DynamicPartitionProperty.START, "-3");
        properties.put(DynamicPartitionProperty.END, "3");
        properties.put(DynamicPartitionProperty.PREFIX, "p");
        properties.put(DynamicPartitionProperty.BUCKETS, "30");
        properties.put("otherProperty", "unknownProperty");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        TableProperty readTableProperty = TableProperty.read(in);
        DynamicPartitionProperty readDynamicPartitionProperty = readTableProperty.getDynamicPartitionProperty();
        DynamicPartitionProperty dynamicPartitionProperty = new DynamicPartitionProperty(properties);
        Assert.assertEquals(readTableProperty.getProperties(), properties);
        Assert.assertEquals(readDynamicPartitionProperty.isEnabled(), dynamicPartitionProperty.isEnabled());
        Assert.assertEquals(readDynamicPartitionProperty.getBuckets(), dynamicPartitionProperty.getBuckets());
        Assert.assertEquals(readDynamicPartitionProperty.getPrefix(), dynamicPartitionProperty.getPrefix());
        Assert.assertEquals(readDynamicPartitionProperty.getStart(), dynamicPartitionProperty.getStart());
        Assert.assertEquals(readDynamicPartitionProperty.getEnd(), dynamicPartitionProperty.getEnd());
        Assert.assertEquals(readDynamicPartitionProperty.getTimeUnit(), dynamicPartitionProperty.getTimeUnit());
        in.close();
    }

    @Test
    public void testBuildDataCachePartitionDuration() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION, "3 month");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        TableProperty readTableProperty = TableProperty.read(in);
        Assert.assertNotNull(readTableProperty.buildProperty(OperationType.OP_ALTER_TABLE_PROPERTIES));
        in.close();
    }

    @Test
    public void testPartitionTTLNumberSerialization() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER, "2");
        properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_TTL, "1 day");
        PeriodDuration duration = TimeUtils.parseHumanReadablePeriodOrDuration("1 day");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildPartitionLiveNumber();
        tableProperty.buildPartitionTTL();
        Assert.assertEquals(2, tableProperty.getPartitionTTLNumber());
        Assert.assertEquals(duration, tableProperty.getPartitionTTL());
        tableProperty.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        TableProperty newTableProperty = TableProperty.read(in);
        Assert.assertEquals(2, newTableProperty.getPartitionTTLNumber());
        Assert.assertEquals(duration, tableProperty.getPartitionTTL());
        in.close();

        // 3. Update again
        properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER, "3");
        properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_TTL, "2 day");
        duration = TimeUtils.parseHumanReadablePeriodOrDuration("2 day");
        newTableProperty.modifyTableProperties(properties);
        newTableProperty.buildPartitionLiveNumber();
        newTableProperty.buildPartitionTTL();
        Assert.assertEquals(3, newTableProperty.getPartitionTTLNumber());
        Assert.assertEquals(duration, newTableProperty.getPartitionTTL());
    }

    @Test
    public void testBuildExternalCooldownConfig() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_WAIT_SECOND, "3600");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        TableProperty readTableProperty = TableProperty.read(in);
        TableProperty propertyRead = readTableProperty.buildProperty(OperationType.OP_MODIFY_EXTERNAL_COOLDOWN_CONFIG);
        Assert.assertNotNull(propertyRead);
        Assert.assertEquals(3600L, propertyRead.getExternalCoolDownWaitSecond());
        Assert.assertNull(propertyRead.getExternalCoolDownSchedule());
        Assert.assertNull(propertyRead.getExternalCoolDownTarget());
        in.close();
    }

    @Test
    public void testBuildExternalCooldownSchedule() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_SCHEDULE,
                "START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE");
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_TARGET, "iceberg0.db.tbl");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        TableProperty readTableProperty = TableProperty.read(in);
        TableProperty propertyRead = readTableProperty.buildProperty(OperationType.OP_MODIFY_EXTERNAL_COOLDOWN_CONFIG);
        Assert.assertNotNull(propertyRead);
        Assert.assertEquals(0L, propertyRead.getExternalCoolDownWaitSecond());
        Assert.assertEquals("START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE",
                propertyRead.getExternalCoolDownSchedule());
        Assert.assertEquals("iceberg0.db.tbl", propertyRead.getExternalCoolDownTarget());
        in.close();

        propertyRead.setExternalCoolDownConfig(null);
        Assert.assertNull(propertyRead.getExternalCoolDownConfig());
        Assert.assertEquals(0L, propertyRead.getExternalCoolDownWaitSecond());
        Assert.assertNull(propertyRead.getExternalCoolDownSchedule());
        Assert.assertNull(propertyRead.getExternalCoolDownTarget());
    }
}
