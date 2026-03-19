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

import com.google.gson.Gson;
import com.starrocks.catalog.DataProperty;
import com.starrocks.thrift.TStorageMedium;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ModifyPartitionInfoTest {

    @Test
    public void testConstructorWithDataCacheEnable() {
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD, DataProperty.MAX_COOLDOWN_TIME_MS);
        ModifyPartitionInfo info = new ModifyPartitionInfo(1L, 2L, 3L, dataProperty, (short) 1, true);

        Assertions.assertEquals(1L, info.getDbId());
        Assertions.assertEquals(2L, info.getTableId());
        Assertions.assertEquals(3L, info.getPartitionId());
        Assertions.assertEquals(dataProperty, info.getDataProperty());
        Assertions.assertEquals((short) 1, info.getReplicationNum());
        Assertions.assertTrue(info.getDataCacheEnable());
    }

    @Test
    public void testConstructorWithoutDataCacheEnableDefaultsNull() {
        DataProperty dataProperty = new DataProperty(TStorageMedium.SSD, DataProperty.MAX_COOLDOWN_TIME_MS);
        ModifyPartitionInfo info = new ModifyPartitionInfo(1L, 2L, 3L, dataProperty, (short) 3);

        Assertions.assertNull(info.getDataCacheEnable());
    }

    @Test
    public void testSetDataCacheEnable() {
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD, DataProperty.MAX_COOLDOWN_TIME_MS);
        ModifyPartitionInfo info = new ModifyPartitionInfo(1L, 2L, 3L, dataProperty, (short) 1, false);

        Assertions.assertFalse(info.getDataCacheEnable());
        info.setDataCacheEnable(true);
        Assertions.assertTrue(info.getDataCacheEnable());
        info.setDataCacheEnable(false);
        Assertions.assertFalse(info.getDataCacheEnable());
    }

    @Test
    public void testEqualsWithDataCacheEnable() {
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD, DataProperty.MAX_COOLDOWN_TIME_MS);

        ModifyPartitionInfo info1 = new ModifyPartitionInfo(1L, 2L, 3L, dataProperty, (short) 1, true);
        ModifyPartitionInfo info2 = new ModifyPartitionInfo(1L, 2L, 3L, dataProperty, (short) 1, true);
        ModifyPartitionInfo info3 = new ModifyPartitionInfo(1L, 2L, 3L, dataProperty, (short) 1, false);

        Assertions.assertEquals(info1, info2);
        Assertions.assertNotEquals(info1, info3);
    }

    @Test
    public void testEqualsSameObject() {
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD, DataProperty.MAX_COOLDOWN_TIME_MS);
        ModifyPartitionInfo info = new ModifyPartitionInfo(1L, 2L, 3L, dataProperty, (short) 1, true);
        Assertions.assertEquals(info, info);
    }

    @Test
    public void testEqualsNull() {
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD, DataProperty.MAX_COOLDOWN_TIME_MS);
        ModifyPartitionInfo info = new ModifyPartitionInfo(1L, 2L, 3L, dataProperty, (short) 1, true);
        Assertions.assertNotEquals(info, null);
    }

    @Test
    public void testEqualsWrongType() {
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD, DataProperty.MAX_COOLDOWN_TIME_MS);
        ModifyPartitionInfo info = new ModifyPartitionInfo(1L, 2L, 3L, dataProperty, (short) 1, true);
        Assertions.assertNotEquals(info, "not a ModifyPartitionInfo");
    }

    @Test
    public void testGsonSerialization() {
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD, DataProperty.MAX_COOLDOWN_TIME_MS);
        ModifyPartitionInfo info = new ModifyPartitionInfo(1L, 2L, 3L, dataProperty, (short) 1, true);

        Gson gson = new Gson();
        String json = gson.toJson(info);

        // Verify the json contains dataCacheEnable
        Assertions.assertTrue(json.contains("\"dataCacheEnable\":true"));

        ModifyPartitionInfo deserialized = gson.fromJson(json, ModifyPartitionInfo.class);
        Assertions.assertEquals(info.getDbId(), deserialized.getDbId());
        Assertions.assertEquals(info.getTableId(), deserialized.getTableId());
        Assertions.assertEquals(info.getPartitionId(), deserialized.getPartitionId());
        Assertions.assertEquals(info.getReplicationNum(), deserialized.getReplicationNum());
        Assertions.assertEquals(info.getDataCacheEnable(), deserialized.getDataCacheEnable());
    }

    @Test
    public void testGsonSerializationWithDataCacheDisabled() {
        DataProperty dataProperty = new DataProperty(TStorageMedium.SSD, DataProperty.MAX_COOLDOWN_TIME_MS);
        ModifyPartitionInfo info = new ModifyPartitionInfo(1L, 2L, 3L, dataProperty, (short) 3, false);

        Gson gson = new Gson();
        String json = gson.toJson(info);
        ModifyPartitionInfo deserialized = gson.fromJson(json, ModifyPartitionInfo.class);

        Assertions.assertFalse(deserialized.getDataCacheEnable());
    }

    @Test
    public void testDefaultConstructor() {
        ModifyPartitionInfo info = new ModifyPartitionInfo();
        // Default values
        Assertions.assertEquals(0L, info.getDbId());
        Assertions.assertEquals(0L, info.getTableId());
        Assertions.assertEquals(0L, info.getPartitionId());
        Assertions.assertNull(info.getDataCacheEnable());
    }

    @Test
    public void testGsonDeserializationWithoutDataCacheEnableField() {
        // Simulate an old edit log entry created before dataCacheEnable was added.
        // Gson should deserialize the missing field as null (not false).
        String oldJson = "{\"dbId\":1,\"tableId\":2,\"partitionId\":3,\"replicationNum\":1}";
        Gson gson = new Gson();
        ModifyPartitionInfo deserialized = gson.fromJson(oldJson, ModifyPartitionInfo.class);

        Assertions.assertNull(deserialized.getDataCacheEnable(),
                "Old entries without dataCacheEnable should deserialize as null, not false");
    }
}
