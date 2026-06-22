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

import com.starrocks.common.DdlException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class WarehousePropertyTest {

    @Test
    public void testDefaultWarehouseProperty() {
        WarehouseProperty property = new WarehouseProperty();
        Assert.assertEquals(1L, property.getComputeReplica());
        Assert.assertEquals(WarehouseProperty.ReplicationType.NONE, property.getReplicationType());
        Assert.assertEquals(WarehouseProperty.WarmupLevelType.NONE, property.getWarmupLevel());
        Assert.assertEquals(0, property.getWarmupTimeoutSecs());
    }

    @Test
    public void testWarehousePropertySerializeToJson() {
        WarehouseProperty property = new WarehouseProperty(2, WarehouseProperty.ReplicationType.SYNC,
                WarehouseProperty.WarmupLevelType.INDEX, false);
        String jsonString = property.toString();
        JSONObject js = new JSONObject(jsonString);
        Assert.assertEquals(2, js.getInt(WarehouseProperty.PROPERTY_COMPUTE_REPLICA));
        Assert.assertEquals("SYNC", js.getString(WarehouseProperty.PROPERTY_REPLICATION_TYPE));
        Assert.assertEquals("INDEX", js.getString(WarehouseProperty.PROPERTY_WARMUP_LEVEL));
    }

    @Test
    public void testWarehousePropertyReplicationTypeConverting() throws DdlException {
        Assert.assertEquals(WarehouseProperty.ReplicationType.NONE,
                WarehouseProperty.replicationTypeFromString("none"));
        Assert.assertEquals(WarehouseProperty.ReplicationType.NONE,
                WarehouseProperty.replicationTypeFromString("None"));
        Assert.assertEquals(WarehouseProperty.ReplicationType.NONE,
                WarehouseProperty.replicationTypeFromString("NONE"));

        Assert.assertEquals(WarehouseProperty.ReplicationType.SYNC,
                WarehouseProperty.replicationTypeFromString("sync"));
        Assert.assertEquals(WarehouseProperty.ReplicationType.SYNC,
                WarehouseProperty.replicationTypeFromString("Sync"));
        Assert.assertEquals(WarehouseProperty.ReplicationType.SYNC,
                WarehouseProperty.replicationTypeFromString("SYNC"));

        Assert.assertEquals(WarehouseProperty.ReplicationType.ASYNC,
                WarehouseProperty.replicationTypeFromString("async"));
        Assert.assertEquals(WarehouseProperty.ReplicationType.ASYNC,
                WarehouseProperty.replicationTypeFromString("aSync"));
        Assert.assertEquals(WarehouseProperty.ReplicationType.ASYNC,
                WarehouseProperty.replicationTypeFromString("ASYNC"));

        Assert.assertThrows(DdlException.class, () -> WarehouseProperty.replicationTypeFromString("kudu"));
    }

    @Test
    public void testWarehousePropertyWarmupLevelConverting() throws DdlException {
        Assert.assertEquals(WarehouseProperty.WarmupLevelType.NONE,
                WarehouseProperty.warmupLevelTypeFromString("none"));
        Assert.assertEquals(WarehouseProperty.WarmupLevelType.NONE,
                WarehouseProperty.warmupLevelTypeFromString("None"));
        Assert.assertEquals(WarehouseProperty.WarmupLevelType.NONE,
                WarehouseProperty.warmupLevelTypeFromString("NONE"));

        Assert.assertEquals(WarehouseProperty.WarmupLevelType.META,
                WarehouseProperty.warmupLevelTypeFromString("meta"));
        Assert.assertEquals(WarehouseProperty.WarmupLevelType.META,
                WarehouseProperty.warmupLevelTypeFromString("mETa"));
        Assert.assertEquals(WarehouseProperty.WarmupLevelType.META,
                WarehouseProperty.warmupLevelTypeFromString("META"));

        Assert.assertEquals(WarehouseProperty.WarmupLevelType.INDEX,
                WarehouseProperty.warmupLevelTypeFromString("index"));
        Assert.assertEquals(WarehouseProperty.WarmupLevelType.INDEX,
                WarehouseProperty.warmupLevelTypeFromString("inDEX"));
        Assert.assertEquals(WarehouseProperty.WarmupLevelType.INDEX,
                WarehouseProperty.warmupLevelTypeFromString("INDEX"));

        Assert.assertEquals(WarehouseProperty.WarmupLevelType.ALL,
                WarehouseProperty.warmupLevelTypeFromString("all"));
        Assert.assertEquals(WarehouseProperty.WarmupLevelType.ALL,
                WarehouseProperty.warmupLevelTypeFromString("aLl"));
        Assert.assertEquals(WarehouseProperty.WarmupLevelType.ALL,
                WarehouseProperty.warmupLevelTypeFromString("all"));

        Assert.assertThrows(DdlException.class, () -> WarehouseProperty.warmupLevelTypeFromString("you-know-who"));
    }

    @Test
    public void testWarehousePropertyEquals() {
        WarehouseProperty property1 = new WarehouseProperty();
        WarehouseProperty property2 = new WarehouseProperty();
        Assert.assertEquals(property1, property2);

        // compute replica property
        property1.setComputeReplica(2);
        Assert.assertNotEquals(property1, property2);
        property2.setComputeReplica(2);
        Assert.assertEquals(property1, property2);

        // replication_type property
        property1.setReplicationType(WarehouseProperty.ReplicationType.SYNC);
        Assert.assertNotEquals(property1, property2);
        property2.setReplicationType(WarehouseProperty.ReplicationType.SYNC);
        Assert.assertEquals(property1, property2);

        // warmup_level property
        property1.setWarmupLevel(WarehouseProperty.WarmupLevelType.INDEX);
        Assert.assertNotEquals(property1, property2);
        property2.setWarmupLevel(WarehouseProperty.WarmupLevelType.INDEX);
        Assert.assertEquals(property1, property2);

        // warmup_timeout_secs property
        property1.setWarmupTimeoutSecs(300);
        Assert.assertNotEquals(property1, property2);
        property2.setWarmupTimeoutSecs(300);
        Assert.assertEquals(property1, property2);
    }

    @Test
    public void testWarehouseWarmupTimeoutSecsProperty() {
        WarehouseProperty property = new WarehouseProperty();
        // 0 means no override (fall back to the global config)
        Assert.assertEquals(0, property.getWarmupTimeoutSecs());

        property.setWarmupTimeoutSecs(600);
        Assert.assertEquals(600, property.getWarmupTimeoutSecs());

        // serialized to json under the documented property key
        JSONObject js = new JSONObject(property.toString());
        Assert.assertEquals(600, js.getInt(WarehouseProperty.PROPERTY_WARMUP_TIMEOUT_SECS));

        // deep copy keeps the override
        WarehouseProperty copy = new WarehouseProperty(property);
        Assert.assertEquals(600, copy.getWarmupTimeoutSecs());
        Assert.assertEquals(property, copy);
    }

    @Test
    public void testWarehouseQueryQueueProperties() {
        WarehouseProperty property = new WarehouseProperty(2, WarehouseProperty.ReplicationType.SYNC,
                WarehouseProperty.WarmupLevelType.INDEX, false);
        Assert.assertFalse(property.isEnableQueryQueue());
        // update query queue properties
        property.setEnableQueryQueue(true);
        property.setEnableQueryQueueLoad(true);
        property.setEnableQueryQueueStatistic(true);
        property.setQueryQueueMaxQueuedQueries(10);
        property.setQueryQueuePendingTimeoutSecond(100);
        Assert.assertTrue(property.isEnableQueryQueue());
        Assert.assertEquals(10, property.getQueryQueueMaxQueuedQueries());
        Assert.assertEquals(100, property.getQueryQueuePendingTimeoutSecond());

        String jsonString = property.toString();
        JSONObject js = new JSONObject(jsonString);
        Assert.assertEquals(2, js.getInt(WarehouseProperty.PROPERTY_COMPUTE_REPLICA));
        Assert.assertEquals("SYNC", js.getString(WarehouseProperty.PROPERTY_REPLICATION_TYPE));
        Assert.assertEquals("INDEX", js.getString(WarehouseProperty.PROPERTY_WARMUP_LEVEL));
        Assert.assertTrue(js.getBoolean(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE));
        Assert.assertTrue(js.getBoolean(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE_LOAD));
        Assert.assertTrue(js.getBoolean(WarehouseProperty.PROPERTY_ENABLE_QUERY_QUEUE_STATISTIC));
        Assert.assertEquals(10, js.getInt(WarehouseProperty.PROPERTY_QUERY_QUEUE_MAX_QUEUED_QUERIES));
        Assert.assertEquals(100, js.getInt(WarehouseProperty.PROPERTY_QUERY_QUEUE_PENDING_TIMEOUT_SECOND));

        WarehouseProperty property2 = new WarehouseProperty(property);
        Assert.assertEquals(property, property2);
        Assert.assertTrue(property2.isEnableQueryQueue());
        Assert.assertEquals(10, property2.getQueryQueueMaxQueuedQueries());
        Assert.assertEquals(100, property2.getQueryQueuePendingTimeoutSecond());
    }
}
