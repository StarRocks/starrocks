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

package com.starrocks.externalcooldown;

import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;


public class ExternalCooldownConfigTest {

    @Test
    public void testPartitionStartEnd() {
        ExternalCooldownConfig config = new ExternalCooldownConfig(
                "iceberg.db1.tbl1", "START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE", 3600L);

        ExternalCooldownSchedule schedule = ExternalCooldownSchedule.fromString(config.getSchedule());
        Assert.assertNotNull(schedule);
        Assert.assertEquals("01:00", schedule.getStart());
        Assert.assertEquals("07:59", schedule.getEnd());
        Assert.assertEquals(1L, schedule.getInterval());
        Assert.assertEquals("MINUTE", schedule.getUnit());
        Assert.assertEquals(60L, schedule.getIntervalSeconds());

        Assert.assertTrue(config.isReadyForAutoCooldown());

        Map<String, String> properties = config.getValidProperties();
        Assert.assertNotNull(properties);
        Assert.assertEquals(3, properties.size());
        Assert.assertTrue(properties.containsKey(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_TARGET));
        Assert.assertEquals("iceberg.db1.tbl1",
                properties.get(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_TARGET));
        Assert.assertTrue(properties.containsKey(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_SCHEDULE));
        Assert.assertEquals("START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE",
                properties.get(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_SCHEDULE));
        Assert.assertTrue(properties.containsKey(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_WAIT_SECOND));
        Assert.assertEquals("3600",
                properties.get(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_WAIT_SECOND));

        String str = "{ target : iceberg.db1.tbl1,\n schedule : START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE,\n "
                + "wait second : 3600 }";
        Assert.assertEquals(str, config.toString());
    }

    @Test
    public void testReadyForAutoCooldown() {
        ExternalCooldownConfig config = new ExternalCooldownConfig(
                "iceberg.db1.tbl1", "START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE", 3600L);
        Assert.assertTrue(config.isReadyForAutoCooldown());

        config.setWaitSecond(null);
        Assert.assertFalse(config.isReadyForAutoCooldown());
        Assert.assertEquals(2, config.getValidProperties().size());
        config.setWaitSecond(0L);
        Assert.assertFalse(config.isReadyForAutoCooldown());
        Assert.assertEquals(3, config.getValidProperties().size());
        config.setWaitSecond(3600L);
        Assert.assertEquals(3, config.getValidProperties().size());

        config.setSchedule(null);
        Assert.assertFalse(config.isReadyForAutoCooldown());
        Assert.assertEquals(2, config.getValidProperties().size());
        config.setSchedule("");
        Assert.assertFalse(config.isReadyForAutoCooldown());
        Assert.assertEquals(3, config.getValidProperties().size());
        config.setSchedule("START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE");
        Assert.assertEquals(3, config.getValidProperties().size());

        config.setTarget(null);
        Assert.assertFalse(config.isReadyForAutoCooldown());
        Assert.assertEquals(2, config.getValidProperties().size());
        config.setTarget("");
        Assert.assertFalse(config.isReadyForAutoCooldown());
        Assert.assertEquals(3, config.getValidProperties().size());
        config.setTarget("iceberg.db1.tbl1");
        Assert.assertEquals(3, config.getValidProperties().size());
    }

    @Test
    public void testConstructor() {
        ExternalCooldownConfig config = new ExternalCooldownConfig(
                "iceberg.db1.tbl1", "START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE", 3600L);
        Assert.assertTrue(config.isReadyForAutoCooldown());
        Assert.assertTrue(config.equals(config));

        ExternalCooldownConfig config2 = new ExternalCooldownConfig(config);
        Assert.assertTrue(config2.isReadyForAutoCooldown());
        Assert.assertEquals(config2.hashCode(), config.hashCode());

        ExternalCooldownConfig config3 = new ExternalCooldownConfig();
        Assert.assertNull(config3.getWaitSecond());
        Assert.assertNull(config3.getSchedule());
        Assert.assertNull(config3.getTarget());
    }

    @Test
    public void testMergeUpdateFromProperties() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectorPlanTestBase.mockCatalog(UtFrameUtils.createDefaultCtx(), MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);

        ExternalCooldownConfig config = new ExternalCooldownConfig(
                "iceberg.db1.tbl1", "START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE", 3600L);

        ExternalCooldownConfig config2 = new ExternalCooldownConfig(
                "iceberg0.partitioned_transforms_db.t0_day",
                "START 01:00 END 07:59 EVERY INTERVAL 5 MINUTE", 1800L);
        config.mergeUpdateFromProperties(config2.getValidProperties());
        Assert.assertEquals("iceberg0.partitioned_transforms_db.t0_day", config.getTarget());
        Assert.assertEquals("START 01:00 END 07:59 EVERY INTERVAL 5 MINUTE", config.getSchedule());
        Assert.assertEquals((Long) 1800L, config.getWaitSecond());

        ExternalCooldownConfig config3 = new ExternalCooldownConfig();
        config.mergeUpdateFromProperties(config3.getValidProperties());
        Assert.assertEquals("iceberg0.partitioned_transforms_db.t0_day", config.getTarget());
        Assert.assertEquals("START 01:00 END 07:59 EVERY INTERVAL 5 MINUTE", config.getSchedule());
        Assert.assertEquals((Long) 1800L, config.getWaitSecond());
    }
}