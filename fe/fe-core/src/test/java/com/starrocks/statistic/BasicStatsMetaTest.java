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

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.PlanTestBase;
import mockit.Expectations;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class BasicStatsMetaTest extends PlanTestBase {

    @Before
    public void before() {
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testHealthy() {
        {
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb("default_catalog", "test");
            Table tbl = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable("default_catalog", "test", "region");
            List<Partition> partitions = Lists.newArrayList(tbl.getPartitions());
            new Expectations(partitions.get(0)) {{
                partitions.get(0).getRowCount();
                result = 100L;
            }};
            BasicStatsMeta basicStatsMeta = new BasicStatsMeta(db.getId(), tbl.getId(), List.of(),
                    StatsConstants.AnalyzeType.FULL,
                    LocalDateTime.of(2024, 07, 22, 12, 20), Map.of(), 100);
            Assert.assertEquals(0.05, basicStatsMeta.getHealthy(), 0.01);
        }

        {
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb("default_catalog", "test");
            Table tbl = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable("default_catalog", "test", "supplier");
            List<Partition> partitions = Lists.newArrayList(tbl.getPartitions());
            new Expectations(partitions.get(0)) {{
                partitions.get(0).getRowCount();
                result = 10000L;
            }};
            BasicStatsMeta basicStatsMeta = new BasicStatsMeta(db.getId(), tbl.getId(), List.of(),
                    StatsConstants.AnalyzeType.FULL,
                    LocalDateTime.of(2024, 07, 22, 12, 20), Map.of(), 15000);
            Assert.assertEquals(0.5, basicStatsMeta.getHealthy(), 0.01);
        }

    }

    @After
    public void after() {
        FeConstants.runningUnitTest = false;
    }

}
