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

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import com.starrocks.qe.SessionVariableConstants;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class IcebergTopNRuntimeFilterTest extends ConnectorPlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(0);
    }

    @Test
    public void testIcebergTopNRuntimeFilterOnGroupByOrderByKey() throws Exception {
        int originalAggStage = connectContext.getSessionVariable().getNewPlannerAggStage();
        boolean originalDisableSingleTableStats =
                connectContext.getSessionVariable().disableTableStatsFromMetadataForSingleTable();
        try {
            connectContext.getSessionVariable()
                    .setNewPlanerAggStage(SessionVariableConstants.AggregationStage.TWO_STAGE.ordinal());
            connectContext.getSessionVariable().setDisableTableStatsFromMetadataForSingleTable(true);

            String sql = "select id, count(*) as revenue from iceberg0.unpartitioned_db.t0 " +
                    "group by id order by id desc limit 10";
            assertVerbosePlanContains(sql,
                    "icebergscannode",
                    "build runtime filters:",
                    "probe runtime filters:");
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(originalAggStage);
            connectContext.getSessionVariable()
                    .setDisableTableStatsFromMetadataForSingleTable(originalDisableSingleTableStats);
        }
    }

    @Test
    public void testIcebergNoAggTopNRuntimeFilterOnAggregateOrderKey() throws Exception {
        int originalAggStage = connectContext.getSessionVariable().getNewPlannerAggStage();
        boolean originalDisableSingleTableStats =
                connectContext.getSessionVariable().disableTableStatsFromMetadataForSingleTable();
        try {
            connectContext.getSessionVariable()
                    .setNewPlanerAggStage(SessionVariableConstants.AggregationStage.TWO_STAGE.ordinal());
            connectContext.getSessionVariable().setDisableTableStatsFromMetadataForSingleTable(true);

            String sql = "select id, count(*) as revenue from iceberg0.unpartitioned_db.t0 " +
                    "group by id order by revenue desc limit 10";
            assertVerbosePlanNotContains(sql, "build runtime filters:", "probe runtime filters:");
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(originalAggStage);
            connectContext.getSessionVariable()
                    .setDisableTableStatsFromMetadataForSingleTable(originalDisableSingleTableStats);
        }
    }

    @Test
    public void testInternalTableTopNRuntimeFilterStillWorks() throws Exception {
        int originalAggStage = connectContext.getSessionVariable().getNewPlannerAggStage();
        try {
            connectContext.getSessionVariable()
                    .setNewPlanerAggStage(SessionVariableConstants.AggregationStage.TWO_STAGE.ordinal());

            String sql = "select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue " +
                    "from lineitem group by l_orderkey order by l_orderkey desc limit 10";
            assertVerbosePlanContains(sql,
                    "olapscannode",
                    "build runtime filters:",
                    "probe runtime filters:");
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(originalAggStage);
        }
    }
}
