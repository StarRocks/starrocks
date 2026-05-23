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

import com.google.common.collect.ImmutableMap;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.sql.optimizer.statistics.IRelaxDictManager;
import mockit.Expectations;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Optional;

public class IcebergCompressedKeyTest extends ConnectorPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeOnLake(true);
        // The lake low-cardinality dict rewrite only runs for queries (DecodeCollector gates the
        // iceberg/hive scan visitors on isQuery). buildPlan() in the test harness does not go through
        // the production setIsQuery(...) path, so QueryState defaults to false and the rewrite is
        // skipped, leaving the group-by key un-encoded and the min-max rule a no-op.
        connectContext.getState().setIsQuery(true);
    }

    @AfterAll
    public static void afterClass() {
        FeConstants.USE_MOCK_DICT_MANAGER = false;
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(false);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeOnLake(false);
        connectContext.getState().setIsQuery(false);
    }

    @Test
    public void testIcebergDictGroupByEmitsMinMaxRange() throws Exception {
        IRelaxDictManager dictManager = IRelaxDictManager.getInstance();
        ColumnDict columnDict = new ColumnDict(
                ImmutableMap.<ByteBuffer, Integer>builder()
                        .put(ByteBuffer.wrap("a".getBytes()), 1)
                        .put(ByteBuffer.wrap("b".getBytes()), 2)
                        .build(),
                0, 0);

        new Expectations(dictManager) {
            {
                dictManager.hasGlobalDict(anyString, "data");
                result = true;
                minTimes = 0;

                dictManager.getGlobalDict(anyString, "data");
                result = Optional.of(columnDict);
                minTimes = 0;
            }
        };

        String sql = "select count(*) from iceberg0.unpartitioned_db.t0 group by data";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "group by min-max stats:");
        assertContains(plan, "- 0:2");
    }
}
