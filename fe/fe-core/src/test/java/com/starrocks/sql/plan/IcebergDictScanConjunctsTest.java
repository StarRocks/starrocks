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

/**
 * The min/max conjuncts of a lake scan are built by OptExternalPartitionPruner during logical
 * optimization, separately from the scan's own predicate, so DecodeCollector has to collect them
 * explicitly for the dictionary rewrite to reach them. These tests pin that down for each shape
 * buildMinMaxConjunct produces: a bound equal to the source predicate (range), and bounds that are
 * equal to nothing that was collected (= and IN).
 */
public class IcebergDictScanConjunctsTest extends ConnectorPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeOnLake(true);
        // The lake rewrite only runs for queries: DecodeCollector gates the iceberg/hive scan
        // visitors on isQuery, and buildPlan() in the harness does not go through the production
        // setIsQuery(...) path.
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

    private void mockDataColumnDict() {
        IRelaxDictManager dictManager = IRelaxDictManager.getInstance();
        // Only the presence of a dict matters: checkConnectorGlobalDict needs getGlobalDict to return
        // one, and nothing compares its contents against the predicate constants.
        ColumnDict columnDict = new ColumnDict(ImmutableMap.of(ByteBuffer.wrap("a".getBytes()), 1), 0, 0);

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
    }

    // An equality predicate is normalized into two bounds with a different binary type (LE/GE), so
    // neither is equal to the collected conjunct and neither could be found in the expression map
    // by structural coincidence.
    @Test
    public void testEqualityMinMaxConjunctsUseDict() throws Exception {
        mockDataColumnDict();
        String plan = getVerboseExplain("select count(*) from iceberg0.unpartitioned_db.t0 where data = 'a'");
        assertContains(plan, "MIN/MAX PREDICATES: DictDecode(");
        assertContains(plan, "[<place-holder> <= 'a']");
        assertContains(plan, "[<place-holder> >= 'a']");
    }

    // IN is normalized into >= min and <= max over constants that appear nowhere in the predicate.
    @Test
    public void testInPredicateMinMaxConjunctsUseDict() throws Exception {
        mockDataColumnDict();
        String plan = getVerboseExplain("select count(*) from iceberg0.unpartitioned_db.t0 where data in ('a', 'b')");
        assertContains(plan, "MIN/MAX PREDICATES: DictDecode(");
        assertContains(plan, "[<place-holder> >= 'a']");
        assertContains(plan, "[<place-holder> <= 'b']");
    }

    // A range bound is built with the same binary type and children as the source predicate, so it
    // used to be rewritten only because it was equal to the collected conjunct. Guards that shape.
    @Test
    public void testRangeMinMaxConjunctUsesDict() throws Exception {
        mockDataColumnDict();
        String plan = getVerboseExplain("select count(*) from iceberg0.unpartitioned_db.t0 where data > 'a'");
        assertContains(plan, "MIN/MAX PREDICATES: DictDecode(");
        assertContains(plan, "[<place-holder> > 'a']");
    }
}
