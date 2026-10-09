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
import com.starrocks.utframe.StarRocksAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * An aggregate-state column stores a serialized aggregate state rather than the values its type
 * describes. The BE reads it with an aggregate function built from the agg state descriptor and
 * never looks at the column type, so it must never be dictionary encoded: it would decode the
 * dictionary codes as if they were still the original values and read wild pointers out of the
 * code buffer.
 *
 * There are two low cardinality implementations and they are mutually exclusive --
 * AddDecodeNodeForDictStringRule bails out when low_cardinality_optimize_v2 is on and
 * LowCardinalityRewriteRule bails out when it is off -- so both have to exclude these columns.
 *
 * The array queries reference the array as a whole: a bare "select col" is subfield pruned and is
 * not a dictionary candidate either way, so it cannot tell the two cases apart.
 */
public class LowCardinalityAggStateTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE `agg_state_dict_t` (\n" +
                "  `k1` bigint,\n" +
                "  `a_plain` array<varchar(100)> REPLACE,\n" +
                "  `s_plain` varchar(100) REPLACE,\n" +
                "  `v_state` array_agg_distinct(varchar(100)),\n" +
                "  `s_state` max(varchar(20))\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");

        // the mock dict manager offers a dictionary for every column, so anything a collector is
        // willing to encode shows up as INT / ARRAY<INT> in the plan
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
    }

    @AfterAll
    public static void afterClass() {
        FeConstants.USE_MOCK_DICT_MANAGER = false;
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);
    }

    private void assertNotEncoded(String sql, String encodedType) throws Exception {
        String plan = getVerboseExplain(sql);
        Assertions.assertFalse(plan.contains(encodedType),
                "aggregate-state column must not be dictionary encoded: " + sql + "\n" + plan);
        Assertions.assertFalse(plan.contains("DictDecode"),
                "aggregate-state column must not need a decode: " + sql + "\n" + plan);
    }

    private void assertEncoded(String sql, String encodedType) throws Exception {
        String plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains(encodedType),
                "an ordinary string column should still be dictionary encoded: " + sql + "\n" + plan);
    }

    // ---------------------------------------------------------------- LowCardinalityRewriteRule

    @Test
    public void testAggStateColumnIsNotDictEncodedV2() throws Exception {
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);
        assertNotEncoded("select array_min(v_state) from agg_state_dict_t", "ARRAY<INT>");
        assertNotEncoded("select k1 from agg_state_dict_t where v_state[0] = 'a'", "ARRAY<INT>");
        assertNotEncoded("select k1, array_min(v_state) from agg_state_dict_t group by k1, v_state", "ARRAY<INT>");
        assertNotEncoded("select max(s_state) from agg_state_dict_t", ", INT,");
    }

    @Test
    public void testOrdinaryColumnIsStillDictEncodedV2() throws Exception {
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);
        assertEncoded("select array_min(a_plain) from agg_state_dict_t", "ARRAY<INT>");
        assertEncoded("select k1 from agg_state_dict_t where a_plain[0] = 'a'", "ARRAY<INT>");
        assertEncoded("select max(s_plain) from agg_state_dict_t", ", INT,");
    }

    // ------------------------------------------------------- AddDecodeNodeForDictStringRule (v1)

    @Test
    public void testAggStateColumnIsNotDictEncodedV1() throws Exception {
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(false);
        // v1 only ever picks scalar varchar columns, and the intermediate type of an
        // aggregate-state column such as max(varchar) is exactly that
        assertNotEncoded("select max(s_state) from agg_state_dict_t", ", INT,");
        assertNotEncoded("select k1 from agg_state_dict_t where s_state = 'a'", ", INT,");
    }

    @Test
    public void testOrdinaryColumnIsStillDictEncodedV1() throws Exception {
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(false);
        assertEncoded("select max(s_plain) from agg_state_dict_t", ", INT,");
    }
}
