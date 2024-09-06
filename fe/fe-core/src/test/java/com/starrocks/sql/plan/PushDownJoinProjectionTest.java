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

import org.junit.Test;

public class PushDownJoinProjectionTest extends PlanTestBase {

    @Test
    public void testPushdown() throws Exception {
        starRocksAssert.query("select tarray.v1, array_contains(tarray.v3, 123) " +
                        "from t0 join tarray on(t0.v1 = tarray.v1) ")
                .explainContains("  1:Project\n" +
                        "  |  <slot 4> : 4: v1\n" +
                        "  |  <slot 8> : array_contains(6: v3, 123)\n" +
                        "  |  \n" +
                        "  0:OlapScanNode");
        starRocksAssert.query("select t0.v1, t0.v2, tarray.v1 as c1, array_contains(tarray.v3, 123) as c2\n" +
                        "from t0 join tarray on(t0.v1 = tarray.v1) ")
                .explainContains(" 2:Project\n" +
                        "  |  <slot 4> : 4: v1\n" +
                        "  |  <slot 8> : array_contains(6: v3, 123)\n" +
                        "  |  \n" +
                        "  1:OlapScanNode");
        starRocksAssert.query("select t0.v1, t0.v2, tarray.v1 as c1, 1 + array_contains(tarray.v3, 123) " +
                        "from t0 join tarray on(t0.v1 = tarray.v1) ")
                .explainContains(" 2:Project\n" +
                        "  |  <slot 4> : 4: v1\n" +
                        "  |  <slot 8> : array_contains(6: v3, 123)\n" +
                        "  |  \n" +
                        "  1:OlapScanNode\n" +
                        "     TABLE: tarray");
        starRocksAssert.query("select t0.v1, t0.v2, array_map(tarray.v3, x -> x + 1) " +
                        "from t0 join tarray on(t0.v1 = tarray.v1) ")
                .explainContains("  4:Project\n" +
                        "  |  <slot 1> : 1: v1\n" +
                        "  |  <slot 2> : 2: v2\n" +
                        "  |  <slot 8> : array_map(<slot 7> -> <slot 7> + 1, 6: v3)\n" +
                        "  |  \n" +
                        "  3:HASH JOIN");

        // multiple table join
        starRocksAssert.query("select t0.v1, t0.v2, " +
                        "   a1.v1 as c1, array_contains(a1.v3, 123) as c2, " +
                        "   t1.v4, t1.v5" +
                        " from t0 " +
                        "   join tarray a1 on(t0.v1 = a1.v1) " +
                        "   join t1 on(t0.v1 = t1.v4) ")
                .explainContains(" 3:Project\n" +
                        "  |  <slot 4> : 4: v1\n" +
                        "  |  <slot 11> : array_contains(6: v3, 123)\n" +
                        "  |  \n" +
                        "  2:OlapScanNode\n" +
                        "     TABLE: tarray");
        starRocksAssert.query("select t0.v1, t0.v2, " +
                        "   a1.v1 as c1, array_contains(a1.v3, 123) as c2, " +
                        "   a2.v1 as c3, map_size(a2.v3) as c4 " +
                        " from t0 " +
                        "   join tarray a1 on(t0.v1 = a1.v1) " +
                        "   join tmap a2 on(t0.v1 = a2.v1) ")
                .explainContains(" 3:Project\n" +
                                "  |  <slot 4> : 4: v1\n" +
                                "  |  <slot 12> : array_contains(6: v3, 123)\n" +
                                "  |  \n" +
                                "  2:OlapScanNode\n" +
                                "     TABLE: tarray",
                        " 7:Project\n" +
                                "  |  <slot 7> : 7: v1\n" +
                                "  |  <slot 13> : map_size(9: v3)\n" +
                                "  |  \n" +
                                "  6:OlapScanNode\n" +
                                "     TABLE: tmap");
        starRocksAssert.query("select " +
                        "   a1.v1 as c1, array_contains(a1.v3, 123) as c2, " +
                        "   a2.v1 as c3, array_contains(a2.v3, 456) as c4 " +
                        " from t0 " +
                        "   join tarray a1 on(t0.v1 = a1.v1) " +
                        "   join tarray a2 on(t0.v1 = a2.v1) ")
                .explainContains(" 1:Project\n" +
                                "  |  <slot 4> : 4: v1\n" +
                                "  |  <slot 12> : array_contains(6: v3, 123)\n" +
                                "  |  \n" +
                                "  0:OlapScanNode\n" +
                                "     TABLE: tarray",
                        "7:Project\n" +
                                "  |  <slot 7> : 7: v1\n" +
                                "  |  <slot 13> : array_contains(9: v3, 456)\n" +
                                "  |  \n" +
                                "  6:OlapScanNode\n" +
                                "     TABLE: tarray");

        // simple expression: do not pushdown
        starRocksAssert.query("select t0.v1, t0.v2, " +
                        "tarray.v1 as c1, 1 + tarray.v1 as c2\n" +
                        "from t0 join tarray on(t0.v1 = tarray.v1) ")
                .explainContains("    UNPARTITIONED\n" +
                        "\n" +
                        "  1:OlapScanNode\n" +
                        "     TABLE: tarray", "-2:EXCHANGE\n" +
                        "  |    \n" +
                        "  0:OlapScanNode\n" +
                        "     TABLE: t0");
    }
}
