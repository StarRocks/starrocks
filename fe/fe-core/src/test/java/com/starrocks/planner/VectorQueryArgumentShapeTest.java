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

package com.starrocks.planner;

import com.starrocks.common.FeConstants;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every argument shape isConstantArrayFloat accepts must still reach the vector index.
 *
 * <p>Reading the query vector and deciding whether a query may use the index are separate jobs, and
 * a reader that cannot handle a shape the gate accepted must not quietly turn the query into a full
 * scan, nor fail it. The shapes below are the ones that gate lets through -- a bare array, the
 * string form a prepared parameter produces, that string form under an extra implicit cast, casts
 * around the array, and elements wrapped in casts that constant folding could not evaluate.
 */
public class VectorQueryArgumentShapeTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.enablePruneEmptyOutputScan = false;
        starRocksAssert.withTable("CREATE TABLE test.vector_shapes ("
                + " c0 INT, c1 array<float> NOT NULL,"
                + " INDEX idx (c1) USING VECTOR ('metric_type' = 'l2_distance',"
                + " 'is_vector_normed' = 'false', 'M' = '16', 'index_type' = 'hnsw', 'dim'='5')"
                + ") DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1"
                + " PROPERTIES ('replication_num'='1');");
    }

    private void assertUsesVectorIndex(String vectorArgument) throws Exception {
        String sql = "select c0 from test.vector_shapes order by approx_l2_distance("
                + vectorArgument + ", c1) limit 10";
        String plan = getVerboseExplain(sql);
        assertTrue(plan.contains("VECTORINDEX: ON"),
                "expected the vector index for argument " + vectorArgument + ", got:\n" + plan);
    }

    @Test
    public void testBareArrayLiteral() throws Exception {
        assertUsesVectorIndex("[1.1,2.2,3.3,4.4,5.5]");
        assertUsesVectorIndex("[1,2,3,4,5]");
    }

    @Test
    public void testStringFormAPreparedParameterProduces() throws Exception {
        assertUsesVectorIndex("cast('[1.1,2.2,3.3,4.4,5.5]' as array<float>)");
    }

    @Test
    public void testStringFormUnderAnImplicitCast() throws Exception {
        // cast(... as array<double>) does not match the function signature, so the optimizer wraps
        // it in cast(... as array<float>) and the string form ends up one level down.
        assertUsesVectorIndex("cast('[1.1,2.2,3.3,4.4,5.5]' as array<double>)");
    }

    @Test
    public void testCastAroundTheArray() throws Exception {
        assertUsesVectorIndex("cast([1.1,2.2,3.3,4.4,5.5] as array<double>)");
    }

    @Test
    public void testElementsWrappedInCasts() throws Exception {
        assertUsesVectorIndex("[cast(1.1 as double),cast(2.1 as double),cast(3.1 as double),"
                + "cast(4.1 as double),cast(5.1 as double)]");
        // cast(1.1 as int) is not folded away: ConstantOperator.castTo runs Integer.parseInt("1.1")
        // and gives up, so the cast survives into the plan and the reader has to look through it.
        assertUsesVectorIndex("[cast(1.1 as int),cast(2.1 as int),cast(3.1 as int),"
                + "cast(4.1 as int),cast(5.1 as int)]");
    }
}
