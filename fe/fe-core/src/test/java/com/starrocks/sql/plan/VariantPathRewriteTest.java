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

import org.junit.jupiter.api.Test;

public class VariantPathRewriteTest extends ConnectorPlanTestBase {
    private static final String VARIANT_TABLE = "iceberg0.unpartitioned_db.variant_t0";

    @Test
    public void testProjectionRewrite() throws Exception {
        connectContext.getSessionVariable().setEnableVariantPathRewrite(true);
        String sql = "select get_variant_int(v, '$.a.b') from " + VARIANT_TABLE;
        String plan = getFragmentPlan(sql);
        assertContains(plan, "IcebergScanNode");
        assertContains(plan, "v.a.b");

        String verbose = getVerboseExplain(sql);
        assertContains(verbose, "ExtendedColumnAccessPath: [/v(bigint(20))/a(bigint(20))/b(bigint(20))]");
    }

    @Test
    public void testRewriteDisabled() throws Exception {
        connectContext.getSessionVariable().setEnableVariantPathRewrite(false);
        String sql = "select get_variant_int(v, '$.a.b') from " + VARIANT_TABLE;
        String plan = getFragmentPlan(sql);
        assertContains(plan, "get_variant_int");

        String verbose = getVerboseExplain(sql);
        assertNotContains(verbose, "ExtendedColumnAccessPath");
    }

    @Test
    public void testPredicateRewrite() throws Exception {
        connectContext.getSessionVariable().setEnableVariantPathRewrite(true);
        String sql = "select * from " + VARIANT_TABLE + " where get_variant_int(v, '$.a.b') > 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES:");
        assertContains(plan, "v.a.b > 10");

        String verbose = getVerboseExplain(sql);
        assertContains(verbose, "ExtendedColumnAccessPath: [/v(bigint(20))/a(bigint(20))/b(bigint(20))]");
    }

    @Test
    public void testUnsupportedArrayPathNotRewrite() throws Exception {
        connectContext.getSessionVariable().setEnableVariantPathRewrite(true);
        String sql = "select get_variant_int(v, '$.a[0]') from " + VARIANT_TABLE;
        String plan = getFragmentPlan(sql);
        assertContains(plan, "get_variant_int");

        String verbose = getVerboseExplain(sql);
        assertNotContains(verbose, "ExtendedColumnAccessPath");
    }

    @Test
    public void testMixedTypeSamePathPartialRewrite() throws Exception {
        connectContext.getSessionVariable().setEnableVariantPathRewrite(true);
        String sql = "select get_variant_int(v, '$.a.b'), get_variant_double(v, '$.a.b') from " + VARIANT_TABLE;
        String plan = getFragmentPlan(sql);
        assertContains(plan, "v.a.b");
        assertContains(plan, "get_variant_double");

        String verbose = getVerboseExplain(sql);
        assertContains(verbose, "ExtendedColumnAccessPath: [/v(bigint(20))/a(bigint(20))/b(bigint(20))]");
    }

    @Test
    public void testCastVariantQueryRewrite() throws Exception {
        connectContext.getSessionVariable().setEnableVariantPathRewrite(true);
        String sql = "select cast(variant_query(v, '$.profile.rank') as bigint) from " + VARIANT_TABLE;
        String plan = getFragmentPlan(sql);
        assertContains(plan, "v.profile.rank");

        String verbose = getVerboseExplain(sql);
        assertContains(verbose, "ExtendedColumnAccessPath: [/v(bigint(20))/profile(bigint(20))/rank(bigint(20))]");
    }

    @Test
    public void testCastVariantQueryRewriteToTime() throws Exception {
        connectContext.getSessionVariable().setEnableVariantPathRewrite(true);
        String sql = "select cast(variant_query(v, '$.profile.rank') as time) from " + VARIANT_TABLE;
        String plan = getFragmentPlan(sql);
        assertContains(plan, "v.profile.rank");

        String verbose = getVerboseExplain(sql);
        assertContains(verbose, "ExtendedColumnAccessPath");
        assertContains(verbose, "/v(TIME)/profile(TIME)/rank(TIME)");
    }

    @Test
    public void testAggregateRewrite() throws Exception {
        connectContext.getSessionVariable().setEnableVariantPathRewrite(true);
        String sql = "select sum(get_variant_int(v, '$.metrics.views')) from " + VARIANT_TABLE;
        String plan = getFragmentPlan(sql);
        assertContains(plan, "sum");
        assertContains(plan, "v.metrics.views");

        String verbose = getVerboseExplain(sql);
        assertContains(verbose, "ExtendedColumnAccessPath: [/v(bigint(20))/metrics(bigint(20))/views(bigint(20))]");
    }
}
