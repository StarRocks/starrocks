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

import com.starrocks.common.DdlException;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IcebergBucketAwareAggFallbackPlanTest extends ConnectorPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        // three workers so surviving-bucket comparisons are meaningful
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
    }

    @AfterAll
    public static void afterClass() {
        try {
            UtFrameUtils.dropMockBackend(10002);
            UtFrameUtils.dropMockBackend(10003);
        } catch (DdlException e) {
            // ignore
        }
    }

    @BeforeEach
    public void setUp() {
        super.setUp();
        try {
            connectContext.changeCatalogDb("iceberg0.bucket_agg_db");
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
        connectContext.getSessionVariable().setPipelineDop(1); // deterministic parallelism
    }

    // the bug scenario: equality prunes to 1 bucket, high-NDV grouping -> two-stage plan
    @Test
    public void testFallbackOnPrunedBucketHighNdvAgg() throws Exception {
        String sql = "select project_id, email, count(*) from t_events " +
                "where project_id = 100 group by project_id, email";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "AGGREGATE (update serialize)");
        assertContains(plan, "AGGREGATE (merge finalize)");
        assertNotContains(plan, "AGGREGATE (update finalize)");
    }

    // counter-test: many buckets survive -> bucket-aware one-stage retained
    @Test
    public void testKeepBucketAwareWhenManyBucketsSurvive() throws Exception {
        String sql = "select project_id, email, count(*) from t_events group by project_id, email";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "AGGREGATE (update finalize)");
        assertNotContains(plan, "AGGREGATE (merge finalize)");
    }

    // counter-test: low-cardinality aggregation stays one-stage even on a single bucket
    @Test
    public void testKeepBucketAwareForLowCardinalityAgg() throws Exception {
        String sql = "select project_id, count(*) from t_events " +
                "where project_id = 100 group by project_id";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "AGGREGATE (update finalize)");
    }

    // conservative behavior without statistics: equality predicate still proves 1 bucket
    @Test
    public void testFallbackWithoutStatsOnEqualityPrunedBucket() throws Exception {
        String sql = "select project_id, email, count(*) from t_events_nostats " +
                "where project_id = 100 group by project_id, email";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "AGGREGATE (merge finalize)");
        assertNotContains(plan, "AGGREGATE (update finalize)");
    }

    // without statistics and without a pruning predicate the feature is preserved
    @Test
    public void testKeepBucketAwareWithoutStatsWithoutPruning() throws Exception {
        String sql = "select project_id, email, count(*) from t_events_nostats " +
                "group by project_id, email";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "AGGREGATE (update finalize)");
    }

    // an unused second bucket dimension (bucket(1024, other_id)) must not mask the fallback:
    // the scan only advertises the intersected dimension, bucket(4, project_id)
    @Test
    public void testUnusedBucketDimensionDoesNotMaskFallback() throws Exception {
        String sql = "select project_id, email, count(*) from t_events_multibucket " +
                "where project_id = 100 group by project_id, email";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "AGGREGATE (merge finalize)");
        assertNotContains(plan, "AGGREGATE (update finalize)");
    }

    // joins keep bucket-aware behavior (fallback is scoped to SHUFFLE_AGG requirements)
    @Test
    public void testColocateJoinUnaffected() throws Exception {
        String sql = "select a.project_id, count(*) from t_events a " +
                "join t_events b on a.project_id = b.project_id " +
                "where a.project_id = 100 group by a.project_id";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "COLOCATE");
    }

    @Test
    public void testRatioZeroDisablesFallback() throws Exception {
        connectContext.getSessionVariable().setLakeBucketAwareMinBucketsPerWorker(0);
        try {
            String sql = "select project_id, email, count(*) from t_events " +
                    "where project_id = 100 group by project_id, email";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "AGGREGATE (update finalize)");
        } finally {
            connectContext.getSessionVariable().setLakeBucketAwareMinBucketsPerWorker(1.0);
        }
    }

    @Test
    public void testFeatureFlagOffStillTwoStage() throws Exception {
        connectContext.getSessionVariable().setEnableBucketAwareExecutionOnLake(false);
        try {
            String sql = "select project_id, email, count(*) from t_events group by project_id, email";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "AGGREGATE (merge finalize)");
        } finally {
            connectContext.getSessionVariable().setEnableBucketAwareExecutionOnLake(true);
        }
    }
}
