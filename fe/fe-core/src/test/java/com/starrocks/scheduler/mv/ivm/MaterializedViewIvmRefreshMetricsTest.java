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

package com.starrocks.scheduler.mv.ivm;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.metric.MaterializedViewMetricsEntity;
import com.starrocks.metric.MaterializedViewMetricsRegistry;
import com.starrocks.metric.MetricRepo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * An incremental (IVM) refresh must bump the per-MV and global refresh metrics through the same
 * MVTaskRunProcessor terminal hooks as a PCT refresh, since those hooks are refresh-mode agnostic.
 */
public class MaterializedViewIvmRefreshMetricsTest extends MVIVMIcebergTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVIVMIcebergTestBase.beforeClass();
    }

    @Test
    public void ivmRefreshBumpsPerMvAndGlobalMetrics() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`test_mv1` " +
                "REFRESH DEFERRED MANUAL PROPERTIES (\"refresh_mode\" = \"incremental\") " +
                "AS SELECT id FROM `iceberg0`.`unpartitioned_db`.`t0` GROUP BY id;");
        MaterializedView mv = getMv("test_mv1");
        // Seed the TVR baseline so the refresh runs through the pure IVM processor, not the first-refresh
        // PCT baseline path.
        seedTvrBaselineAtVersionZero(mv);

        String warehouse = mv.getWarehouseName();
        long jobsBefore = MetricRepo.COUNTER_MV_GLOBAL_REFRESH_JOBS.getMetric(warehouse).getValue();
        long successBefore = MetricRepo.COUNTER_MV_GLOBAL_REFRESH_SUCCESS_JOBS.getMetric(warehouse).getValue();
        long durationBefore = MaterializedViewMetricsRegistry.getGlobalDurationHistogram(warehouse).getCount();

        getIVMRefreshedExecPlan(mv);

        IMaterializedViewMetricsEntity iEntity =
                MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
        Assertions.assertInstanceOf(MaterializedViewMetricsEntity.class, iEntity);
        MaterializedViewMetricsEntity entity = (MaterializedViewMetricsEntity) iEntity;
        Assertions.assertEquals(1, entity.counterRefreshJobTotal.getValue(),
                "IVM refresh must count one per-MV job");
        Assertions.assertEquals(1, entity.counterRefreshJobSuccessTotal.getValue(),
                "IVM refresh must count one per-MV success");
        Assertions.assertEquals(1, entity.histRefreshJobDuration.getCount(),
                "IVM refresh must record one per-MV duration sample");

        Assertions.assertEquals(jobsBefore + 1,
                MetricRepo.COUNTER_MV_GLOBAL_REFRESH_JOBS.getMetric(warehouse).getValue(),
                "IVM refresh must bump the global jobs counter");
        Assertions.assertEquals(successBefore + 1,
                MetricRepo.COUNTER_MV_GLOBAL_REFRESH_SUCCESS_JOBS.getMetric(warehouse).getValue(),
                "IVM refresh must bump the global success counter");
        Assertions.assertEquals(durationBefore + 1,
                MaterializedViewMetricsRegistry.getGlobalDurationHistogram(warehouse).getCount(),
                "IVM refresh must record one global duration sample");

        starRocksAssert.dropMaterializedView("test_mv1");
    }
}
