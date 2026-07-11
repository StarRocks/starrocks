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

package com.starrocks.alter;

import com.codahale.metrics.Histogram;
import com.starrocks.lake.LakeTable;
import com.starrocks.metric.MetricRepo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Shared-data FSE v1 (async LakeTableAsyncFastSchemaChangeJob) records
 * alter_job_duration_ms{type=fse_v1} when the job reaches FINISHED.
 */
public class AlterJobDurationMetricV1Test extends LakeFastSchemaChangeTestBase {

    @Override
    protected boolean isFastSchemaEvolutionV2() {
        return false;
    }

    @Test
    public void addColumnRecordsFseV1Duration() throws Exception {
        boolean saved = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            LakeTable table = createTable(connectContext,
                    "CREATE TABLE t_v1 (c0 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 2 "
                            + "PROPERTIES('cloud_native_fast_schema_evolution_v2'='false');");
            Histogram h = AlterColumnMetrics.getDurationHistogram("fse_v1");
            long before = h.getCount();
            executeAlterAndWaitFinish(table, "ALTER TABLE t_v1 ADD COLUMN c1 BIGINT", true);
            Assertions.assertEquals(before + 1L, AlterColumnMetrics.getDurationHistogram("fse_v1").getCount(),
                    "shared-data FSE v1 add-column must record one fse_v1 duration observation on FINISHED");
        } finally {
            MetricRepo.hasInit = saved;
        }
    }
}
