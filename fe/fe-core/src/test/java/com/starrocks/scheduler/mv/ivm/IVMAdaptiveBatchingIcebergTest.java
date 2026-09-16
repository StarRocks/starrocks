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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.Config;
import com.starrocks.common.tvr.TvrVersionRange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class IVMAdaptiveBatchingIcebergTest extends MVIVMIcebergTestBase {

    @Test
    public void testFirstOversizedDeltaStillBatchesAndConverges() throws Exception {
        starRocksAssert.withMaterializedView(
                "CREATE MATERIALIZED VIEW `test`.`test_mv_batch` REFRESH DEFERRED MANUAL " +
                        "PROPERTIES (\"refresh_mode\" = \"incremental\") " +
                        "AS SELECT id, data, date FROM `iceberg0`.`unpartitioned_db`.`t0`;");
        MaterializedView mv = getMv("test_mv_batch");
        seedTvrBaselineAtVersionZero(mv);          // committed = 0 -> routes to pure IVM, not first-refresh PCT
        advanceTableVersionTo(4);
        // Version 1 alone (5 rows) already exceeds the cap; versions 2-4 add 1 row each.
        mockListTableDeltaTraitsPerVersion(5, 1, 1, 1);

        long savedRows = Config.mv_max_rows_per_refresh;
        Config.mv_max_rows_per_refresh = 2;
        try {
            ImmutableList.Builder<Long> committed = ImmutableList.builder();
            int guard = 0;
            while (getIVMRefreshedExecPlan(mv) != null) {
                committed.add(committedToVersion(mv));
                Assertions.assertTrue(++guard <= 10, "refresh did not converge");
            }
            // The oversized first delta becomes its own batch, then one version per run, converging
            // to head 4. Before the shared-loop fix this collapsed to a single unbounded run ([4]).
            Assertions.assertEquals(ImmutableList.of(1L, 2L, 3L, 4L), committed.build(),
                    "an oversized first delta must still split into bounded batches");
        } finally {
            Config.mv_max_rows_per_refresh = savedRows;
        }
    }

    private static long committedToVersion(MaterializedView mv) {
        Map<BaseTableInfo, TvrVersionRange> committed = mv.getRefreshScheme().getAsyncRefreshContext()
                .getBaseTableInfoTvrVersionRangeMap();
        return committed.values().iterator().next().to().getVersion();
    }
}
