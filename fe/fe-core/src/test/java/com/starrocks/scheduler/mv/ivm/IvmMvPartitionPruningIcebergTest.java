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
import com.starrocks.catalog.MaterializedView;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IvmMvPartitionPruningIcebergTest extends MVIVMIcebergTestBase {

    @Test
    public void testIcebergBaseKeepsWholeMvScan() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`test_mv1` " +
                "PARTITION BY str2date(`date`, '%Y-%m-%d') " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\"refresh_mode\" = \"incremental\") " +
                "AS SELECT date, sum(id) FROM `iceberg0`.`partitioned_db`.`t1` GROUP BY date;");
        try {
            MaterializedView mv = getMv("test_mv1");
            seedTvrBaselineAtVersionZero(mv);
            getIVMRefreshedExecPlan(mv);

            MockIcebergMetadata metadata = (MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr()
                    .getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).get();
            metadata.updatePartitions("partitioned_db", "t1", ImmutableList.of("date=2020-01-02"));
            advanceTableVersionTo(2);

            ExecPlan plan = getIVMRefreshedExecPlan(mv);
            Assertions.assertNotNull(plan);
            String explain = plan.getExplainString(TExplainLevel.NORMAL);
            int scan = explain.indexOf("TABLE: test_mv1\n     PREAGGREGATION");
            Assertions.assertTrue(scan >= 0, "no MV scan in plan:\n" + explain);
            Assertions.assertTrue(explain.substring(scan, Math.min(explain.length(), scan + 300))
                    .contains("partitions=4/4"), explain);
        } finally {
            starRocksAssert.dropMaterializedView("test_mv1");
        }
    }
}
