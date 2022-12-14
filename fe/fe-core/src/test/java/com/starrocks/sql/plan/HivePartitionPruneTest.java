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
import com.starrocks.server.GlobalStateMgr;
import org.junit.Before;
import org.junit.Test;

public class HivePartitionPruneTest extends ConnectorPlanTestBase {
    @Before
    public void setUp() throws DdlException {
        GlobalStateMgr.getCurrentState().changeCatalogDb(connectContext, "hive0.partitioned_db");
    }

    @Test
    public void testHivePartitionPrune() throws Exception {
        String sql = "select * from t1 where par_col = 0;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:HdfsScanNode\n" +
                "     TABLE: t1\n" +
                "     PARTITION PREDICATES: 4: par_col = 0\n" +
                "     partitions=1/3");

        sql = "select * from t1 where par_col = 1 and c1 = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "0:HdfsScanNode\n" +
                "     TABLE: t1\n" +
                "     PARTITION PREDICATES: 4: par_col = 1\n" +
                "     NON-PARTITION PREDICATES: 1: c1 = 2\n" +
                "     MIN/MAX PREDICATES: 5: c1 <= 2, 6: c1 >= 2\n" +
                "     partitions=1/3");

        sql = "select * from t1 where par_col = abs(-1) and c1 = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "0:HdfsScanNode\n" +
                "     TABLE: t1\n" +
                "     PARTITION PREDICATES: 4: par_col = CAST(abs(-1) AS INT)\n" +
                "     NON-PARTITION PREDICATES: 1: c1 = 2\n" +
                "     NO EVAL-PARTITION PREDICATES: 4: par_col = CAST(abs(-1) AS INT)\n" +
                "     MIN/MAX PREDICATES: 5: c1 <= 2, 6: c1 >= 2\n" +
                "     partitions=3/3");

        sql = "select * from t1 where par_col = 1+1 and c1 = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "0:HdfsScanNode\n" +
                "     TABLE: t1\n" +
                "     PARTITION PREDICATES: 4: par_col = 2\n" +
                "     NON-PARTITION PREDICATES: 1: c1 = 2\n" +
                "     MIN/MAX PREDICATES: 5: c1 <= 2, 6: c1 >= 2\n" +
                "     partitions=1/3");
    }
}