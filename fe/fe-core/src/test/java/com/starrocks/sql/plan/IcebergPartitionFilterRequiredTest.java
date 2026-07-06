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
import com.starrocks.sql.common.StarRocksPlannerException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IcebergPartitionFilterRequiredTest extends ConnectorPlanTestBase {
    @BeforeEach
    public void setUp() {
        super.setUp();
        try {
            connectContext.changeCatalogDb("iceberg0.partitioned_db");
            connectContext.getSessionVariable().setAllowLakeWithoutPartitionFilter(false);
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testValidPartitionFilter() throws Exception {
        String sql = "select * from t1 where date = '2020-01-01'";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "IcebergScanNode");
    }

    @Test
    public void testMissingPartitionFilter() {
        String sql = "select * from t1";
        Assertions.assertThrows(StarRocksPlannerException.class, () -> getFragmentPlan(sql));

        String sql2 = "select * from t1 where id = 1";
        Assertions.assertThrows(StarRocksPlannerException.class, () -> getFragmentPlan(sql2));
    }

    @Test
    public void testFunctionWrappedPartitionFilter() {
        String sql = "select * from t1 where upper(date) = '2020-01-01'";
        Assertions.assertThrows(StarRocksPlannerException.class, () -> getFragmentPlan(sql));
    }

    @Test
    public void testLikeInPartitionColumn() {
        String sql = "select * from t1 where date like '2020%'";
        Assertions.assertThrows(StarRocksPlannerException.class, () -> getFragmentPlan(sql));
    }

    @Test
    public void testUnpartitionedTableAlwaysSucceeds() throws Exception {
        String sql = "select * from iceberg0.unpartitioned_db.t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "IcebergScanNode");
    }
}
