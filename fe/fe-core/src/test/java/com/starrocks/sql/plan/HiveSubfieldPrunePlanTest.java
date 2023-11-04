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

import org.junit.BeforeClass;
import org.junit.Test;

public class HiveSubfieldPrunePlanTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @Test
    public void testAggCTE() throws Exception {
        String sql = "with stream as (select array_agg(col_struct.c0) as t1 from hive0.subfield_db.subfield group by " +
                "col_int) select t1 from stream";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "[/col_struct/c0]");
    }
}
