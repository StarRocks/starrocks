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

import org.junit.Test;

public class AddIndexOnlyFilterTest extends PlanTestBase {
    @Test
    public void testAddIndexOnlyFilter() throws Exception {
        String sql = "select * from test_all_type order by ngram_search(t1a,\"china\",4) desc limit 10";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "Predicates: ngram_search");
        String thriftPlan = getThriftPlan(sql);
        assertContains(thriftPlan, "is_index_only_filter:true");

        sql = "select ngram_search(t1a,\"china\",4),t1a from test_all_type";
        plan = getVerboseExplain(sql);
        assertContains(plan, "Predicates: ngram_search");
        thriftPlan = getThriftPlan(sql);
        assertContains(thriftPlan, "is_index_only_filter:true");
    }
}
