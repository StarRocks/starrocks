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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class JDBCIdentifierQuoteTest extends ConnectorPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
    }

    @Test
    public void testPostgreSQLIdentifierQuote() throws Exception {
        String sql = "select a, b from jdbc_postgres.partitioned_db0.tbl0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN JDBC");
        assertContains(plan, "TABLE: \"tbl0\"");
        assertContains(plan, "QUERY: SELECT \"a\", \"b\"");
    }

    @Test
    public void testMySQLIdentifierQuote() throws Exception {
        String sql = "select a, b from jdbc0.partitioned_db0.tbl0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN JDBC");
        assertContains(plan, "TABLE: `tbl0`");
        assertContains(plan, "QUERY: SELECT `a`, `b` FROM `tbl0`");
    }
}
