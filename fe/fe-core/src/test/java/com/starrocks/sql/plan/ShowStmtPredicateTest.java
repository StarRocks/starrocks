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

import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowStmtPredicateTest extends PlanTestBase {
    public ShowResultSet execute(String originStmt) {
        try {
            StatementBase stmt = SqlParser.parse(originStmt, connectContext.getSessionVariable()).get(0);
            Analyzer.analyze(stmt, connectContext);

            if (stmt instanceof ShowStmt show) {
                return ShowExecutor.execute(show, connectContext);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            Assertions.fail();
            throw ex;
        }
        return null;
    }

    @Test
    public void testShowDatabaseTest() {
        String sql = "show data where tableName like 't%' order by `TableName` limit 2; ";

        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
        Assertions.assertEquals("tprimary_multi_cols", show.getResultRows().get(0).get(0));
    }

    @Test
    public void testShowDataWithWhere() {
        String sql = "show data where tableName = 'tprimary_multi_cols';";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
        Assertions.assertEquals(1, show.getResultRows().size());
        Assertions.assertEquals("tprimary_multi_cols", show.getResultRows().get(0).get(0));
    }

    @Test
    public void testShowDataWithOrder() {
        String sql = "show data order by `TableName` asc;";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
        Assertions.assertTrue(show.getResultRows().size() > 1);
        // Verify order is ascending
        String first = show.getResultRows().get(0).get(0);
        String second = show.getResultRows().get(1).get(0);
        Assertions.assertTrue(first.compareTo(second) <= 0);
    }

    @Test
    public void testShowDataWithLimit() {
        String sql = "show data limit 1;";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
        Assertions.assertEquals(1, show.getResultRows().size());
    }

    @Test
    public void testShowDataWithLimitAndOffset() {
        String sql = "show data limit 1 offset 1;";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
        Assertions.assertEquals(1, show.getResultRows().size());
    }

    @Test
    public void testShowDataWithOrderAndLimit() {
        String sql = "show data order by `TableName` limit 1;";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
        Assertions.assertEquals(1, show.getResultRows().size());
    }

    @Test
    public void testShowTablesWithWhere() {
        String sql = "show tables where tableName like 't%';";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
        // At least one table starts with 't'
        boolean hasTableStartsWithT = show.getResultRows().stream()
                .anyMatch(row -> row.get(0).startsWith("t"));
        Assertions.assertTrue(hasTableStartsWithT);
    }

    @Test
    public void testShowTablesWithLimit() {
        String sql = "show tables limit 2;";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
        Assertions.assertTrue(show.getResultRows().size() <= 2);
    }
    @Test
    public void testShowDatabasesWithWhere() {
        String sql = "show databases where `Database` = 'default_cluster';";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
    }

    @Test
    public void testShowDatabasesWithOrder() {
        String sql = "show databases order by `Database` desc;";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
        Assertions.assertTrue(show.getResultRows().size() > 0);
    }

    @Test
    public void testShowDatabasesWithLimit() {
        String sql = "show databases limit 1;";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
        Assertions.assertEquals(1, show.getResultRows().size());
    }

    @Test
    public void testShowColumnsWithWhere() {
        String sql = "show columns from tprimary_multi_cols where Field = 'pk';";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
    }

    @Test
    public void testShowColumnsWithOrder() {
        String sql = "show columns from tprimary_multi_cols order by Field asc;";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
        Assertions.assertTrue(show.getResultRows().size() > 0);
    }

    @Test
    public void testShowColumnsWithLimit() {
        String sql = "show columns from tprimary_multi_cols limit 2;";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
        Assertions.assertTrue(show.getResultRows().size() <= 2);
    }

    @Test
    public void testShowBackendsWithLimit() {
        String sql = "show backends limit 1;";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
    }

    @Test
    public void testShowBackendsWithWhere() {
        String sql = "show backends where Alive = 'true';";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
    }

    @Test
    public void testShowFrontendsWithLimit() {
        String sql = "show frontends limit 1;";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
    }

    @Test
    public void testShowVariablesWithWhere() {
        String sql = "show variables where Variable_name = 'query_timeout';";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
    }

    @Test
    public void testShowVariablesWithLike() {
        String sql = "show variables like 'query%';";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
    }

    @Test
    public void testShowVariablesWithOrder() {
        String sql = "show variables order by Variable_name asc limit 5;";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
        Assertions.assertTrue(show.getResultRows().size() <= 5);
    }

    @Test
    public void testShowCatalogsWithLimit() {
        String sql = "show catalogs limit 1;";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
    }

    @Test
    public void testShowCatalogsWithWhere() {
        String sql = "show catalogs where `Catalog` = 'default_catalog';";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
    }

    @Test
    public void testShowMaterializedViewsWithWhere() {
        String sql = "show materialized views where Name like 'test%';";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
    }

    @Test
    public void testShowMaterializedViewsWithOrder() {
        String sql = "show materialized views order by Name limit 3;";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
    }

    @Test
    public void testShowProcesslistWithLimit() {
        String sql = "show processlist limit 5;";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
    }

    @Test
    public void testShowMultipleOrderBy() {
        String sql = "show data order by `TableName` asc, Size desc limit 3;";
        ShowResultSet show = execute(sql);
        Assertions.assertNotNull(show);
        Assertions.assertTrue(show.getResultRows().size() <= 3);
    }
}
