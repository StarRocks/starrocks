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

import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
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

    @Test
    public void testMySQLPassThroughQueryTableFunction() throws Exception {
        String sql = "select a from table(jdbc0.native_query('select a, b from remote_table')) t " +
                "where b = 'x'";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN JDBC");
        assertContains(plan, "TABLE: (select a, b from remote_table) starrocks_query");
        assertContains(plan, "QUERY: SELECT `a`, `b` FROM (select a, b from remote_table) " +
                "starrocks_query WHERE (`b` = 'x')");
    }

    @Test
    public void testMySQLPassThroughQueryTableFunctionSelectStar() throws Exception {
        String sql = "select * from table(jdbc0.native_query('select * from remote_table'))";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN JDBC");
        assertContains(plan, "TABLE: (select * from remote_table) starrocks_query");
        assertContains(plan, "QUERY: SELECT `a`, `b`, `c`, `d` FROM (select * from remote_table) starrocks_query");
    }

    @Test
    public void testMySQLPassThroughQueryTableFunctionInitializesChildExpressions() throws Exception {
        String sql = "select * from table(jdbc0.native_query('select * from remote_table'))";
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Assertions.assertDoesNotThrow(() -> AnalyzerUtils.collectAllSelectTableColumns(statement));
    }

    @Test
    public void testMySQLPassThroughQueryTableFunctionRejectsColumnAlias() {
        String sql = "select * from table(jdbc0.native_query('select a from remote_table')) t(x)";
        assertExceptionMsgContains(sql, "column aliases are not supported for JDBC query table function");
    }

    @Test
    public void testMySQLPassThroughQueryTableFunctionRejectsNamedArgument() {
        String sql = "select * from table(jdbc0.native_query(query => 'select a from remote_table'))";
        assertExceptionMsgContains(sql,
                "JDBC query table function only supports TABLE(<catalog>.native_query('<sql>'))");
    }

    @Test
    public void testMySQLPassThroughQueryTableFunctionDoesNotReserveTwoPartQueryName() {
        String sql = "select * from table(jdbc0.query('select a from remote_table'))";
        assertExceptionMsgContains(sql,
                "Unknown table function 'jdbc0.query(");
    }

    @Test
    public void testMySQLPassThroughQueryTableFunctionRejectsLegacySystemQueryName() {
        String sql = "select * from table(jdbc0.system.query('select a from remote_table'))";
        assertExceptionMsgContains(sql,
                "JDBC query table function only supports TABLE(<catalog>.native_query('<sql>'))");
    }

    @Test
    public void testMySQLPassThroughQueryTableFunctionRejectsInsert() {
        String sql = "select * from table(jdbc0.native_query('insert into remote_table values (1)'))";
        assertExceptionMsgContains(sql, "JDBC query table function only supports SELECT queries");
    }

    @Test
    public void testMySQLPassThroughQueryTableFunctionRejectsDdl() {
        String sql = "select * from table(jdbc0.native_query('create table remote_table(id int)'))";
        assertExceptionMsgContains(sql, "JDBC query table function only supports SELECT queries");
    }

    @Test
    public void testMySQLPassThroughQueryTableFunctionRejectsWithQuery() {
        String sql = "select * from table(jdbc0.native_query('with cte as (select 1) select * from cte'))";
        assertExceptionMsgContains(sql, "JDBC query table function only supports SELECT queries");
    }
}
