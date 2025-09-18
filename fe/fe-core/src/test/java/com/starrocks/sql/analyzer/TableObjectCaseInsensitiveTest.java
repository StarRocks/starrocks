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

package com.starrocks.sql.analyzer;

import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.InvalidConfException;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SetExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.ShowTableStmt;
import com.starrocks.sql.ast.UseDbStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.connectContext;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.starRocksAssert;

public class TableObjectCaseInsensitiveTest {

    @BeforeAll
    public static void beforeClass() throws Exception {
        Config.enable_table_name_case_insensitive = true;
        AnalyzeTestUtil.initWithoutTableAndDb(RunMode.SHARED_NOTHING);
        starRocksAssert.withDatabase("test_db").useDatabase("test_db");
        starRocksAssert.withTable("CREATE TABLE test_db.t0 (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @Test
    public void testModifyConfigFailed() throws Exception {
        AdminSetConfigStmt adminSetConfigStmt = (AdminSetConfigStmt) UtFrameUtils.parseStmtWithNewParser(
                "admin set frontend config(\"enable_table_name_case_insensitive\" = \"false\");",
                connectContext);
        ExceptionChecker.expectThrowsWithMsg(InvalidConfException.class,
                "Config 'enable_table_name_case_insensitive' does not exist or is not mutable",
                () -> DDLStmtExecutor.execute(adminSetConfigStmt, connectContext));

        SetStmt stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(
                "set global enable_table_name_case_insensitive = false", connectContext);
        SetExecutor executor = new SetExecutor(connectContext, stmt);

        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Variable 'enable_table_name_case_insensitive' is a read only variable",
                executor::execute);
    }

    @Test
    public void testTableObjectCaseInsensitive() throws Exception {
        CreateDbStmt createDbStmt =
                (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser("create database TEST_db", connectContext);
        ExceptionChecker.expectThrowsWithMsg(AlreadyExistsException.class,
                "Database Already Exists",
                () -> GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName()));

        analyzeSuccess("use test_db");
        analyzeSuccess("use TEST_Db");
        analyzeSuccess("set catalog Default_cataLOG");
        analyzeFail("select * from non_db");
        analyzeSuccess("select * from TEST_db.T0");
        analyzeSuccess("select * from DEFAULT_cataLOG.TEST_db.T0");
        analyzeSuccess("select A.V1 from t0 a");
        analyzeSuccess("with X0 as (select 111 v1) select L.* from X0 l join X0 r on L.v1 = R.V1");
        analyzeSuccess("with cte as (select 222 c2) select C2 from CTE");

        ShowTableStmt stmt = new ShowTableStmt("test_DB", false, null);
        ShowResultSet res = ShowExecutor.execute(stmt, connectContext);
        Assertions.assertEquals("t0", res.getResultRows().get(0).get(0));
    }

    @Test
    public void testViewCaseInsensitive() throws Exception {
        String createView = "create view test_Db.viEw1 as select * from test_db.t0";
        analyzeSuccess(createView);
        CreateViewStmt createViewStmt =
                (CreateViewStmt) UtFrameUtils.parseStmtWithNewParser(createView, connectContext);
        connectContext.getGlobalStateMgr().getLocalMetastore().createView(createViewStmt);

        analyzeSuccess("select * from test_db.VIEW1");

        ShowTableStmt stmt = new ShowTableStmt("test_DB", false, null);
        ShowResultSet res = ShowExecutor.execute(stmt, connectContext);
        Assertions.assertEquals("view1", res.getResultRows().get(1).get(0));
    }

    @Test
    public void testBasicTableReferenceCaseInsensitive() {
        analyzeSuccess("select * from t0");
        analyzeSuccess("select * from T0");
        analyzeSuccess("select * from TEST_db.t0");
        analyzeSuccess("select * from test_DB.T0");
        analyzeSuccess("select * from DEFAULT_catalog.test_db.T0");
        analyzeSuccess("select * from default_CATALOG.TEST_db.t0");
    }

    @Test
    public void testColumnReferenceCaseInsensitive() {
        analyzeSuccess("select V1, v2, V3 from t0");
        analyzeSuccess("select v1, V2, v3 from T0");
        analyzeSuccess("select t0.V1, t0.v2 from T0");

        analyzeSuccess("select A.V1, a.v2, A.V3 from t0 a");
        analyzeSuccess("select T.v1, t.V2 from T0 T");
    }

    @Test
    public void testWhereCaseInsensitive() {
        analyzeSuccess("select * from t0 where V1 > 100");
        analyzeSuccess("select * from T0 where v1 > 100 and V2 < 200");
        analyzeSuccess("select * from t0 where V1 = V2 or v3 is null");
    }

    @Test
    public void testGroupByCaseInsensitive() {
        analyzeSuccess("select V1, count(*) from t0 group by v1");
        analyzeSuccess("select v1, V2, sum(V3) from T0 group by V1, v2");
        analyzeSuccess("select V1, count(*) from t0 group by V1 having count(*) > 1");
    }

    @Test
    public void testOrderByCaseInsensitive() {
        analyzeSuccess("select * from t0 order by V1");
        analyzeSuccess("select * from T0 order by v1 desc, V2 asc");
        analyzeSuccess("select V1 as id, V2 as value from t0 order by ID, Value");
    }

    @Test
    public void testJoinCaseInsensitive() {
        analyzeSuccess("select * from t0 A inner join T0 B on A.v1 = B.V1");
        analyzeSuccess("select A.V1, b.v2 from T0 a left join t0 B on a.V1 = b.v1");
        analyzeSuccess("select * from t0 T1 cross join T0 t2");

        analyzeSuccess("select * from t0 A, T0 B where A.v1 = B.V1 and a.V2 > b.v2");
    }

    @Test
    public void testSubqueryCaseInsensitive() {
        analyzeSuccess("select * from (select V1, v2 from t0) AS SUb where sub.V1 > 100");
        analyzeSuccess("select * from t0 where V1 in (select v1 from T0 where V2 > 50)");
        analyzeSuccess("select * from t0 where exists (select 1 from T0 t where t.V1 = t0.v1)");

        analyzeSuccess("select * from t0 outer_t where V1 > (select avg(v1) from T0 inner_t where inner_t.V2 = outer_t.v2)");
    }

    @Test
    public void testCTEComplexCaseInsensitive() {
        analyzeSuccess("with High_Values as (select V1, v2 from t0 where V1 > 100), " +
                "Low_Values as (select v1, V2 from T0 where v1 <= 100) " +
                "select * from HIGH_values union all select * from low_VALUES");

        analyzeSuccess("with cte as (select V1, V2 from t0) select CTE.v1, cte.V2 from CTE");
    }

    @Test
    public void testWindowFunctionCaseInsensitive() {
        analyzeSuccess("select V1, row_number() over (partition by V2 order by v1) from t0");
        analyzeSuccess("select v1, V2, rank() over (order by V1 desc) from T0");
        analyzeSuccess("select V1, sum(v2) over (partition by V3) from t0");
    }

    @Test
    public void testAggregateFunctionCaseInsensitive() {
        analyzeSuccess("select COUNT(*), SUM(V1), AVG(v2), MAX(V3), MIN(v1) from t0");
        analyzeSuccess("select count(distinct V1), sum(distinct v2) from T0");
        analyzeSuccess("select V1, count(*) from t0 group by v1 having COUNT(*) > 1");
    }

    @Test
    public void testCaseSensitiveInStringLiterals() {
        analyzeSuccess("select * from t0 where cast(V1 as varchar) = 'Test'");
        analyzeSuccess("select 'Hello' as greeting, V1 from T0");

        analyzeSuccess("select 'test' as Test_Col, V1 as test_COL from t0");
    }

    @Test
    public void testInsertSelectCaseInsensitive() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test_db.t1 (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        analyzeSuccess("insert into T1 select V1, v2, V3 from t0");
        analyzeSuccess("insert into t1 (V1, v2, V3) select v1, V2, v3 from T0 where V1 > 100");
    }

    @Test
    public void testComplexExpressionsCaseInsensitive() {
        analyzeSuccess("select V1 + v2 as sum_val, V2 * V3 as product from t0");
        analyzeSuccess("select case when V1 > v2 then V1 else v2 end as max_val from T0");
        analyzeSuccess("select coalesce(V1, v2, V3) from t0");

        analyzeSuccess("select concat(cast(V1 as varchar), '_', cast(v2 as varchar)) from T0");
        analyzeSuccess("select if(V1 > v2, 'greater', 'less_or_equal') from t0");
    }

    @Test
    public void testUnionWithCaseInsensitive() {
        analyzeSuccess("select V1, v2 from t0 union select v2, V3 from T0");
        analyzeSuccess("select V1 as col1, V2 as col2 from t0 union all select v3, V1 from T0 order by COL1");
    }

    @Test
    public void testShowCreateStatements() {
        analyzeSuccess("show create table T0");
        analyzeSuccess("show create table t0");
        analyzeSuccess("show create table test_db.T0");
    }

    @Test
    public void testExplainWithCaseInsensitive() {
        analyzeSuccess("explain select V1, count(*) from T0 group by v1");
        analyzeSuccess("explain verbose select * from t0 where V1 > 100 order by v2");
    }

    @Test
    public void testErrorHandlingCaseInsensitive() {
        analyzeFail("select * from non_existent_table");
        analyzeFail("select * from NON_EXISTENT_TABLE");

        analyzeFail("select non_existent_column from t0");
        analyzeFail("select NON_EXISTENT_COLUMN from T0");

        analyzeFail("select V1 from t0, T0");

        analyzeSuccess("select A.V1 from t0 A, T0 B where A.v1 = B.V1");
    }

    @Test
    public void testDDLStatementsCaseInsensitive() {
        analyzeSuccess("ALTER DATABASE Test_DB SET DATA QUOTA 1024MB");
        analyzeSuccess("alter database TEST_db set data quota 1024MB");

        analyzeSuccess("ALTER TABLE T0 ADD COLUMN new_col INT");
        analyzeSuccess("alter table t0 add column NEW_col int");

        analyzeSuccess("DROP DATABASE IF EXISTS Test_DB");
        analyzeSuccess("drop database if exists TEST_db");

        analyzeSuccess("DROP TABLE IF EXISTS Test_DB.New_Table");
        analyzeSuccess("drop table if exists test_db.NEW_table");
    }

    @Test
    public void testCatalogStatementsCaseInsensitive() {
        analyzeSuccess("CREATE EXTERNAL CATALOG Test_Catalog PROPERTIES('type'='hive')");
        analyzeSuccess("create external catalog TEST_catalog properties('type'='hive')");

        analyzeSuccess("SET CATALOG Test_Catalog");
        analyzeSuccess("set catalog TEST_catalog");

        analyzeSuccess("DROP CATALOG IF EXISTS Test_Catalog");
        analyzeSuccess("drop catalog TEST_catalog");
    }

    @Test
    public void testShowStatementsCaseInsensitive() {
        analyzeSuccess("SHOW TABLES FROM aa");
        analyzeSuccess("show tables from TEST_db");
        analyzeSuccess("SHOW TABLES IN Test_DB LIKE 'T%'");
        analyzeSuccess("show tables in test_db like 't%'");

        analyzeSuccess("SHOW COLUMNS FROM T0");
        analyzeSuccess("show columns from t0");
        analyzeSuccess("SHOW COLUMNS FROM Test_DB.T0");
        analyzeSuccess("show columns from test_db.t0");

        analyzeSuccess("SHOW CREATE DATABASE Test_DB");
        analyzeSuccess("show create database TEST_db");
        analyzeSuccess("SHOW CREATE TABLE T0");
        analyzeSuccess("show create table t0");

        analyzeSuccess("SHOW INDEX FROM T0");
        analyzeSuccess("show index from t0");

        analyzeSuccess("SHOW TABLE STATUS FROM Test_DB");
        analyzeSuccess("show table status from TEST_db");

        analyzeSuccess("SHOW ALTER TABLE column from TEST_db");
    }

    @Test
    public void testBackupRestoreStatementsCaseInsensitive() {
        analyzeSuccess("SHOW BACKUP FROM Test_DB");
        analyzeSuccess("show backup from TEST_db");

        analyzeSuccess("SHOW RESTORE FROM Test_DB");
        analyzeSuccess("show restore from TEST_db");

        analyzeSuccess("CANCEL BACKUP FROM Test_DB");
        analyzeSuccess("cancel backup from TEST_db");
    }

    @Test
    public void testExportStatementsCaseInsensitive() {
        analyzeSuccess("SHOW EXPORT FROM Test_DB");
        analyzeSuccess("show export from TEST_db");
        analyzeSuccess("CANCEL EXPORT FROM Test_DB WHERE queryid = \"921d8f80-7c9d-11eb-9342-acde48001122\"");
    }

    @Test
    public void testTransactionStatementsCaseInsensitive() {
        analyzeSuccess("SHOW TRANSACTION FROM Test_DB WHERE Id = 123");
        analyzeSuccess("show transaction from TEST_db where ID = 123");
    }

    @Test
    public void testPartitionStatementsCaseInsensitive() {
        analyzeSuccess("SHOW DYNAMIC PARTITION TABLES FROM Test_DB");
        analyzeSuccess("show dynamic partition tables from TEST_db");
    }

    @Test
    public void testMaterializedViewStatementsCaseInsensitive() {
        analyzeSuccess("SHOW MATERIALIZED VIEWS FROM Test_DB");
        analyzeSuccess("show materialized views from TEST_db");
    }

    @Test
    public void testDeleteStatementsCaseInsensitive() {
        analyzeSuccess("SHOW DELETE FROM T0");
        analyzeSuccess("show delete from t0");
    }

    @Test
    public void testTableOperationsCaseInsensitive() {
        analyzeSuccess("ALTER TABLE T0 ADD ROLLUP Test_Rollup (V1, V2)");
        analyzeSuccess("alter table t0 add rollup TEST_rollup (v1, v2)");

        analyzeSuccess("ALTER TABLE T0 SWAP WITH T1");
        analyzeSuccess("alter table t0 swap with t1");
    }

    @Test
    public void testComplexIdentifiersCaseInsensitive() {
        analyzeSuccess("select * from `T0`");
        analyzeSuccess("select * from `t0`");

        analyzeSuccess("select T0.V1 from t0");
        analyzeSuccess("select t0.v1 from T0");

        analyzeSuccess("select `T0`.V1, t0.v2 from `T0` T0");
        analyzeSuccess("select T0.`V1`, T0.v2 from t0 T0");
    }

    @Test
    public void testGrantCaseInsensitive() {
        analyzeSuccess("grant select on table T0 to root;");
    }

    @Test
    public void testUseDb() {
        String sql = "use TEST_db";
        UseDbStmt useDbStmt = (UseDbStmt) SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertEquals("test_db", useDbStmt.getDbName());

        sql = "use TEST_DB";
        useDbStmt = (UseDbStmt) SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertEquals("test_db", useDbStmt.getDbName());
    }

    @Test
    public void testCreateDb() {
        String sql = "CREATE DATABASE TEST_db";
        CreateDbStmt createDbStmt = (CreateDbStmt) SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertEquals("test_db", createDbStmt.getFullDbName());

        sql = "CREATE DATABASE TEST_DB";
        createDbStmt = (CreateDbStmt) SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertEquals("test_db", createDbStmt.getFullDbName());
    }

    @Test
    public void testDropDb() {
        String sql = "DROP DATABASE TEST_db";
        DropDbStmt dropDbStmt = (DropDbStmt) SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertEquals("test_db", dropDbStmt.getDbName());

        sql = "DROP DATABASE TEST_DB";
        dropDbStmt = (DropDbStmt) SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertEquals("test_db", dropDbStmt.getDbName());
    }
}
