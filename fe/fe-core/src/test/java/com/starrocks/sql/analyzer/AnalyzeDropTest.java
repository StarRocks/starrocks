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

import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeDropTest {

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testDropTable() {
        analyzeSuccess("drop table if exists table_to_drop force");
        analyzeSuccess("drop table if exists table_to_drop");
        analyzeSuccess("drop table table_to_drop force");
        analyzeSuccess("drop table test.table_to_drop");
        analyzeFail("drop table exists table_to_drop");
        DropTableStmt stmt = (DropTableStmt) analyzeSuccess("drop table if exists test.table_to_drop force");
        Assertions.assertEquals("test", stmt.getDbName());
        Assertions.assertEquals("table_to_drop", stmt.getTableName());
        Assertions.assertTrue(stmt.isSetIfExists());
        Assertions.assertTrue(stmt.isForceDrop());
        stmt = (DropTableStmt) analyzeSuccess("drop table t0");
        Assertions.assertEquals("test", stmt.getDbName());
        Assertions.assertEquals("t0", stmt.getTableName());
        Assertions.assertFalse(stmt.isSetIfExists());
        Assertions.assertFalse(stmt.isForceDrop());
    }

    @Test
    public void testForceDropTableWithMvDependency() throws Exception {
        StarRocksAssert starRocksAssert = AnalyzeTestUtil.getStarRocksAssert();
        SessionVariable session = AnalyzeTestUtil.getConnectContext().getSessionVariable();
        boolean previous = session.isEnableDropTableCheckMvDependency();
        starRocksAssert.withTable("create table test.force_drop_base (k int) " +
                "distributed by hash(k) buckets 1 properties('replication_num'='1')");
        try {
            starRocksAssert.withMaterializedView("create materialized view test.force_drop_mv " +
                    "distributed by hash(k) buckets 1 refresh manual as select k from test.force_drop_base");
            try {
                session.setEnableDropTableCheckMvDependency(true);
                analyzeFail("drop table test.force_drop_base", "exists mv dependencies");
                analyzeFail("drop table if exists test.force_drop_base", "exists mv dependencies");

                DropTableStmt stmt = (DropTableStmt) analyzeSuccess("drop table test.force_drop_base force");
                Assertions.assertTrue(stmt.isForceDrop());
                analyzeSuccess("drop table if exists test.force_drop_base force");
                Assertions.assertTrue(session.isEnableDropTableCheckMvDependency());
                analyzeFail("drop table test.force_drop_base", "exists mv dependencies");

                // FORCE bypasses only the dependency guard, not existence or object-type checks.
                analyzeFail("drop table test.force_drop_missing force");
                analyzeSuccess("drop table if exists test.force_drop_missing force");
                analyzeFail("drop table test.view_to_drop force");
                analyzeFail("drop table test.force_drop_mv force", "use 'drop materialized view");
                analyzeSuccess("drop table test.t0");

                session.setEnableDropTableCheckMvDependency(false);
                analyzeSuccess("drop table test.force_drop_base");
            } finally {
                starRocksAssert.dropMaterializedView("test.force_drop_mv");
            }
        } finally {
            session.setEnableDropTableCheckMvDependency(previous);
            starRocksAssert.dropTable("test.force_drop_base");
        }
    }

    @Test
    public void testDropFunctionIfExists() {
        // DROP FUNCTION IF EXISTS should not throw when function does not exist
        analyzeSuccess("drop function if exists non_existent_func(int)");
        analyzeSuccess("drop function if exists test.non_existent_func(int)");

        // DROP FUNCTION without IF EXISTS should fail when function does not exist
        analyzeFail("drop function non_existent_func(int)");
        analyzeFail("drop function test.non_existent_func(int)");

        StatementBase dbStmt = AnalyzeTestUtil.parseSql("drop function non_existent_func(int)");
        Assertions.assertThrows(SemanticException.class, () -> {
            Analyzer.analyze(dbStmt, AnalyzeTestUtil.getConnectContext());
        });
    }

    @Test
    public void testDropGlobalFunctionIfExists() {
        // DROP GLOBAL FUNCTION IF EXISTS should not throw when function does not exist
        analyzeSuccess("drop global function if exists non_existent_global_func(int)");

        // DROP GLOBAL FUNCTION without IF EXISTS should fail when function does not exist
        analyzeFail("drop global function non_existent_global_func(int)");

        // Direct analyzer call to guarantee coverage of reportSemanticException line
        StatementBase globalStmt = AnalyzeTestUtil.parseSql("drop global function non_existent_global_func(int)");
        Assertions.assertThrows(SemanticException.class, () -> {
            Analyzer.analyze(globalStmt, AnalyzeTestUtil.getConnectContext());
        });
    }

    @Test
    public void testDropFunctionWithUnsizedParameterizedTypes() {
        analyzeSuccess("drop function if exists non_existent_func(varchar)");
        analyzeSuccess("drop function if exists non_existent_func(char)");
        analyzeSuccess("drop function if exists non_existent_func(varchar, char)");
        analyzeSuccess("drop function if exists non_existent_func(int, varchar)");
        analyzeSuccess("drop function if exists non_existent_func(string, varchar)");
        analyzeSuccess("drop function if exists non_existent_func(decimal)");
    }

    @Test
    public void testDropFunctionWithUnsizedTypesInsideComplexTypes() {
        // ArrayType wrapping an unsized scalar
        analyzeSuccess("drop function if exists non_existent_func(array<varchar>)");
        analyzeSuccess("drop function if exists non_existent_func(array<char>)");
        analyzeSuccess("drop function if exists non_existent_func(array<array<varchar>>)");

        // MapType with unsized key and/or value
        analyzeSuccess("drop function if exists non_existent_func(map<varchar, int>)");
        analyzeSuccess("drop function if exists non_existent_func(map<int, varchar>)");
        analyzeSuccess("drop function if exists non_existent_func(map<varchar, varchar>)");
        analyzeSuccess("drop function if exists non_existent_func(map<char, varchar>)");

        // StructType with unsized field types
        analyzeSuccess("drop function if exists non_existent_func(struct<a varchar>)");
        analyzeSuccess("drop function if exists non_existent_func(struct<a varchar, b int>)");
    }

    @Test
    public void testDropView() {
        analyzeSuccess("drop view if exists view_to_drop");
        analyzeSuccess("drop view view_to_drop");
        analyzeSuccess("drop view test.view_to_drop");
        analyzeFail("drop view view_to_drop force");
        analyzeFail("drop view exists view_to_drop");
        DropTableStmt stmt = (DropTableStmt) analyzeSuccess("drop view if exists test.view_to_drop");
        Assertions.assertEquals("test", stmt.getDbName());
        Assertions.assertEquals("view_to_drop", stmt.getTableName());
        Assertions.assertTrue(stmt.isView());
        Assertions.assertTrue(stmt.isSetIfExists());
        Assertions.assertFalse(stmt.isForceDrop());
    }

}
