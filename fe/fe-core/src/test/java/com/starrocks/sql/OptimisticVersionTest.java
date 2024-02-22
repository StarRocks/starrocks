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

package com.starrocks.sql;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestBase;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OptimisticVersionTest extends PlanTestBase {

    @BeforeAll
    public static void beforeAll() throws Exception {
        PlanTestBase.beforeClass();
    }

    @AfterAll
    public static void afterAll() {
        PlanTestBase.afterClass();
    }

    @Test
    public void testOptimisticVersion() {
        OlapTable table = new OlapTable();

        // initialized
        assertTrue(OptimisticVersion.validateTableUpdate(table, OptimisticVersion.generate()));

        // schema change
        table.lastSchemaUpdateTime.set(OptimisticVersion.generate());
        assertTrue(OptimisticVersion.validateTableUpdate(table, OptimisticVersion.generate()));

        // in update
        table.lastVersionUpdateStartTime.set(OptimisticVersion.generate());
        assertFalse(OptimisticVersion.validateTableUpdate(table, OptimisticVersion.generate()));

        table.lastVersionUpdateEndTime.set(OptimisticVersion.generate());
        assertTrue(OptimisticVersion.validateTableUpdate(table, OptimisticVersion.generate()));
    }

    @Test
    public void testInsert() throws Exception {
        starRocksAssert.withTable("create table test_insert(c1 int, c2 int) " +
                "distributed by hash(c1) " +
                "properties('replication_num'='1')");
        final String sql = "insert into test_insert select * from test_insert";

        List<StatementBase> stmts = SqlParser.parse(sql, new SessionVariable());
        InsertStmt insertStmt = (InsertStmt) stmts.get(0);

        // analyze
        Analyzer.analyze(insertStmt, starRocksAssert.getCtx());
        Map<String, Database> dbs = AnalyzerUtils.collectAllDatabase(starRocksAssert.getCtx(), insertStmt);

        // normal planner
        StatementPlanner.lock(dbs);
        new InsertPlanner(dbs, true).plan(insertStmt, starRocksAssert.getCtx());
        StatementPlanner.unLock(dbs);

        // retry but failed
        new MockUp<OptimisticVersion>() {
            @Mock
            public boolean validateTableUpdate(OlapTable olapTable, long candidateVersion) {
                return false;
            }
        };
        try {
            StatementPlanner.lock(dbs);
            assertThrows(StarRocksPlannerException.class, () ->
                    new InsertPlanner(dbs, true).plan(insertStmt, starRocksAssert.getCtx()));
        } finally {
            StatementPlanner.unLock(dbs);
        }

        // retry and succeed
        new MockUp<OptimisticVersion>() {
            private boolean retried = false;

            @Mock
            public boolean validateTableUpdate(OlapTable olapTable, long candidateVersion) {
                if (retried) {
                    return true;
                }
                retried = true;
                return false;
            }
        };
        try {
            StatementPlanner.lock(dbs);
            new InsertPlanner(dbs, true).plan(insertStmt, starRocksAssert.getCtx());
        } finally {
            StatementPlanner.unLock(dbs);
        }

    }

}