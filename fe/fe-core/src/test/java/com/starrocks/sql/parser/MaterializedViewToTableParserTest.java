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

package com.starrocks.sql.parser;

import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.StatementBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Parser-level tests for CK-compatible {@code CREATE MATERIALIZED VIEW ... TO <table>} (logical sink MV).
 */
public class MaterializedViewToTableParserTest {

    private static StatementBase parseOne(String sql) {
        return SqlParser.parse(sql, new SessionVariable()).get(0);
    }

    @Test
    public void testParseToTableRoutesToSyncMvWithTarget() {
        String sql = "CREATE MATERIALIZED VIEW test_mv TO test_db.target_tbl AS " +
                "SELECT k1, sum(v1) FROM test_db.base_tbl GROUP BY k1";
        StatementBase stmt = parseOne(sql);
        Assertions.assertTrue(stmt instanceof CreateMaterializedViewStmt,
                "TO-table MV should route to the synchronous CreateMaterializedViewStmt");
        CreateMaterializedViewStmt mvStmt = (CreateMaterializedViewStmt) stmt;
        Assertions.assertTrue(mvStmt.isLogicalSinkMV());
        Assertions.assertNotNull(mvStmt.getToTableName());
        Assertions.assertEquals("target_tbl", mvStmt.getToTableName().getTbl());
        Assertions.assertEquals("test_db", mvStmt.getToTableName().getDb());
    }

    @Test
    public void testParseToTableWithoutDb() {
        String sql = "CREATE MATERIALIZED VIEW test_mv TO target_tbl AS SELECT k1 FROM base_tbl";
        CreateMaterializedViewStmt mvStmt = (CreateMaterializedViewStmt) parseOne(sql);
        Assertions.assertTrue(mvStmt.isLogicalSinkMV());
        Assertions.assertEquals("target_tbl", mvStmt.getToTableName().getTbl());
    }

    @Test
    public void testRegularSyncMvHasNoToTable() {
        String sql = "CREATE MATERIALIZED VIEW test_mv AS SELECT k1, sum(v1) FROM base_tbl GROUP BY k1";
        StatementBase stmt = parseOne(sql);
        Assertions.assertTrue(stmt instanceof CreateMaterializedViewStmt);
        Assertions.assertFalse(((CreateMaterializedViewStmt) stmt).isLogicalSinkMV());
        Assertions.assertNull(((CreateMaterializedViewStmt) stmt).getToTableName());
    }

    @Test
    public void testToTableWithAsyncClauseIsRejected() {
        // DISTRIBUTION routes to the asynchronous MV path, which is incompatible with TO.
        String sql = "CREATE MATERIALIZED VIEW test_mv TO target_tbl " +
                "DISTRIBUTED BY HASH(k1) REFRESH ASYNC AS SELECT k1 FROM base_tbl";
        Assertions.assertThrows(Exception.class, () -> parseOne(sql));
    }
}
