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

package com.starrocks.sql.analyzer.mv;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.lake.bookmark.BookmarkTestBase;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A refresh must encode {@code __ROW_ID__} with the version its materialized view was created with, on every
 * retractable path that builds one.
 *
 * <p>That encoding is the mv's primary key, so an mv re-encoded under a different version matches none of its
 * own rows: its retractions stop cancelling and it keeps the rows they should have deleted, with no error.
 * Each case pins the version the deduction would not choose on its own, so a path that re-deduces instead of
 * reading the stored version fails here rather than in a user's data.
 */
public class IvmRowIdVersionPinTest extends BookmarkTestBase {

    private static final int VERSION_SORT_KEY = 0;
    private static final int VERSION_FINGERPRINT = 1;

    @BeforeAll
    public static void beforeAll() throws Exception {
        createPkTable("pin_pk_int");
        createPkTable("pin_pk_int2");
    }

    @ParameterizedTest(name = "refresh of {0} keeps the pinned version")
    @CsvSource(delimiter = '|', value = {
            "projection    | SELECT k, val FROM D.pin_pk_int",
            "aggregate     | SELECT k, sum(val) AS s FROM D.pin_pk_int GROUP BY k",
            "union         | SELECT k, val FROM D.pin_pk_int UNION ALL SELECT k, val FROM D.pin_pk_int2",
            "derived table | SELECT t.k, t.val FROM (SELECT k, val FROM D.pin_pk_int) t",
            "join          | SELECT a.k, a.val FROM D.pin_pk_int a INNER JOIN D.pin_pk_int2 b ON a.k = b.k",
    })
    public void testRefreshUsesThePinnedVersion(String shape, String query) throws Exception {
        assertRefreshUses(query, VERSION_SORT_KEY, FunctionSet.ENCODE_SORT_KEY);
        assertRefreshUses(query, VERSION_FINGERPRINT, FunctionSet.ENCODE_FINGERPRINT_SHA256);
    }

    private static void assertRefreshUses(String query, int pinnedVersion, String expectedFunction)
            throws Exception {
        CreateMaterializedViewStatement stmt = parseMvDdl(createMvDdl(query));
        QueryStatement queryStatement = stmt.getQueryStatement();
        Analyzer.analyze(queryStatement, connectContext);

        new IVMAnalyzer(connectContext, stmt, queryStatement)
                .rewriteForRefresh(MaterializedView.RefreshMode.INCREMENTAL, pinnedVersion);

        assertEquals(expectedFunction, rowIdEncodeFunctionName(queryStatement));
        assertEquals(pinnedVersion, stmt.getEncodeRowIdVersion());
    }

    /** The row id is output column 0: {@code FROM_BINARY(<encode>(keys), 'encode64')}. */
    private static String rowIdEncodeFunctionName(QueryStatement queryStatement) {
        QueryRelation relation = queryStatement.getQueryRelation();
        if (relation instanceof SetOperationRelation) {
            relation = ((SetOperationRelation) relation).getRelations().get(0);
        }
        Expr rowId = ((SelectRelation) relation).getOutputExpression().get(0);
        assertTrue(FunctionSet.FROM_BINARY.equalsIgnoreCase(((FunctionCallExpr) rowId).getFunctionName()),
                "output column 0 must be the wrapped row id but was: " + rowId);
        return ((FunctionCallExpr) ((FunctionCallExpr) rowId).getChild(0)).getFunctionName().toLowerCase();
    }

    private static String createMvDdl(String query) {
        return "CREATE MATERIALIZED VIEW mv_pinned REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") AS " + query.replace("D.", DB_NAME + ".");
    }

    private static CreateMaterializedViewStatement parseMvDdl(String ddl) {
        return (CreateMaterializedViewStatement) SqlParser.parse(ddl,
                connectContext.getSessionVariable().getSqlMode()).get(0);
    }

    private static void createPkTable(String name) throws Exception {
        String ddl = "CREATE TABLE " + name + " (k INT NOT NULL, val INT) PRIMARY KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', 'enable_change_data_capture' = 'true');";
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(ddl, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
    }
}
