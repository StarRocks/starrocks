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

package com.starrocks.cdc;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.lake.bookmark.BookmarkRange;
import com.starrocks.lake.bookmark.BookmarkTestBase;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.QueryAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ChangePeriod;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Grammar negative + analyzer negative cases for the CHANGES clause.
 *
 * <p>Relies on BookmarkTestBase's SHARED_DATA mini-cluster so the cloud-native
 * guard in QueryAnalyzer ("CHANGES is only supported on cloud-native tables")
 * never fires on these tables — we want the grammar mutex / PK-key guard to be
 * the thing that throws.
 *
 * <p>Note: UtFrameUtils#parseStmtWithNewParser rewraps both ParsingException
 * and SemanticException as AnalysisException, so the grammar tests assert on
 * AnalysisException.
 */
public class ChangesSyntaxTest extends BookmarkTestBase {

    @BeforeAll
    public static void beforeAllSyntax() throws Exception {
        // The plan-test framework wraps each SQL as CREATE VIEW for a round-trip
        // probe; FOR VERSION AS OF / CHANGES are rejected on views, so disable.
        FeConstants.unitTestView = false;

        // Parent's @BeforeAll beforeBase() has already booted the SHARED_DATA
        // cluster and created the db; we just add per-class tables here.
        createTableStatic(
                "CREATE TABLE dup_t (k int, v int) "
                        + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "PROPERTIES ('replication_num' = '1');");

        // PK cloud-native table — needed so the analyzer-level PK guard is the
        // one that throws (the cloud-native guard runs earlier).
        createTableStatic(
                "CREATE TABLE pk_t (k int, v int) "
                        + "PRIMARY KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "PROPERTIES ('replication_num' = '1');");
    }

    @AfterAll
    public static void afterAll() {
        FeConstants.unitTestView = true;
    }

    @Test
    public void testMissingTo() {
        // CHANGES FROM VERSION <n> without `TO VERSION <n>` is a grammar error.
        // parseStmtWithNewParser rewraps ParsingException as AnalysisException,
        // so we assert on the wrapper and inspect its message.
        String sql = "SELECT * FROM dup_t CHANGES FROM VERSION 1";
        AnalysisException ex = assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        String msg = ex.getMessage();
        assertTrue(msg.contains("Unexpected input") && msg.contains("'TO'"),
                "expected parser error naming missing 'TO' token, got: " + msg);
    }

    @Test
    public void testForVersionAsOfBeforeChangesIsMutex() {
        // Grammar allows `(queryPeriod | changePeriod)?` — at most one per relation.
        String sql = "SELECT * FROM dup_t FOR VERSION AS OF 100 "
                + "CHANGES FROM VERSION 1 TO VERSION 2";
        assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
    }

    @Test
    public void testForVersionAsOfAfterChangesIsMutex() {
        // Same grammar mutex, reversed order.
        String sql = "SELECT * FROM dup_t CHANGES FROM VERSION 1 TO VERSION 2 "
                + "FOR VERSION AS OF 100";
        assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
    }

    @Test
    public void testPrimaryKeyTableRejected() {
        // Analyzer rejects CHANGES on a PK table with a stage-1 not-yet-supported message.
        String sql = "SELECT * FROM pk_t CHANGES FROM VERSION 1 TO VERSION 2";
        Exception ex = assertThrows(Exception.class,
                () -> UtFrameUtils.getFragmentPlan(connectContext, sql));
        assertTrue(ex.getMessage().contains("primary-key table is not supported"),
                "expected PK guard message, got: " + ex.getMessage());
    }

    @Test
    public void testUnsupportedPeriodTypeMessage() {
        // The grammar + AstBuilder collapse SYSTEM_TIME onto QueryPeriod.PeriodType.TIMESTAMP,
        // so a real SQL never reaches the defensive default-case throw. We exercise the
        // guard directly with a ChangePeriod whose periodType is null — same code path,
        // same message — so a future enum addition that skips the switch fails loudly here.
        ChangePeriod bad = new ChangePeriod(
                /* periodType */ null,
                new IntLiteral(1L),
                Optional.of(new IntLiteral(2L)),
                /* isStats */ false,
                NodePosition.ZERO);
        SemanticException ex = assertThrows(SemanticException.class,
                () -> QueryAnalyzer.validateChangePeriod(bad));
        assertTrue(ex.getMessage().contains("Unsupported CHANGES period type, expected VERSION or TIMESTAMP"),
                "actual: " + ex.getMessage());
    }

    @Test
    public void testUnsupportedTableTypeMessage() {
        // information_schema.tables is a built-in SchemaTable (TableType.SCHEMA),
        // not an OlapTable — the analyzer must report this as an unsupported table type.
        String sql = "SELECT * FROM information_schema.tables CHANGES FROM VERSION 1 TO VERSION 2";
        AnalysisException ex = assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        assertTrue(ex.getMessage().contains("Unsupported table type for CHANGES, table type:"),
                "actual: " + ex.getMessage());
    }

    @Test
    public void testVersionRequiresBigintMessage() {
        // VERSION endpoints must be fixed-point integers; a quoted string must be
        // rejected at analyzer time with the BIGINT requirement message.
        String sql = "SELECT * FROM dup_t CHANGES FROM VERSION 'abc' TO VERSION 'xyz'";
        AnalysisException ex = assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        assertTrue(ex.getMessage().contains("CHANGES VERSION requires BIGINT"),
                "actual: " + ex.getMessage());
    }

    @Test
    public void testTimestampRequiresDatetimeMessage() {
        // TIMESTAMP endpoints must be castable to DATETIME (string / date / datetime);
        // a bare bigint literal must be rejected with the DATETIME-castable message.
        String sql = "SELECT * FROM dup_t CHANGES FROM TIMESTAMP 12345 TO TIMESTAMP 67890";
        AnalysisException ex = assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        assertTrue(ex.getMessage().contains("CHANGES TIMESTAMP requires a DATETIME-castable expression"),
                "actual: " + ex.getMessage());
    }

    @Test
    public void testStatsNotYetSupported() {
        // Spec section 4 reserves STATS for a future stage; the analyzer must reject
        // CHANGES STATS up front so users get a clear not-yet-supported message
        // rather than a downstream failure from the version-interval code path.
        String sql = "SELECT * FROM dup_t CHANGES STATS FROM 1 TO 2";
        AnalysisException ex = assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        assertTrue(ex.getMessage().contains("CHANGES STATS is not yet supported"),
                "actual: " + ex.getMessage());
    }

    @Test
    public void testHintParsesIntoBookmarkRange() throws Exception {
        String sql = "SELECT * FROM dup_t [_CHANGES_5_7_]";
        QueryStatement stmt = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                sql, connectContext);
        TableRelation tr = (TableRelation) ((SelectRelation) stmt.getQueryRelation()).getRelation();
        Optional<BookmarkRange> range = tr.getBookmarkRange();
        assertTrue(range.isPresent());
        assertEquals(5L, range.get().base());
        assertEquals(7L, range.get().head());
    }

    @Test
    public void testHintMalformedThrows() {
        String sql = "SELECT * FROM dup_t [_CHANGES_5_]";
        AnalysisException ex = assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        assertTrue(ex.getMessage().contains("invalid changes hint format"),
                "actual: " + ex.getMessage());
    }

    @Test
    public void testHintDuplicateThrows() {
        String sql = "SELECT * FROM dup_t [_CHANGES_1_2_, _CHANGES_3_4_]";
        AnalysisException ex = assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        assertTrue(ex.getMessage().contains("multiple changes hints are not allowed"),
                "actual: " + ex.getMessage());
    }

    @Test
    public void testHintIdOutOfBigintRange() {
        String sql = "SELECT * FROM dup_t [_CHANGES_99999999999999999999_1_]";
        AnalysisException ex = assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        assertTrue(ex.getMessage().contains("out of BIGINT range"),
                "actual: " + ex.getMessage());
    }

    @Test
    public void testHintRejectsPkTable() {
        // PK tables are out of scope for stage-1 CHANGES (hint or clause).
        String sql = "SELECT * FROM pk_t [_CHANGES_1_2_]";
        AnalysisException ex = assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        assertTrue(ex.getMessage().contains("CHANGES on primary-key table is not supported yet"),
                "actual: " + ex.getMessage());
    }

    @Test
    public void testHintBaseGreaterThanHead() {
        // base must precede head in the bookmark range; otherwise the interval is empty / inverted.
        String sql = "SELECT * FROM dup_t [_CHANGES_9_3_]";
        AnalysisException ex = assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        assertTrue(ex.getMessage().contains("CHANGES hint base must not be later than head"),
                "actual: " + ex.getMessage());
    }

    @Test
    public void testHintConflictsWithMeta() {
        // _META_ flips the relation to a live index-metadata introspection view; CHANGES is
        // historical-row data — the two cannot coexist on the same TableRelation.
        String sql = "SELECT * FROM dup_t [_META_, _CHANGES_1_2_]";
        AnalysisException ex = assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        assertTrue(ex.getMessage().contains("CHANGES hint cannot combine with"),
                "actual: " + ex.getMessage());
    }

    @Test
    public void testHintConflictsWithBookmark() {
        // _BOOKMARK_ is a PITQ scope; CHANGES is an interval scan. Combining them would
        // require resolving two independent histories on one relation — reject explicitly.
        String sql = "SELECT * FROM dup_t [_CHANGES_1_2_, _BOOKMARK_5_]";
        AnalysisException ex = assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        assertTrue(ex.getMessage().contains("CHANGES hint cannot combine with"),
                "actual: " + ex.getMessage());
    }

    @Test
    public void testHintConflictsWithClause() throws Exception {
        // The grammar tableAtom permits both `changePeriod` and `bracketHint`, but the
        // changePeriod's `end=expression` greedily consumes a trailing `[...]` as a
        // collectionSubscript on the version literal — so SQL today never produces a
        // TableRelation with both fields set. The resolveTableRef guard is defense-in-
        // depth for a future grammar that separates the two; this test exercises it by
        // parsing a hint-only SQL (parser-only, no analyzer pass) and then mutating the
        // AST to also attach a changePeriod before invoking the analyzer.
        String sql = "SELECT * FROM dup_t [_CHANGES_3_4_]";
        QueryStatement stmt = (QueryStatement) UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                sql, connectContext);
        TableRelation tr = (TableRelation) ((SelectRelation) stmt.getQueryRelation()).getRelation();
        tr.setChangePeriod(new ChangePeriod(
                com.starrocks.sql.ast.QueryPeriod.PeriodType.VERSION,
                new IntLiteral(1L),
                Optional.of(new IntLiteral(2L)),
                /* isStats */ false,
                NodePosition.ZERO));
        SemanticException ex = assertThrows(SemanticException.class,
                () -> com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext));
        assertTrue(ex.getMessage().contains("CHANGES hint cannot combine with the CHANGES clause"),
                "actual: " + ex.getMessage());
    }

    /**
     * Static counterpart to BookmarkTestBase#createTable. Needed because @BeforeAll
     * runs in static context but the base helper is an instance method.
     */
    private static void createTableStatic(String ddl) throws Exception {
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(
                ddl, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
    }
}
