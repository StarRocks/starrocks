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
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Grammar and analyzer-stage test for the [_CHANGES_&lt;base&gt;_&lt;head&gt;_] hint
 * on cloud-native OlapTable.
 *
 * <p>Relies on BookmarkTestBase's SHARED_DATA mini-cluster so the cloud-native
 * guard in QueryAnalyzer never fires on the DUP / PK tables created here — we
 * want the regex / PK / conflict guard to be the thing that throws.
 *
 * <p>UtFrameUtils.parseStmtWithNewParser rewraps both ParsingException and
 * SemanticException as AnalysisException, so every test here asserts on
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
    public void testHintRejectsNonOlapTable() {
        // information_schema.tables is a built-in SchemaTable (TableType.SCHEMA),
        // not an OlapTable — the hint path's cloud-native-OlapTable guard fires
        // before any analyzer-stage CHANGES code runs.
        String sql = "SELECT * FROM information_schema.tables [_CHANGES_1_2_]";
        AnalysisException ex = assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        assertTrue(ex.getMessage().contains("CHANGES hint is only supported on cloud-native OlapTable"),
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

    /**
     * Static counterpart to BookmarkTestBase.createTable, needed because
     * {@code @BeforeAll} runs in static context but the base helper is an
     * instance method.
     */
    private static void createTableStatic(String ddl) throws Exception {
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(
                ddl, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt);
    }
}
