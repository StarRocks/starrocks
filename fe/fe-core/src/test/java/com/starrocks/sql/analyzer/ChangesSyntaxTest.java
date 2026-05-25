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

import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.lake.bookmark.BookmarkRange;
import com.starrocks.lake.bookmark.BookmarkTestBase;
import com.starrocks.lake.changes.ChangesMetaDescriptor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.thrift.TChangesMetaKind;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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

        // Partitioned variant for table-scope-hint conflict tests; PARTITION(p1)
        // only reaches validateChangesHint when the partition resolves.
        createTableStatic(
                "CREATE TABLE dup_partitioned (k int, v int) DUPLICATE KEY(k) "
                        + "PARTITION BY RANGE(k) (PARTITION p1 VALUES [('-2147483648'), ('10'))) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "PROPERTIES ('replication_num' = '1');");

        createTableStatic(
                "CREATE TABLE pk_t (k int, v int) "
                        + "PRIMARY KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "PROPERTIES ('replication_num' = '1');");

        createTableStatic(
                "CREATE TABLE unique_t (k int, v int) "
                        + "UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "PROPERTIES ('replication_num' = '1');");

        // Conflict-fixture tables: each carries a real column that shadows one
        // or both of the default CDC metadata names so the analyzer must mint
        // an alternate query name.
        createTableStatic(
                "CREATE TABLE dup_ct (k int, `__CHANGE_TYPE__` int) "
                        + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "PROPERTIES ('replication_num' = '1');");

        createTableStatic(
                "CREATE TABLE dup_rv (k int, `__ROW_VERSION__` int) "
                        + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "PROPERTIES ('replication_num' = '1');");

        createTableStatic(
                "CREATE TABLE dup_both (k int, `__CHANGE_TYPE__` int, `__ROW_VERSION__` int) "
                        + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "PROPERTIES ('replication_num' = '1');");

        createTableStatic(
                "CREATE TABLE dup_chain (k int, `__CHANGE_TYPE__` int, `__CHANGE_TYPE_1__` int) "
                        + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
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
    public void testHintRejectsPkAndUniqueTable() {
        String[] tableNames = {"pk_t", "unique_t"};
        for (String tableName : tableNames) {
            String sql = "SELECT * FROM " + tableName + " [_CHANGES_1_2_]";
            AnalysisException ex = assertThrows(AnalysisException.class,
                    () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
            assertTrue(ex.getMessage().contains("CHANGES hint is only supported on DUPLICATE / AGGREGATE table"),
                    "actual for " + tableName + ": " + ex.getMessage());
        }
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

    @Test
    public void testHintConflictsWithTableScopeHints() {
        // PARTITION / TABLET / REPLICA each restrict the OLAP scan range, but the
        // CHANGES branch ignores them; reject the combination instead of silently
        // scanning beyond the requested scope.
        String[] queries = {
                "SELECT * FROM dup_partitioned PARTITION(p1) [_CHANGES_1_2_]",
                "SELECT * FROM dup_t TABLET(123) [_CHANGES_1_2_]",
                "SELECT * FROM dup_t REPLICA(456) [_CHANGES_1_2_]",
        };
        for (String sql : queries) {
            AnalysisException ex = assertThrows(AnalysisException.class,
                    () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext),
                    "expected reject for: " + sql);
            assertTrue(ex.getMessage().contains(
                            "CHANGES hint cannot combine with PARTITION / TABLET / REPLICA hints"),
                    "actual: " + ex.getMessage() + " for: " + sql);
        }
    }

    @Test
    public void testDefaultMetadataNamesWhenNoConflict() throws Exception {
        // dup_t has no column shadowing the default names; both descriptors
        // should resolve to the defaults.
        String sql = "SELECT __CHANGE_TYPE__, __ROW_VERSION__ FROM dup_t [_CHANGES_1_2_]";
        TableRelation tr = analyzeChangesRelation(sql);
        List<ChangesMetaDescriptor> descriptors = tr.getChangesMetaDescriptors().orElseThrow();
        assertEquals(2, descriptors.size());
        assertEquals(TChangesMetaKind.CHANGE_TYPE, descriptors.get(0).kind());
        assertEquals("__CHANGE_TYPE__", descriptors.get(0).name());
        assertEquals(TChangesMetaKind.ROW_VERSION, descriptors.get(1).kind());
        assertEquals("__ROW_VERSION__", descriptors.get(1).name());
    }

    @Test
    public void testChangeTypeConflictUsesAlternateName() throws Exception {
        // SELECT real __CHANGE_TYPE__ resolves to the real table column (not the
        // CDC metadata). The CDC kind takes __CHANGE_TYPE_1__.
        String sql = "SELECT __CHANGE_TYPE__, __CHANGE_TYPE_1__, __ROW_VERSION__ "
                + "FROM dup_ct [_CHANGES_1_2_]";
        TableRelation tr = analyzeChangesRelation(sql);
        List<ChangesMetaDescriptor> descriptors = tr.getChangesMetaDescriptors().orElseThrow();
        assertEquals(2, descriptors.size());
        assertEquals("__CHANGE_TYPE_1__", descriptors.get(0).name());
        assertEquals("__ROW_VERSION__", descriptors.get(1).name());

        // Scope must contain both names — proves the real column survives and
        // the alternate metadata name is queryable.
        assertFieldPresent(tr, "__CHANGE_TYPE__");
        assertFieldPresent(tr, "__CHANGE_TYPE_1__");
        assertFieldPresent(tr, "__ROW_VERSION__");
    }

    @Test
    public void testRowVersionConflictUsesAlternateName() throws Exception {
        String sql = "SELECT __CHANGE_TYPE__, __ROW_VERSION__, __ROW_VERSION_1__ "
                + "FROM dup_rv [_CHANGES_1_2_]";
        TableRelation tr = analyzeChangesRelation(sql);
        List<ChangesMetaDescriptor> descriptors = tr.getChangesMetaDescriptors().orElseThrow();
        assertEquals("__CHANGE_TYPE__", descriptors.get(0).name());
        assertEquals("__ROW_VERSION_1__", descriptors.get(1).name());
    }

    @Test
    public void testBothMetadataNamesConflict() throws Exception {
        String sql = "SELECT __CHANGE_TYPE_1__, __ROW_VERSION_1__ FROM dup_both [_CHANGES_1_2_]";
        TableRelation tr = analyzeChangesRelation(sql);
        List<ChangesMetaDescriptor> descriptors = tr.getChangesMetaDescriptors().orElseThrow();
        assertEquals("__CHANGE_TYPE_1__", descriptors.get(0).name());
        assertEquals("__ROW_VERSION_1__", descriptors.get(1).name());
        assertFieldPresent(tr, "__CHANGE_TYPE__");
        assertFieldPresent(tr, "__ROW_VERSION__");
        assertFieldPresent(tr, "__CHANGE_TYPE_1__");
        assertFieldPresent(tr, "__ROW_VERSION_1__");
    }

    @Test
    public void testCandidateChainSkipsToNextSuffix() throws Exception {
        // Both __CHANGE_TYPE__ and __CHANGE_TYPE_1__ already exist as real
        // columns; the CHANGE_TYPE metadata kind must skip to _2_.
        String sql = "SELECT __CHANGE_TYPE_2__, __ROW_VERSION__ FROM dup_chain [_CHANGES_1_2_]";
        TableRelation tr = analyzeChangesRelation(sql);
        List<ChangesMetaDescriptor> descriptors = tr.getChangesMetaDescriptors().orElseThrow();
        assertEquals("__CHANGE_TYPE_2__", descriptors.get(0).name());
        assertEquals("__ROW_VERSION__", descriptors.get(1).name());
    }

    @Test
    public void testSelectStarIncludesAlternateMetadata() throws Exception {
        // SELECT * exposes the real shadowing column AND the alternate
        // metadata column — keeps the user's data accessible while still
        // surfacing CDC metadata.
        String sql = "SELECT * FROM dup_ct [_CHANGES_1_2_]";
        TableRelation tr = analyzeChangesRelation(sql);
        assertFieldPresent(tr, "__CHANGE_TYPE__");
        assertFieldPresent(tr, "__CHANGE_TYPE_1__");
        assertFieldPresent(tr, "__ROW_VERSION__");
    }

    private static TableRelation analyzeChangesRelation(String sql) throws Exception {
        QueryStatement stmt = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                sql, connectContext);
        return (TableRelation) ((SelectRelation) stmt.getQueryRelation()).getRelation();
    }

    private static void assertFieldPresent(TableRelation tr, String name) {
        for (Field f : tr.getScope().getRelationFields().getAllFields()) {
            if (f.getName().equalsIgnoreCase(name)) {
                return;
            }
        }
        fail("expected field '" + name + "' in relation scope");
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
