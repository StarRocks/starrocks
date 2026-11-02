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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmOpUtils;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The hidden columns an incremental mv adds for itself ({@code __ROW_ID__}, {@code __AGG_STATE_*}) stay out of
 * what users list and write: DESC, SHOW COLUMNS, SHOW CREATE, and the column list of CREATE MATERIALIZED VIEW.
 */
public class IvmHiddenColumnVisibilityTest {
    private static final String DB = "ivm_hidden_cols";
    private static final String OTHER_DB = "ivm_hidden_cols_other";
    private static final String BASE_TABLE_DDL = "CREATE TABLE t (k INT NOT NULL, v BIGINT) DUPLICATE KEY(k) "
            + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')";
    private static final String QUERY = "SELECT k, SUM(v) AS s, AVG(v) AS a FROM t GROUP BY k";

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(OTHER_DB).useDatabase(OTHER_DB).withTable(BASE_TABLE_DDL);
        starRocksAssert.withDatabase(DB).useDatabase(DB).withTable(BASE_TABLE_DDL);
    }

    @AfterEach
    public void dropMvs() throws Exception {
        starRocksAssert.useDatabase(DB);
        for (String db : List.of(DB, OTHER_DB)) {
            for (String mv : List.of("mv", "mv_copy")) {
                starRocksAssert.ddl("DROP MATERIALIZED VIEW IF EXISTS " + db + "." + mv);
            }
        }
    }

    private static String mvDdl(String name, String columnList, String refreshMode, String query) {
        return "CREATE MATERIALIZED VIEW " + name + " " + columnList + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES ('refresh_mode' = '" + refreshMode + "') AS " + query;
    }

    private static MaterializedView getMv(String db, String name) {
        return (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(db).getTable(name);
    }

    private static List<String> column(List<List<String>> rows, int index) {
        return rows.stream().map(row -> row.get(index)).collect(Collectors.toList());
    }

    private static List<String> visibleColumnNames(MaterializedView mv) {
        return mv.getBaseSchema().stream().filter(Column::isVisible).map(Column::getName).collect(Collectors.toList());
    }

    private static String schemaFingerprint(MaterializedView mv) {
        return mv.getBaseSchema().stream()
                .map(col -> String.format("%s|%s|key=%s|hidden=%s|null=%s|comment=%s", col.getName(),
                        col.getType().toSql(), col.isKey(), col.isHidden(), col.isAllowNull(), col.getComment()))
                .collect(Collectors.joining("\n"));
    }

    private static String columnList(String ddl) {
        return ddl.substring(ddl.indexOf('('), ddl.indexOf(")\n") + 1);
    }

    private static void assertIncrementallyMaintained(MaterializedView mv, String refreshMode) {
        assertEquals(refreshMode, mv.getCurrentRefreshMode().name().toLowerCase(), mv.getCurrentRefreshModeReason());
        assertEquals(RowIdStrategy.QUERY_COMPUTED, mv.getRowIdStrategy());
    }

    @ParameterizedTest
    @ValueSource(strings = {"incremental", "auto"})
    public void testDescAndShowColumnsListOnlyQueryColumns(String refreshMode) throws Exception {
        starRocksAssert.withMaterializedView(mvDdl("mv", "", refreshMode, QUERY));
        assertIncrementallyMaintained(getMv(DB, "mv"), refreshMode);

        List<String> queryColumns = List.of("k", "s", "a");
        assertEquals(queryColumns, column(starRocksAssert.show("DESC mv"), 0));
        assertEquals(queryColumns, column(starRocksAssert.show("SHOW COLUMNS FROM mv"), 0));
        assertEquals(queryColumns, column(starRocksAssert.show("SHOW FULL COLUMNS FROM mv"), 0));

        List<String> physicalColumns = column(starRocksAssert.show("DESC mv ALL"), 2);
        assertTrue(physicalColumns.contains(IvmOpUtils.COLUMN_ROW_ID), physicalColumns.toString());
        assertTrue(physicalColumns.contains("__AGG_STATE_avg(ivm_hidden_cols.t.v)"), physicalColumns.toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"incremental", "auto"})
    public void testShowCreateOutputRunsInAnotherDatabase(String refreshMode) throws Exception {
        starRocksAssert.withMaterializedView(mvDdl("mv", "", refreshMode, QUERY));
        String showCreate = starRocksAssert.show("SHOW CREATE MATERIALIZED VIEW mv").get(0).get(1);
        assertEquals("(`k`, `s`, `a`)", columnList(showCreate));

        String copy = showCreate.replace("`mv`", "`mv_copy`");
        starRocksAssert.withMaterializedView(copy);
        assertEquals(visibleColumnNames(getMv(DB, "mv")), visibleColumnNames(getMv(DB, "mv_copy")));

        starRocksAssert.useDatabase(OTHER_DB);
        starRocksAssert.withMaterializedView(copy);
        MaterializedView other = getMv(OTHER_DB, "mv_copy");
        assertIncrementallyMaintained(other, refreshMode);
        assertEquals(List.of("k", "s", "a"), visibleColumnNames(other));
        assertTrue(other.getColumn("__AGG_STATE_sum(ivm_hidden_cols_other.t.v)").isHidden());
    }

    @ParameterizedTest
    @ValueSource(strings = {"incremental", "auto"})
    public void testColumnListNamesOnlyQueryColumns(String refreshMode) throws Exception {
        starRocksAssert.withMaterializedView(mvDdl("mv", "(kk, ss COMMENT 'total') ", refreshMode,
                "SELECT k, SUM(v) FROM t GROUP BY k"));
        MaterializedView mv = getMv(DB, "mv");
        assertIncrementallyMaintained(mv, refreshMode);
        assertEquals(List.of("kk", "ss"), visibleColumnNames(mv));
        assertEquals("total", mv.getColumn("ss").getComment());
        assertTrue(mv.getColumn("__AGG_STATE_sum(ivm_hidden_cols.t.v)").isHidden());

        // The refresh re-derives the hidden columns by name and position.
        IvmRefreshDefinition.derive(connectContext, mv);

        String before = schemaFingerprint(mv);
        starRocksAssert.ddl("ALTER MATERIALIZED VIEW mv INACTIVE");
        starRocksAssert.ddl("ALTER MATERIALIZED VIEW mv ACTIVE");
        assertTrue(mv.isActive(), mv.getInactiveReason());
        assertEquals(before, schemaFingerprint(mv));
    }

    @Test
    public void testColumnListWithHiddenColumnsStillAccepted() throws Exception {
        starRocksAssert.withMaterializedView(mvDdl("mv", "(`__ROW_ID__`, kk, ss, `__AGG_STATE_sum(ivm_hidden_cols.t.v)`) ",
                "incremental", "SELECT k, SUM(v) FROM t GROUP BY k"));
        assertEquals(List.of("kk", "ss"), visibleColumnNames(getMv(DB, "mv")));
    }

    @Test
    public void testColumnListOfAnotherLengthRejected() {
        Exception e = assertThrows(Exception.class, () -> starRocksAssert.withMaterializedView(
                mvDdl("mv", "(kk, ss, extra) ", "incremental", "SELECT k, SUM(v) FROM t GROUP BY k")));
        assertTrue(e.getMessage().contains("different column counts"), e.getMessage());
    }
}
