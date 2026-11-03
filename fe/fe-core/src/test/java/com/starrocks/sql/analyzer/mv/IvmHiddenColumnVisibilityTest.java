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
import com.starrocks.scheduler.mv.ivm.MVIVMIcebergTestBase;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmOpUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The hidden columns an incremental mv adds for itself ({@code __ROW_ID__}, {@code __AGG_STATE_*}) stay out of
 * what users list and write: DESC, SHOW COLUMNS, SHOW CREATE, and the column list of CREATE MATERIALIZED VIEW.
 */
public class IvmHiddenColumnVisibilityTest extends MVIVMIcebergTestBase {
    private static final String BASE_TABLE = "`iceberg0`.`unpartitioned_db`.`t_numeric`";
    private static final String QUERY = "SELECT id, SUM(c1) AS s, AVG(c2) AS a FROM " + BASE_TABLE + " GROUP BY id";
    private static final String SUM_STATE_COLUMN = "__AGG_STATE_sum(iceberg0.unpartitioned_db.t_numeric.c1)";

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVIVMIcebergTestBase.beforeClass();
    }

    @Override
    public void advanceTableVersionTo(long toVersion) {
    }

    @AfterEach
    public void dropMvs() throws Exception {
        starRocksAssert.useDatabase("test");
        for (String mv : List.of("mv", "mv_copy")) {
            starRocksAssert.ddl("DROP MATERIALIZED VIEW IF EXISTS test." + mv);
        }
    }

    private static String mvDdl(String name, String columnList, String query) {
        return "CREATE MATERIALIZED VIEW " + name + " " + columnList + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES ('refresh_mode' = 'incremental') AS " + query;
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

    @Test
    public void testDescAndShowColumnsListOnlyQueryColumns() throws Exception {
        starRocksAssert.withMaterializedView(mvDdl("mv", "", QUERY));
        assertEquals(RowIdStrategy.QUERY_COMPUTED, getMv("test", "mv").getRowIdStrategy());

        List<String> queryColumns = List.of("id", "s", "a");
        assertEquals(queryColumns, column(starRocksAssert.show("DESC mv"), 0));
        assertEquals(queryColumns, column(starRocksAssert.show("SHOW COLUMNS FROM mv"), 0));
        assertEquals(queryColumns, column(starRocksAssert.show("SHOW FULL COLUMNS FROM mv"), 0));

        List<String> physicalColumns = column(starRocksAssert.show("DESC mv ALL"), 2);
        assertTrue(physicalColumns.contains(IvmOpUtils.COLUMN_ROW_ID), physicalColumns.toString());
        assertTrue(physicalColumns.contains(SUM_STATE_COLUMN), physicalColumns.toString());
    }

    @Test
    public void testShowCreateOutputCreatesTheSameMv() throws Exception {
        starRocksAssert.withMaterializedView(mvDdl("mv", "", QUERY));
        String showCreate = starRocksAssert.show("SHOW CREATE MATERIALIZED VIEW mv").get(0).get(1);
        assertEquals("(`id`, `s`, `a`)", columnList(showCreate));

        starRocksAssert.withMaterializedView(showCreate.replace("`mv`", "`mv_copy`"));
        MaterializedView copy = getMv("test", "mv_copy");
        assertEquals(RowIdStrategy.QUERY_COMPUTED, copy.getRowIdStrategy());
        assertEquals(schemaFingerprint(getMv("test", "mv")), schemaFingerprint(copy));
    }

    @Test
    public void testColumnListNamesOnlyQueryColumns() throws Exception {
        starRocksAssert.withMaterializedView(mvDdl("mv", "(kk, ss COMMENT 'total') ",
                "SELECT id, SUM(c1) FROM " + BASE_TABLE + " GROUP BY id"));
        MaterializedView mv = getMv("test", "mv");
        assertEquals(RowIdStrategy.QUERY_COMPUTED, mv.getRowIdStrategy());
        assertEquals(List.of("kk", "ss"), visibleColumnNames(mv));
        assertEquals("total", mv.getColumn("ss").getComment());
        assertTrue(mv.getColumn(SUM_STATE_COLUMN).isHidden());

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
        starRocksAssert.withMaterializedView(mvDdl("mv", "(`__ROW_ID__`, kk, ss, `" + SUM_STATE_COLUMN + "`) ",
                "SELECT id, SUM(c1) FROM " + BASE_TABLE + " GROUP BY id"));
        assertEquals(List.of("kk", "ss"), visibleColumnNames(getMv("test", "mv")));
    }

    @Test
    public void testColumnListOfAnotherLengthRejected() {
        Exception e = assertThrows(Exception.class, () -> starRocksAssert.withMaterializedView(
                mvDdl("mv", "(kk, ss, extra) ", "SELECT id, SUM(c1) FROM " + BASE_TABLE + " GROUP BY id")));
        assertTrue(e.getMessage().contains("different column counts"), e.getMessage());
    }
}
