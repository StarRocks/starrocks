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

package com.starrocks.scheduler.mv.ivm;

import com.starrocks.alter.AlterMVJobExecutor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.scheduler.MVActiveChecker;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

public class IvmUnaliasedColumnNamesIcebergTest extends MVIVMIcebergTestBase {
    private static final String T0 = "`iceberg0`.`unpartitioned_db`.`t0`";

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVIVMIcebergTestBase.beforeClass();
        starRocksAssert.useDatabase("test");
    }

    static Stream<String> unaliasedQueries() {
        return Stream.of(
                "SELECT id, sum(id), count(*) FROM " + T0 + " GROUP BY id",
                "SELECT data, id + 1, count(data), sum(id * 2) FROM " + T0 + " GROUP BY data, id + 1",
                "SELECT id, data, id * 2 FROM " + T0,
                "SELECT *, id + 1 FROM " + T0,
                "SELECT id, id * 2 FROM " + T0 + " UNION ALL SELECT id, id * 3 FROM " + T0);
    }

    static Stream<String> pctQueries() {
        return Stream.of(
                "SELECT id, count(*), id * 2 FROM " + T0 + " GROUP BY id",
                "SELECT id, concat(data, '" + "x".repeat(1100) + "') FROM " + T0);
    }

    @ParameterizedTest(name = "[{index}]")
    @MethodSource("pctQueries")
    public void testPctColumnNamesSurviveActivation(String query) throws Exception {
        Assertions.assertTrue(RunMode.isSharedNothingMode());
        List<String> expectedNames = outputNames(query);
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`mv_pct_unaliased` REFRESH DEFERRED MANUAL "
                + "PROPERTIES ('refresh_mode' = 'pct') AS " + query);
        try {
            starRocksAssert.alterMvProperties("ALTER MATERIALIZED VIEW `test`.`mv_pct_unaliased` INACTIVE");
            starRocksAssert.alterMvProperties("ALTER MATERIALIZED VIEW `test`.`mv_pct_unaliased` ACTIVE");
            MaterializedView mv = getMv("mv_pct_unaliased");
            Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
            Assertions.assertEquals(expectedNames, visibleColumns(mv));
        } finally {
            starRocksAssert.dropMaterializedView("mv_pct_unaliased");
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unaliasedQueries")
    public void testUnaliasedColumnsKeepTheirNamesThroughActivationAndRefresh(String query) throws Exception {
        Assertions.assertTrue(RunMode.isSharedNothingMode());
        List<String> expectedNames = outputNames(query);
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`mv_unaliased` REFRESH DEFERRED MANUAL "
                + "PROPERTIES ('refresh_mode' = 'incremental') AS " + query);
        try {
            Assertions.assertEquals(expectedNames, visibleColumns(getMv("mv_unaliased")));

            starRocksAssert.alterMvProperties("ALTER MATERIALIZED VIEW `test`.`mv_unaliased` INACTIVE");
            starRocksAssert.alterMvProperties("ALTER MATERIALIZED VIEW `test`.`mv_unaliased` ACTIVE");
            MaterializedView mv = getMv("mv_unaliased");
            Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
            Assertions.assertEquals(expectedNames, visibleColumns(mv));

            seedTvrBaselineAtVersionZero(mv);
            Assertions.assertNotNull(getIVMRefreshedExecPlan(mv));

            AlterMVJobExecutor.inactiveMvAndLog(mv, MaterializedViewExceptions.inactiveReasonForBaseTableChanged("t0"));
            MVActiveChecker.tryToActivate(mv, false);
            Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
            Assertions.assertEquals(expectedNames, visibleColumns(mv));
        } finally {
            starRocksAssert.dropMaterializedView("mv_unaliased");
        }
    }

    private static List<String> outputNames(String query) {
        QueryStatement statement = (QueryStatement) SqlParser.parse(query, connectContext.getSessionVariable()).get(0);
        Analyzer.analyze(statement, connectContext);
        return statement.getQueryRelation().getRelationFields().getAllFields().stream().map(Field::getName).toList();
    }

    private static List<String> visibleColumns(MaterializedView mv) {
        return mv.getBaseSchema().stream().filter(column -> !column.isHidden()).map(Column::getName).toList();
    }
}
