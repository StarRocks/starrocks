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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.scheduler.mv.ivm.MVIVMIcebergTestBase;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.parser.SqlParser;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The CREATE-time gate on complex-typed row identity keys.
 *
 * <p>Both halves matter: a complex GROUP BY / DISTINCT key must be rejected, and every other use of a
 * complex column -- pass-through, aggregate argument, scalar expression over one -- must still be accepted.
 */
public class IvmComplexTypeRowIdKeyTest extends MVIVMIcebergTestBase {

    @Override
    public void advanceTableVersionTo(long toVersion) {
    }

    @ParameterizedTest(name = "reject: {0}")
    @CsvSource(delimiter = '|', value = {
            "array group key        | SELECT k, arr, SUM(v) FROM D.t_complex GROUP BY k, arr",
            "struct group key       | SELECT k, st, SUM(v) FROM D.t_complex GROUP BY k, st",
            "map group key          | SELECT k, mp, SUM(v) FROM D.t_complex GROUP BY k, mp",
            "array of struct        | SELECT k, arr_st, SUM(v) FROM D.t_complex GROUP BY k, arr_st",
            "struct of array        | SELECT k, st_arr, SUM(v) FROM D.t_complex GROUP BY k, st_arr",
            "distinct over array    | SELECT DISTINCT k, arr FROM D.t_complex",
            "group by only, no agg  | SELECT k, arr FROM D.t_complex GROUP BY k, arr",
    })
    public void testRejectsComplexRowIdKey(String shape, String query) {
        SemanticException ex = assertThrows(SemanticException.class, () -> runIvmAnalyzer(query),
                shape + " must be rejected at CREATE, not silently collapsed onto one row id");
        assertTrue(ex.getMessage().contains("row id"),
                "error should name the row id key: " + ex.getMessage());
    }

    /**
     * AUTO analyses the definition as INCREMENTAL and falls back to PCT when that is rejected, so the gate
     * turns a silently-collapsing incremental MV into a correct full-recompute one rather than a CREATE error.
     * The scalar arm is the control: without it, {@code getCurrentRefreshMode()} defaulting to PCT would make
     * the complex arm pass no matter what the gate did.
     */
    @ParameterizedTest(name = "AUTO: {0} -> {2}")
    @CsvSource(delimiter = '|', value = {
            "scalar group key | SELECT k, name, SUM(v) AS s FROM D.t_complex GROUP BY k, name | false",
            "array group key  | SELECT k, arr, SUM(v) AS s FROM D.t_complex GROUP BY k, arr   | true",
            "map group key    | SELECT k, mp, SUM(v) AS s FROM D.t_complex GROUP BY k, mp     | true",
    })
    public void testAutoDegradesToPctForComplexRowIdKey(String shape, String query, boolean expectPct) {
        // AUTO is internal-only: getRefreshMode rejects refresh_mode=auto before the gate is reached.
        new MockUp<IVMAnalyzer>() {
            @Mock
            public static MaterializedView.RefreshMode getRefreshMode(CreateMaterializedViewStatement statement) {
                return MaterializedView.RefreshMode.AUTO;
            }
        };
        CreateMaterializedViewStatement stmt =
                parseMvDdl("CREATE MATERIALIZED VIEW mv_auto REFRESH DEFERRED MANUAL AS " + resolveTable(query));
        Analyzer.analyze(stmt, connectContext);
        MaterializedView.RefreshMode mode = stmt.getCurrentRefreshMode();
        if (expectPct) {
            assertEquals(MaterializedView.RefreshMode.PCT, mode,
                    shape + " must fall back to PCT instead of maintaining a collapsed row id");
        } else {
            assertNotEquals(MaterializedView.RefreshMode.PCT, mode,
                    shape + " is the control: it must still be maintained incrementally");
        }
    }

    @ParameterizedTest(name = "accept: {0}")
    @CsvSource(delimiter = '|', value = {
            "scalar group key       | SELECT k, name, SUM(v) FROM D.t_complex GROUP BY k, name",
            "array pass-through     | SELECT k, arr FROM D.t_complex",
            "array_agg argument     | SELECT k, array_agg(arr) FROM D.t_complex GROUP BY k",
            "count of array         | SELECT k, COUNT(arr) FROM D.t_complex GROUP BY k",
            "array_length group key | SELECT array_length(arr) AS n, SUM(v) FROM D.t_complex GROUP BY n",
            "struct field group key | SELECT st.a AS a, SUM(v) FROM D.t_complex GROUP BY a",
    })
    public void testAcceptsComplexColumnsElsewhere(String shape, String query) throws Exception {
        runIvmAnalyzer(query);
    }

    private static void runIvmAnalyzer(String query) throws Exception {
        CreateMaterializedViewStatement stmt = parseMvDdl("CREATE MATERIALIZED VIEW mv_ct REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") AS " + resolveTable(query));
        QueryStatement qs = stmt.getQueryStatement();
        Analyzer.analyze(qs, connectContext);
        new IVMAnalyzer(connectContext, stmt, qs).rewrite(MaterializedView.RefreshMode.INCREMENTAL);
    }

    private static CreateMaterializedViewStatement parseMvDdl(String ddl) {
        return (CreateMaterializedViewStatement) SqlParser.parse(ddl,
                connectContext.getSessionVariable().getSqlMode()).get(0);
    }

    private static String resolveTable(String query) {
        return query.replace("D.", "`iceberg0`.`unpartitioned_db`.");
    }
}
