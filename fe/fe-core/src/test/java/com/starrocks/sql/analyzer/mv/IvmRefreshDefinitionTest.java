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
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IvmRefreshDefinitionTest extends MVIVMIcebergTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVIVMIcebergTestBase.beforeClass();
    }

    @Override
    public void advanceTableVersionTo(long toVersion) {
        // refresh is never run here; re-derivation only parses/analyzes the stored query.
    }

    @Test
    public void testDeriveProducesRewrittenSelect() throws Exception {
        String ddl = "CREATE MATERIALIZED VIEW mv_derive "
                + "REFRESH DEFERRED MANUAL "
                + "PROPERTIES (\"refresh_mode\" = \"incremental\") "
                + "AS SELECT id, count(data) AS cnt FROM `iceberg0`.`unpartitioned_db`.`t0` GROUP BY id";
        starRocksAssert.withMaterializedView(ddl, () -> {
            MaterializedView mv = getMv("test", "mv_derive");
            String sql = IvmRefreshDefinition.derive(connectContext, mv);

            assertNotNull(sql);
            assertTrue(sql.contains("from_binary"),
                    "re-derived maintenance SELECT must encode __ROW_ID__ via from_binary, got: " + sql);
            assertTrue(sql.contains("count_combine") || sql.contains("__AGG_STATE"),
                    "re-derived maintenance SELECT must project an agg-state column, got: " + sql);
        });
    }

    @Test
    public void testStrategyGateThrowsOnMismatch() {
        // DISTINCT query re-derives to QUERY_COMPUTED; the stored strategy is forced to
        // AUTO_INCREMENT so the gate in deriveRewrittenQuery must reject the mismatch.
        String distinctSql = "SELECT DISTINCT id FROM `iceberg0`.`unpartitioned_db`.`t0`";
        MaterializedView mv = Mockito.mock(MaterializedView.class);
        Mockito.when(mv.getName()).thenReturn("mv_strategy_drift");
        Mockito.when(mv.getViewDefineSql()).thenReturn(distinctSql);
        Mockito.when(mv.getCurrentRefreshMode()).thenReturn(MaterializedView.RefreshMode.INCREMENTAL);
        Mockito.when(mv.getEncodeRowIdVersion()).thenReturn(0);
        Mockito.when(mv.getRowIdStrategy()).thenReturn(RowIdStrategy.AUTO_INCREMENT);
        Mockito.when(mv.getIvmDefineSql()).thenReturn(null);

        SemanticException ex = assertThrows(SemanticException.class,
                () -> IvmRefreshDefinition.derive(connectContext, mv),
                "strategy mismatch (stored AUTO_INCREMENT vs re-derived QUERY_COMPUTED) must throw");
        String msg = ex.getMessage().toLowerCase();
        assertTrue(msg.contains("strategy") || msg.contains("drop"),
                "error must mention the strategy gate / drop-recreate, got: " + ex.getMessage());
    }
}
