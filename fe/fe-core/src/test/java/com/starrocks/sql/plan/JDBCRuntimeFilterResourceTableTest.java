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
package com.starrocks.sql.plan;

import com.starrocks.catalog.JDBCTable;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The PostgreSQL exemptions in {@code JDBCScanNode.canCarryRuntimeFilter} are properties of
 * {@code PostgresSchemaResolver}, so they hold only where that resolver produced the schema. A
 * legacy resource-backed external table's schema is written by hand: the protocol still reads
 * POSTGRES from the URI and the source-type map is empty, so without an explicit check a VARCHAR
 * column the user declared over a {@code timestamptz}, {@code uuid} or {@code numeric} would be
 * waved through and the remote query would fail with {@code operator does not exist}.
 *
 * <p>{@code PushDownTopNToJDBCScanRule} and {@code PushDownJoinToJDBCRule} already exclude these
 * tables for the same reason; this pins the runtime filter gate to the same rule.
 */
public class JDBCRuntimeFilterResourceTableTest extends ConnectorPlanTestBase {
    private static final String RESOURCE_TABLE = "test.jdbc_rf_pg_resource_table";
    private static final String CATALOG_TABLE = "jdbc_postgres.partitioned_db0.tbl0";
    private boolean previousRuntimeFilterPushDown;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        boolean runningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        try {
            starRocksAssert.withResource("CREATE EXTERNAL RESOURCE jdbc_rf_pg_resource PROPERTIES ("
                            + "\"type\"=\"jdbc\", \"user\"=\"test\", \"password\"=\"test\", "
                            + "\"driver_url\"=\"test_driver_url\", \"driver_class\"=\"org.postgresql.Driver\", "
                            + "\"jdbc_uri\"=\"jdbc:postgresql://127.0.0.1:5432/testdb\")")
                    // The hand-written schema the gate has to distrust: the remote column behind
                    // `k` may be a timestamptz or a uuid, and nothing here says which.
                    .withTable("CREATE EXTERNAL TABLE " + RESOURCE_TABLE
                            + " (id BIGINT, k VARCHAR(64)) ENGINE=jdbc PROPERTIES ("
                            + "\"resource\"=\"jdbc_rf_pg_resource\", \"table\"=\"public.rf_probe\")");
        } finally {
            FeConstants.runningUnitTest = runningUnitTest;
        }
    }

    @BeforeEach
    public void setUp() {
        previousRuntimeFilterPushDown = connectContext.getSessionVariable().isEnableJdbcRuntimeFilterPushDown();
        connectContext.getSessionVariable().setEnableJdbcRuntimeFilterPushDown(true);
    }

    @AfterEach
    public void tearDown() {
        connectContext.getSessionVariable().setEnableJdbcRuntimeFilterPushDown(previousRuntimeFilterPushDown);
    }

    @Test
    public void testResourceBackedPostgresTableIsNotExemptFromTheTextProof() throws Exception {
        JDBCTable table = (JDBCTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable("test", "jdbc_rf_pg_resource_table");
        // The three conditions that together made this reachable: it reads as PostgreSQL, it is
        // resource-backed, and it carries no source-type names to fall back on.
        Assertions.assertEquals("jdbc_rf_pg_resource", table.getResourceName());
        Assertions.assertEquals(JDBCTable.ProtocolType.POSTGRES, table.getProtocolType());
        Assertions.assertTrue(table.getOriginalJdbcColumnTypeNames().isEmpty());

        // Only `k` is materialized, so the authorized-column count is exactly the verdict on the
        // VARCHAR column. Selecting the BIGINT id as well would report 1 either way -- integers are
        // authorized regardless -- and the test would pass without the gate.
        String plan = getVerboseExplain(
                "SELECT t.k FROM " + RESOURCE_TABLE + " t JOIN [broadcast] (VALUES ('a'),('b')) d(k) "
                        + "ON t.k = d.k");
        Assertions.assertFalse(plan.contains("RUNTIME FILTER PUSH DOWN"), plan);
    }

    @Test
    public void testResourceBackedIntegerKeyStillCarriesAFilter() throws Exception {
        // Integers need no property of the resolver to be bindable, so excluding resource tables
        // wholesale would give up more than the defect required. Only the exemptions are withdrawn.
        String plan = getVerboseExplain(
                "SELECT t.k FROM " + RESOURCE_TABLE + " t JOIN [broadcast] (VALUES (1),(2)) d(id) "
                        + "ON t.id = d.id");
        assertContains(plan, "RUNTIME FILTER PUSH DOWN: allowed on 1 column(s)");
    }

    @Test
    public void testCatalogBackedPostgresVarcharKeyStillCarriesAFilter() throws Exception {
        // The control: the same VARCHAR shape over a catalog table, where PostgresSchemaResolver
        // did produce the schema, must keep the exemption this change narrows.
        String plan = getVerboseExplain(
                "SELECT t.c FROM " + CATALOG_TABLE + " t JOIN [broadcast] (VALUES ('a'),('b')) d(k) "
                        + "ON t.a = d.k");
        assertContains(plan, "RUNTIME FILTER PUSH DOWN: allowed on");
    }
}
