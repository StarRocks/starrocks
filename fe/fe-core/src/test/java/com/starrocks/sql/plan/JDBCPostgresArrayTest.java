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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class JDBCPostgresArrayTest extends ConnectorPlanTestBase {
    private static final String TABLE = "jdbc_postgres.partitioned_db0.pg_array_probe";
    private boolean previousJoinPushdown;
    private boolean previousAggregatePushdown;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        JDBCTable table = (JDBCTable) GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(connectContext,
                "jdbc_postgres", "partitioned_db0", "pg_array_probe");
        table.setNewFullSchema(List.of(new Column("id", IntegerType.INT),
                new Column("items", new ArrayType(VarcharType.VARCHAR))));
    }

    @BeforeEach
    public void enablePushdown() {
        previousJoinPushdown = connectContext.getSessionVariable().isEnableJdbcJoinPushDown();
        previousAggregatePushdown = connectContext.getSessionVariable().isEnableJdbcAggPushDown();
        connectContext.getSessionVariable().setEnableJdbcJoinPushDown(true);
        connectContext.getSessionVariable().setEnableJdbcAggPushDown(true);
    }

    @AfterEach
    public void restorePushdown() {
        connectContext.getSessionVariable().setEnableJdbcJoinPushDown(previousJoinPushdown);
        connectContext.getSessionVariable().setEnableJdbcAggPushDown(previousAggregatePushdown);
    }

    private String remoteQueries(String plan) {
        return plan.lines().filter(line -> line.contains("QUERY:")).collect(Collectors.joining("\n"));
    }

    @Test
    public void testArraySubscriptAndPredicateStayLocal() throws Exception {
        String plan = getFragmentPlan("SELECT id, items[1] FROM " + TABLE + " WHERE items[1] = 'abc'");
        Assertions.assertFalse(remoteQueries(plan).contains("WHERE"), plan);
        Assertions.assertTrue(remoteQueries(plan).contains("items"), plan);
    }

    @Test
    public void testScalarFilterStillPushesWithArrayOutput() throws Exception {
        String plan = getFragmentPlan("SELECT items FROM " + TABLE + " WHERE id = 1");
        Assertions.assertTrue(remoteQueries(plan).contains("WHERE"), plan);
        Assertions.assertTrue(remoteQueries(plan).contains("items"), plan);
    }

    @Test
    public void testArrayGroupingAndDistinctStayLocal() throws Exception {
        for (String query : List.of("SELECT items, COUNT(*) FROM %s GROUP BY items",
                "SELECT DISTINCT items FROM %s", "SELECT COUNT(DISTINCT items) FROM %s")) {
            String plan = getFragmentPlan(String.format(query, TABLE));
            String remote = remoteQueries(plan).toUpperCase();
            Assertions.assertFalse(remote.contains("GROUP BY"), plan);
            Assertions.assertFalse(remote.contains("COUNT("), plan);
            Assertions.assertFalse(remote.contains("DISTINCT"), plan);
        }
    }

    @Test
    public void testArrayJoinEqualityStaysLocal() throws Exception {
        String plan = getFragmentPlan("SELECT a.id FROM " + TABLE + " a JOIN " + TABLE + " b ON a.items = b.items");
        Assertions.assertFalse(remoteQueries(plan).toUpperCase().contains(" JOIN "), plan);
    }

    @Test
    public void testScalarAggregationStillPushes() throws Exception {
        String plan = getFragmentPlan("SELECT id, COUNT(*) FROM " + TABLE + " GROUP BY id");
        Assertions.assertTrue(remoteQueries(plan).contains("GROUP BY"), plan);
    }
}
