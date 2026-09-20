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
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class JDBCUnboundedNumericTest extends ConnectorPlanTestBase {
    private JDBCTable table;
    private List<Column> originalSchema;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
    }

    @BeforeEach
    public void before() {
        table = (JDBCTable) GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(
                connectContext, "jdbc_postgres", "partitioned_db0", "tbl0");
        originalSchema = List.copyOf(table.getFullSchema());
        table.setNewFullSchema(List.of(new Column("id", IntegerType.INT),
                new Column("amount", TypeFactory.createUnifiedDecimalType(38, 18))));
        table.setUnboundedNumericColumns(Set.of("amount"));
        connectContext.getSessionVariable().setEnableJdbcAggPushDown(true);
        connectContext.getSessionVariable().setEnableJdbcProjectPushDown(true);
        connectContext.getSessionVariable().setEnableJdbcJoinPushDown(true);
    }

    @AfterEach
    public void after() {
        table.setNewFullSchema(originalSchema);
        table.setUnboundedNumericColumns(Set.of());
        connectContext.getSessionVariable().setEnableJdbcAggPushDown(true);
        connectContext.getSessionVariable().setEnableJdbcProjectPushDown(true);
        connectContext.getSessionVariable().setEnableJdbcJoinPushDown(false);
    }

    private void assertReadBeforeEvaluation(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("JDBC"), plan);
        for (String line : plan.split("\n")) {
            if (line.contains("QUERY:")) {
                Assertions.assertFalse(line.contains("WHERE"), line);
                Assertions.assertFalse(line.contains("GROUP BY"), line);
                Assertions.assertFalse(line.contains("sum("), line);
                Assertions.assertFalse(line.contains("avg("), line);
                Assertions.assertFalse(line.contains(" + "), line);
                Assertions.assertFalse(line.contains("JOIN"), line);
            }
        }
    }

    @Test
    public void testPredicateAndProjectionStayLocal() throws Exception {
        assertReadBeforeEvaluation("select amount + 1 from jdbc_postgres.partitioned_db0.tbl0 where amount > 0");
    }

    @Test
    public void testAggregateAndGroupingStayLocal() throws Exception {
        assertReadBeforeEvaluation("select sum(amount), avg(amount), min(amount), max(amount) "
                + "from jdbc_postgres.partitioned_db0.tbl0");
        assertReadBeforeEvaluation("select amount, count(*) from jdbc_postgres.partitioned_db0.tbl0 group by amount");
    }

    @Test
    public void testJoinStaysLocal() throws Exception {
        assertReadBeforeEvaluation("select a.amount from jdbc_postgres.partitioned_db0.tbl0 a "
                + "join jdbc_postgres.partitioned_db0.tbl0 b on a.amount=b.amount");
    }

    @Test
    public void testTopNStaysLocal() throws Exception {
        // A pushed ORDER BY/LIMIT decides which rows are read, so a value the strict read would
        // reject could fall outside the limit and never be seen. Keep the TopN local, and keep
        // the scan free of the remote ORDER BY that would have selected the rows.
        for (String sql : new String[] {
                "select id from jdbc_postgres.partitioned_db0.tbl0 order by amount limit 10",
                "select id, amount from jdbc_postgres.partitioned_db0.tbl0 order by id limit 10"}) {
            String plan = getFragmentPlan(sql);
            assertContains(plan, "TOP-N");
            Assertions.assertFalse(plan.contains("ORDER BY"), plan);
        }
    }

    @Test
    public void testTopNOverAnUnrelatedColumnStillPushes() throws Exception {
        table.setUnboundedNumericColumns(Set.of());
        assertContains(getFragmentPlan("select id from jdbc_postgres.partitioned_db0.tbl0 order by id limit 10"),
                "ORDER BY \"id\" ASC NULLS FIRST LIMIT 10");
    }

    @Test
    public void testUnusedUnboundedColumnDoesNotBlockOtherColumns() throws Exception {
        String plan = getFragmentPlan("select sum(id) from jdbc_postgres.partitioned_db0.tbl0");
        Assertions.assertTrue(plan.contains("sum(\"id\")"), plan);
    }

    @Test
    public void testExplicitPrecisionKeepsExistingPushdown() throws Exception {
        table.setUnboundedNumericColumns(Set.of());
        String plan = getFragmentPlan("select sum(amount) from jdbc_postgres.partitioned_db0.tbl0");
        Assertions.assertTrue(plan.contains("sum(\"amount\")"), plan);
    }
}
