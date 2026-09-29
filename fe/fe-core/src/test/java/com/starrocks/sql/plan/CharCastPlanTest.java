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

import com.starrocks.planner.PlanFragment;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TPlanNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class CharCastPlanTest extends PlanTestNoneDBBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
        starRocksAssert.withDatabase("char_cast_test").useDatabase("char_cast_test");
        starRocksAssert.withTable("CREATE TABLE char_target (id INT, c CHAR(10)) PRIMARY KEY(id) " +
                "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE char_source (id INT, n BIGINT, s VARCHAR(64)) " +
                "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE char_part (s VARCHAR(64), id INT) " +
                "PARTITION BY LIST(s) (PARTITION p1 VALUES IN ('hello world'), PARTITION p2 VALUES IN ('hi')) " +
                "DISTRIBUTED BY HASH(s) BUCKETS 1 PROPERTIES ('replication_num'='1')");
    }

    private List<TExprNode> projectCasts(String sql) throws Exception {
        List<TExprNode> casts = new ArrayList<>();
        for (PlanFragment fragment : getExecPlan(sql).getFragments()) {
            for (TPlanNode node : fragment.getPlanRoot().treeToThrift().getNodes()) {
                if (node.isSetProject_node()) {
                    for (TExpr expr : node.getProject_node().getSlot_map().values()) {
                        expr.getNodes().stream().filter(n -> n.getNode_type() == TExprNodeType.CAST_EXPR)
                                .forEach(casts::add);
                    }
                }
            }
        }
        return casts;
    }

    @Test
    public void testInsertValuesPreservesImplicitAssignmentValue() throws Exception {
        String implicit = getFragmentPlan("INSERT INTO char_target VALUES (1, 1775580223839)");
        Assertions.assertTrue(implicit.contains("1775580223839"), implicit);
        String explicit = getFragmentPlan("INSERT INTO char_target VALUES (1, CAST(1775580223839 AS CHAR(10)))");
        Assertions.assertTrue(explicit.contains("1775580223"), explicit);
        Assertions.assertFalse(explicit.contains("1775580223839"), explicit);
    }

    @Test
    public void testInsertSelectAndUpdateOnlyTruncateExplicitCasts() throws Exception {
        for (String sql : List.of("INSERT INTO char_target SELECT id, n FROM char_source",
                "UPDATE char_target SET c = id + 1775580223838 WHERE id > 0")) {
            List<TExprNode> casts = projectCasts(sql);
            Assertions.assertFalse(casts.isEmpty(), sql);
            Assertions.assertTrue(casts.stream().noneMatch(TExprNode::isCast_char_truncate), sql);
        }
        String constantUpdate = getFragmentPlan("UPDATE char_target SET c = 1775580223839 WHERE id = 1");
        Assertions.assertTrue(constantUpdate.contains("1775580223839"), constantUpdate);
        List<TExprNode> explicit = projectCasts(
                "INSERT INTO char_target SELECT id, CAST(n AS CHAR(10)) FROM char_source");
        Assertions.assertTrue(explicit.stream().anyMatch(TExprNode::isCast_char_truncate));
    }

    @Test
    public void testNonconstantNestedVarcharCastKeepsCharTruncation() throws Exception {
        List<TExprNode> casts = projectCasts("SELECT CAST(CAST(n AS VARCHAR(10)) AS CHAR(10)) FROM char_source");
        Assertions.assertTrue(casts.stream().anyMatch(TExprNode::isCast_char_truncate));
    }

    @Test
    public void testListPartitionsAndExecutionPredicateAreRetained() throws Exception {
        for (String predicate : List.of("CAST(s AS CHAR(5)) = 'hello'", "CAST(s AS CHAR(5)) IN ('hello')",
                "CAST(s AS CHAR(5)) < 'hi'")) {
            String plan = getFragmentPlan("SELECT * FROM char_part WHERE " + predicate);
            Assertions.assertTrue(plan.contains("partitions=2/2"), plan);
            Assertions.assertTrue(plan.contains("PREDICATES:"), plan);
            Assertions.assertTrue(plan.contains("CHAR(5)"), plan);
        }
    }
}
