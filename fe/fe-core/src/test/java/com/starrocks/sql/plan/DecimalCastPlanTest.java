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
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TPlanNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class DecimalCastPlanTest extends PlanTestNoneDBBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
        starRocksAssert.withDatabase("decimal_cast_test").useDatabase("decimal_cast_test");
        starRocksAssert.withTable("CREATE TABLE decimal_target (id INT, d DECIMAL(5, 2)) PRIMARY KEY(id) " +
                "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE decimal_source (id INT, n BIGINT, d DECIMAL(18, 3)) " +
                "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE decimal_json (id INT, j JSON) " +
                "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE decimal_struct (id INT, s STRUCT<a DECIMAL(3, 0), b DECIMAL(5, 0)>) " +
                "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE decimal_struct_target (id INT, " +
                "s STRUCT<b DECIMAL(3, 0), a DECIMAL(5, 0)>) " +
                "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
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
    public void testInsertSelectKeepsValuesPastPrecisionForTheSink() throws Exception {
        // An INSERT adapts a value to the type of its column with an implicit cast, which keeps one with more digits
        // than the column's precision for the sink to report or filter
        for (String sql : List.of("INSERT INTO decimal_target SELECT id, n FROM decimal_source",
                "INSERT INTO decimal_target SELECT id, d FROM decimal_source")) {
            List<TExprNode> casts = projectCasts(sql);
            Assertions.assertFalse(casts.isEmpty(), sql);
            Assertions.assertTrue(casts.stream().noneMatch(TExprNode::isCast_decimal_check_precision), sql);
        }
        for (String sql : List.of("INSERT INTO decimal_target SELECT id, CAST(n AS DECIMAL(5, 2)) FROM decimal_source",
                "INSERT INTO decimal_target SELECT id, CAST(d AS DECIMAL(5, 2)) FROM decimal_source")) {
            List<TExprNode> casts = projectCasts(sql);
            Assertions.assertTrue(casts.stream().anyMatch(TExprNode::isCast_decimal_check_precision), sql);
        }
    }

    @Test
    public void testJsonSubfieldCastKeepsItsImplicitness() throws Exception {
        // The prune-subfield rule rewrites CAST(json_query(...) AS DECIMAL) to a cast of get_json_double (without
        // the JSON path rewrite, which reads j.a as a column instead)
        SessionVariable sv = connectContext.getSessionVariable();
        boolean pruneSubfield = sv.isCboPruneSubfield();
        boolean pruneJsonSubfield = sv.isCboPruneJsonSubfield();
        boolean pruneComplexTypes = sv.getEnablePruneComplexTypes();
        boolean jsonV2Rewrite = sv.isEnableJSONV2Rewrite();
        try {
            sv.setCboPruneSubfield(true);
            sv.setCboPruneJsonSubfield(true);
            sv.setEnablePruneComplexTypes(false);
            sv.setEnableJSONV2Rewrite(false);
            String implicit = "INSERT INTO decimal_target SELECT id, j->'a' FROM decimal_json";
            assertContains(getFragmentPlan(implicit), "get_json_double");
            List<TExprNode> casts = projectCasts(implicit);
            Assertions.assertFalse(casts.isEmpty(), implicit);
            Assertions.assertTrue(casts.stream().noneMatch(TExprNode::isCast_decimal_check_precision), implicit);

            String explicit = "INSERT INTO decimal_target SELECT id, CAST(j->'a' AS DECIMAL(5, 2)) FROM decimal_json";
            assertContains(getFragmentPlan(explicit), "get_json_double");
            Assertions.assertTrue(projectCasts(explicit).stream().anyMatch(TExprNode::isCast_decimal_check_precision),
                    explicit);
        } finally {
            sv.setCboPruneSubfield(pruneSubfield);
            sv.setCboPruneJsonSubfield(pruneJsonSubfield);
            sv.setEnablePruneComplexTypes(pruneComplexTypes);
            sv.setEnableJSONV2Rewrite(jsonV2Rewrite);
        }
    }

    @Test
    public void testStructCastByNameChecksPrecision() throws Exception {
        // By position the fields fit (DECIMAL(3, 0) to DECIMAL(3, 0), DECIMAL(5, 0) to DECIMAL(5, 0)), but by name
        // b goes from DECIMAL(5, 0) to DECIMAL(3, 0)
        String sql = "INSERT INTO decimal_struct_target SELECT id, " +
                "CAST(s AS STRUCT<b DECIMAL(3, 0), a DECIMAL(5, 0)>) FROM decimal_struct";
        SessionVariable sv = connectContext.getSessionVariable();
        long sqlMode = sv.getSqlMode();
        try {
            sv.setSqlMode(sqlMode | SqlModeHelper.MODE_STRUCT_CAST_BY_NAME);
            List<TExprNode> casts = projectCasts(sql);
            Assertions.assertTrue(casts.stream().anyMatch(n -> n.isCast_struct_by_name()
                    && n.isCast_decimal_check_precision()), sql);

            sv.setSqlMode(sqlMode & ~SqlModeHelper.MODE_STRUCT_CAST_BY_NAME);
            casts = projectCasts(sql);
            Assertions.assertFalse(casts.isEmpty(), sql);
            Assertions.assertTrue(casts.stream().noneMatch(TExprNode::isCast_decimal_check_precision), sql);
        } finally {
            sv.setSqlMode(sqlMode);
        }
    }
}
