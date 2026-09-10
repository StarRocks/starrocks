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

package com.starrocks.sql.analyzer;

import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getConnectContext;

public class AnalyzeSetOperationTest {

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testColumnNumberUnequal() {
        analyzeSuccess("select v1,v2,v3 from t0 union select v4,v5,v6 from t1");

        analyzeFail("select v1,v2 from t0 union select v4,v5,v6 from t1");
        analyzeFail("select v1,v2 from t0 union all select v4,v5,v6 from t1");
        analyzeFail("select v1,v2 from t0 except select v4,v5,v6 from t1");
        analyzeFail("select v1,v2 from t0 intersect select v4,v5,v6 from t1");
        analyzeFail("select v1,v2 from t0 union select v5,v6 from t1 union select v7,v8,v9 from t2");
    }

    @Test
    public void testQualifier() {
        analyzeSuccess("select v1,v2,v3 from t0 union all select v4,v5,v6 from t1");
        analyzeSuccess("select v1,v2,v3 from t0 union distinct select v4,v5,v6 from t1");

        analyzeFail("select v1,v2,v3 from t0 except all select v4,v5,v6 from t1");
        analyzeSuccess("select v1,v2,v3 from t0 except distinct select v4,v5,v6 from t1");

        analyzeFail("select v1,v2,v3 from t0 intersect all select v4,v5,v6 from t1");
        analyzeSuccess("select v1,v2,v3 from t0 intersect distinct select v4,v5,v6 from t1");
    }

    @Test
    public void testOutput() {
        analyzeSuccess("select b1 from test_object union all select b1 from test_object");
        analyzeFail("select b1 from test_object union select b1 from test_object",
                "not support set operation");
        analyzeFail("select b1 from test_object except select b1 from test_object",
                "not support set operation");
        analyzeFail("select b1 from test_object intersect select b1 from test_object",
                "not support set operation");
    }

    @Test
    public void testValues() {
        analyzeFail("(SELECT 1 AS c1, 2 AS c2) UNION ALL SELECT * FROM (VALUES (10, 1006), (NULL)) tmp",
                "Values have unequal number of columns");

        // column_0 should be non-nullable VARCHAR, and column_1 should be nullable TINYINT.
        String sql = "SELECT * FROM (VALUES (1,  2), (3, 4), ('10', NULL)) t;";
        QueryRelation queryRelation = ((QueryStatement) analyzeSuccess(sql)).getQueryRelation();

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan =
                new RelationTransformer(columnRefFactory, getConnectContext()).transform(queryRelation);
        List<ColumnRefOperator> outColumns = logicalPlan.getOutputColumn();

        Assertions.assertEquals(2, outColumns.size());
        Assertions.assertEquals(VarcharType.VARCHAR, outColumns.get(0).getType());
        Assertions.assertFalse(outColumns.get(0).isNullable());
        Assertions.assertEquals(IntegerType.TINYINT, outColumns.get(1).getType());
        Assertions.assertTrue(outColumns.get(1).isNullable());
    }

    @Test
    public void testWithSort() {
        analyzeSuccess("select t0.v1 from t0,t1 union all select v7 from t2 order by v1");
        analyzeSuccess("select v1 from t0,t1 union all select v7 from t2 order by v1");
        analyzeSuccess("select t0.v1 from t0,t1 union all select v7 from t2 order by t0.v1");
        analyzeSuccess("select t0.v1,v2 from t0,t1 union all select v7,v8 from t2 order by v2");
        analyzeFail("select t0.v1 from t0,t1 union all select v7 from t2 order by v2");
        analyzeFail("select t0.v1 from t0,t1 union all select v7 from t2 order by t1.v4");
        analyzeFail("select t0.v1 from t0,t1 union all select v7 from t2 order by t2.v7");
        analyzeFail("select t0.v1 from t0,t1 union all select v7 from t2 order by v7");
        analyzeFail("select t0.v1 from t0,t1 union all select v7 from t2 order by v8");
    }

    /**
     * INTERSECT and EXCEPT deduplicate by serializing a whole row into one byte string and comparing
     * those bytes. JSON has no single byte form -- velocypack stores object members in the order they
     * were written, and 1 and 1.0 are two encodings of one value -- so equal values compare as
     * different and the query silently returns the wrong rows. Off by default; enable_json_set_operation
     * puts it back for whoever knows their producer writes stable bytes.
     */
    @Test
    public void testJsonSetOperationNeedsOptIn() {
        SessionVariable sv = getConnectContext().getSessionVariable();
        boolean original = sv.isEnableJsonSetOperation();
        try {
            sv.setEnableJsonSetOperation(false);

            analyzeFail("select v_json from tjson intersect select v_json from tjson",
                    "INTERSECT over JSON can return wrong rows");
            analyzeFail("select v_json from tjson except select v_json from tjson",
                    "EXCEPT over JSON can return wrong rows");
            // MINUS is a synonym for EXCEPT and goes through the same relation.
            analyzeFail("select v_json from tjson minus select v_json from tjson",
                    "EXCEPT over JSON can return wrong rows");

            // The message has to be actionable on its own: it names the variable that lifts the
            // restriction, and the column and type it is complaining about.
            analyzeFail("select v_json from tjson intersect select v_json from tjson",
                    "Set enable_json_set_operation = true to run it anyway");
            analyzeFail("select v_json from tjson intersect select v_json from tjson",
                    "Column: 'v_json' (JSON)");

            // containsJson() recurses into containers, so ARRAY<JSON> is the same case.
            analyzeFail("select v5 from tarray intersect select v5 from tarray",
                    "INTERSECT over JSON can return wrong rows");
            analyzeFail("select v5 from tarray intersect select v5 from tarray",
                    "Column: 'v5' (ARRAY<JSON>)");

            // Only INTERSECT and EXCEPT compare JSON by bytes without anything else stopping them.
            // UNION ALL never compares its inputs at all, and UNION without ALL is already refused by
            // the backend's group-by type check -- neither is this variable's business, and neither
            // may start failing during analysis because of it.
            analyzeSuccess("select v_json from tjson union all select v_json from tjson");
            analyzeSuccess("select v_json from tjson union select v_json from tjson");
            analyzeSuccess("select v_json from tjson union distinct select v_json from tjson");
            analyzeSuccess("select v5 from tarray union all select v5 from tarray");

            // Nothing that does not carry JSON in its output may be touched. This is the part that
            // would show up as collateral damage, so it is asserted for both operations, for a JSON
            // column that stays out of the output list, and for a container of a non-JSON type.
            analyzeSuccess("select v1, v2, v3 from t0 intersect select v4, v5, v6 from t1");
            analyzeSuccess("select v1, v2, v3 from t0 except select v4, v5, v6 from t1");
            analyzeSuccess("select v_int from tjson intersect select v_int from tjson");
            analyzeSuccess("select v_int from tjson except select v_int from tjson");
            analyzeSuccess("select v3 from tarray intersect select v3 from tarray");

            // With the opt-in, analysis is back to what it was: the query plans and runs, byte
            // comparison and all.
            sv.setEnableJsonSetOperation(true);
            analyzeSuccess("select v_json from tjson intersect select v_json from tjson");
            analyzeSuccess("select v_json from tjson except select v_json from tjson");
            analyzeSuccess("select v_json from tjson minus select v_json from tjson");
            analyzeSuccess("select v5 from tarray intersect select v5 from tarray");
        } finally {
            sv.setEnableJsonSetOperation(original);
        }
    }
}
