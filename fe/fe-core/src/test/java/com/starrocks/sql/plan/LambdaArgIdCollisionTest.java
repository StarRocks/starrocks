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

import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

// Regression test for historical bug fixed by #73273: when the same analyzed AST is
// transformed twice with different ColumnRefFactory instances, lambda-argument ColumnRefOperator
// ids must not leak across factories and collide with scalar column ids.
//
// The original bug cached the lambda argument ColumnRefOperator on the AST node
// (LambdaArgument.transformedOp). A second transform with a fresh factory could then reuse that
// stale id and collide with a newly created scalar column id, corrupting types (e.g. VARCHAR
// transaction_uuid/client_key becoming the array element struct<...>) and producing invalid
// predicates (e.g. struct = varchar join conjunct).
public class LambdaArgIdCollisionTest extends PlanTestNoneDBBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
        starRocksAssert.withDatabase("repro_lambda").useDatabase("repro_lambda");
        starRocksAssert.withTable("CREATE TABLE `conn_event` (\n" +
                "  `transaction_uuid` varchar(200) NULL,\n" +
                "  `client_key` varchar(200) NULL,\n" +
                "  `request_attributes` ARRAY<STRUCT<name varchar(200), value varchar(200)>> NULL,\n" +
                "  `errors` ARRAY<STRUCT<code varchar(200), description varchar(200)>> NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`transaction_uuid`)\n" +
                "DISTRIBUTED BY HASH(`transaction_uuid`) BUCKETS 3\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");
    }

    @Test
    public void testLambdaArgDoesNotCorruptScalarColumnType() throws Exception {
        String sql = "WITH base AS (\n" +
                "    SELECT transaction_uuid, client_key, errors,\n" +
                "           array_filter(request_attributes, x -> x.name = 'unitId')[1].value AS room_id,\n" +
                "           array_filter(request_attributes, x -> x.name = 'ratePlanId')[1].value AS rate_plan_id\n" +
                "    FROM conn_event\n" +
                "),\n" +
                "errs AS (\n" +
                "    SELECT b.transaction_uuid AS transaction_uuid, unnest.code AS code\n" +
                "    FROM base b CROSS JOIN UNNEST(b.errors) AS unnest\n" +
                ")\n" +
                "SELECT LEFT(c.transaction_uuid, 50) AS tid, LEFT(c.client_key, 50) AS ck,\n" +
                "       c.room_id, e.code\n" +
                "FROM base c INNER JOIN errs e ON e.transaction_uuid = c.transaction_uuid";

        StatementBase statement = SqlParser.parse(sql,
                connectContext.getSessionVariable().getSqlMode()).get(0);
        Analyzer.analyze(statement, connectContext);
        QueryStatement query = (QueryStatement) statement;

        // First pass: warms the lambda-argument cache on the shared AST (LambdaArgument.transformedOp).
        new RelationTransformer(new ColumnRefFactory(), connectContext).transform(query.getQueryRelation());

        // Second pass over the SAME AST with a fresh factory, exactly what plan retry / MV
        // rewrite do. On buggy code the cached lambda-arg ids leak in and collide here.
        ColumnRefFactory factory2 = new ColumnRefFactory();
        LogicalPlan plan2 = new RelationTransformer(factory2, connectContext).transform(query.getQueryRelation());

        List<ColumnRefOperator> all = new ArrayList<>();
        collectColumnRefs(plan2.getRoot(), all);

        // BUG signature: one id is owned by both a lambda argument and a normal column.
        Map<Integer, ColumnRefOperator> lambdaById = new HashMap<>();
        Map<Integer, ColumnRefOperator> scalarById = new HashMap<>();
        for (ColumnRefOperator ref : all) {
            if (ref.getOpType() == OperatorType.LAMBDA_ARGUMENT) {
                lambdaById.put(ref.getId(), ref);
            } else {
                scalarById.put(ref.getId(), ref);
            }
        }
        Set<Integer> collisions = new TreeSet<>(lambdaById.keySet());
        collisions.retainAll(scalarById.keySet());
        Assertions.assertTrue(collisions.isEmpty(),
                "lambda argument id collides with a scalar column id (lambda-arg id leaked across "
                        + "ColumnRefFactory). colliding ids=" + collisions
                        + ", lambda=" + lambdaById + ", scalar=" + scalarById);

        // And the varchar scalar columns must never be re-typed as struct.
        for (ColumnRefOperator ref : all) {
            if (("transaction_uuid".equals(ref.getName()) || "client_key".equals(ref.getName()))
                    && ref.getOpType() != OperatorType.LAMBDA_ARGUMENT) {
                Assertions.assertFalse(ref.getType().isStructType(),
                        ref.getName() + " should stay VARCHAR but is " + ref.getType()
                                + " — lambda arg id collision corrupted its type.");
            }
        }
    }

    private static void collectColumnRefs(OptExpression expr, List<ColumnRefOperator> out) {
        if (expr.getOp() instanceof LogicalProjectOperator) {
            LogicalProjectOperator project = (LogicalProjectOperator) expr.getOp();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> e : project.getColumnRefMap().entrySet()) {
                collectFromScalar(e.getKey(), out);
                collectFromScalar(e.getValue(), out);
            }
        }
        if (expr.getOp() instanceof LogicalJoinOperator) {
            collectFromScalar(((LogicalJoinOperator) expr.getOp()).getOnPredicate(), out);
        }
        collectFromScalar(expr.getOp().getPredicate(), out);
        if (expr.getOp().getProjection() != null) {
            for (ScalarOperator s : expr.getOp().getProjection().getColumnRefMap().values()) {
                collectFromScalar(s, out);
            }
            for (ColumnRefOperator c : expr.getOp().getProjection().getColumnRefMap().keySet()) {
                collectFromScalar(c, out);
            }
        }
        for (OptExpression child : expr.getInputs()) {
            collectColumnRefs(child, out);
        }
    }

    private static void collectFromScalar(ScalarOperator s, List<ColumnRefOperator> out) {
        if (s == null) {
            return;
        }
        if (s instanceof ColumnRefOperator) {
            out.add((ColumnRefOperator) s);
        }
        if (s instanceof LambdaFunctionOperator) {
            LambdaFunctionOperator lambda = (LambdaFunctionOperator) s;
            for (ColumnRefOperator ref : lambda.getRefColumns()) {
                out.add(ref);
            }
            // The CSE/reuse columnRefMap holds ColumnRefOperators (keys) and their defining
            // expressions (values) that live only in the map: the lambda body references the
            // keys, so the values are not reachable via getChildren()/getLambdaExpr().
            for (Map.Entry<ColumnRefOperator, ScalarOperator> e : lambda.getColumnRefMap().entrySet()) {
                collectFromScalar(e.getKey(), out);
                collectFromScalar(e.getValue(), out);
            }
            // lambdaExpr is traversed below via getChildren() (== [lambdaExpr]); don't recurse it here.
        }
        for (ScalarOperator child : s.getChildren()) {
            collectFromScalar(child, out);
        }
    }
}