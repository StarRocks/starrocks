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

import com.starrocks.connector.iceberg.ScalarOperatorToIcebergExpr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IcebergPredicateSqlTest extends PlanTestBase {
    @Test
    public void testTrinoIntegralScientificPredicatesReachIceberg() throws Exception {
        String previousDialect = connectContext.getSessionVariable().getSqlDialect();
        try {
            connectContext.getSessionVariable().setSqlDialect("trino");
            // Use the existing BIGINT fixture to exercise parsing and the full scalar rewrite pipeline.
            // Then convert its optimized scan predicate with the same Iceberg LONG column types.
            OptExpression plan = getExecPlan("SELECT * FROM t0 WHERE v1 = 8.094E4 "
                    + "AND v2 IN (2.1E1, 2.3E1, 2.5E1, 3.2E1, 5.9E1) AND v3 > 0E0").getPhysicalPlan();
            ScalarOperator predicate = scanPredicate(plan);
            Assertions.assertNotNull(predicate);
            Assertions.assertFalse(predicate.toString().contains("cast"), predicate.toString());
            Schema schema = new Schema(Types.NestedField.optional(1, "v1", Types.LongType.get()),
                    Types.NestedField.optional(2, "v2", Types.LongType.get()),
                    Types.NestedField.optional(3, "v3", Types.LongType.get()));
            Expression actual = new ScalarOperatorToIcebergExpr().convert(Utils.extractConjuncts(predicate),
                    new ScalarOperatorToIcebergExpr.IcebergContext(schema.asStruct()));
            String filter = actual.toString();
            Assertions.assertTrue(filter.contains(Expressions.equal("v1", 80940L).toString()), filter);
            Assertions.assertTrue(filter.contains(Expressions.in("v2", 21L, 23L, 25L, 32L, 59L).toString()), filter);
            Assertions.assertTrue(filter.contains(Expressions.greaterThan("v3", 0L).toString()), filter);
        } finally {
            connectContext.getSessionVariable().setSqlDialect(previousDialect);
        }
    }

    private static ScalarOperator scanPredicate(OptExpression expression) {
        if (expression.getOp() instanceof PhysicalScanOperator scan) {
            return scan.getPredicate();
        }
        for (OptExpression input : expression.getInputs()) {
            ScalarOperator result = scanPredicate(input);
            if (result != null) {
                return result;
            }
        }
        return null;
    }
}
