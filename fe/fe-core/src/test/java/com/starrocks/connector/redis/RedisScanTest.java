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

package com.starrocks.connector.redis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.RedisTable;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalRedisScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRedisScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rule.implementation.RedisScanImplementationRule;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisScanTest {

    static ColumnRefOperator intColumnOperator = new ColumnRefOperator(1, IntegerType.INT, "id", true);
    static ColumnRefOperator strColumnOperator = new ColumnRefOperator(2, VarcharType.VARCHAR, "name", true);

    static Map<ColumnRefOperator, Column> scanColumnMap = new HashMap<>() {
        {
            put(intColumnOperator, new Column("id", IntegerType.INT));
            put(strColumnOperator, new Column("name", VarcharType.VARCHAR));
        }
    };

    static RedisTable redisTable;
    static LogicalRedisScanOperator logicalRedisScanOperator;
    static PhysicalRedisScanOperator physicalRedisScanOperator;
    static RedisScanImplementationRule redisRule = new RedisScanImplementationRule();

    @Mocked
    static OptimizerContext optimizerContext;

    @BeforeAll
    public static void setUp() throws IOException {
        redisTable = new RedisTable();
        logicalRedisScanOperator = new LogicalRedisScanOperator(redisTable,
                scanColumnMap, Maps.newHashMap(), -1,
                new BinaryPredicateOperator(BinaryType.EQ,
                        new ColumnRefOperator(1, IntegerType.INT, "id", true),
                        ConstantOperator.createInt(1)));
        OptExpression scan = new OptExpression(logicalRedisScanOperator);
        List<OptExpression> transform = redisRule.transform(scan, optimizerContext);
        physicalRedisScanOperator = (PhysicalRedisScanOperator) transform.get(0).getOp();
    }

    @Test
    public void testPhysicalRedisScanOperator() {
        ScanOperatorPredicates scanOperatorPredicates = physicalRedisScanOperator.getScanOperatorPredicates();
        Assertions.assertNotNull(scanOperatorPredicates);
        ColumnRefSet usedColumns = physicalRedisScanOperator.getUsedColumns();
        Assertions.assertNotNull(usedColumns);
        Assertions.assertEquals(2, usedColumns.size());
    }

    @Test
    public void testPlanFragmentBuilder(@Mocked com.starrocks.qe.ConnectContext connectContext,
                                        @Mocked ColumnRefFactory columnRefFactory) {
        OptExpression phys = new OptExpression(physicalRedisScanOperator);
        ExecPlan plan = PlanFragmentBuilder.createPhysicalPlan(phys, connectContext,
                physicalRedisScanOperator.getOutputColumns(), columnRefFactory,
                ImmutableList.of("id"), TResultSinkType.FILE, true);
        Assertions.assertNotNull(plan);
        Assertions.assertEquals("id", plan.getColNames().get(0));
    }

    @Test
    public void testLogicalRedisScanOperatorBuilder() {
        LogicalRedisScanOperator.Builder builder = new LogicalRedisScanOperator.Builder();
        LogicalRedisScanOperator cloneOperator = builder.withOperator(logicalRedisScanOperator).build();
        Assertions.assertEquals(logicalRedisScanOperator, cloneOperator);
    }
}
