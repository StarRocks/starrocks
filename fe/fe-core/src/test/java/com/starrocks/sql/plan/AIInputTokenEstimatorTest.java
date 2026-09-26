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
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnBasicStatsCacheLoader;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.statistic.StatisticsCollectJob;
import com.starrocks.statistic.base.PrimitiveTypeColumnStats;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

class AIInputTokenEstimatorTest {
    private static final ColumnRefOperator TEXT = new ColumnRefOperator(1, VarcharType.VARCHAR, "text", true);
    private static final ColumnRefOperator OUTPUT = new ColumnRefOperator(2, VarcharType.VARCHAR, "result", true);

    @Test
    void testUtf8NullAndCapability() {
        OptExpression values = values();
        assertTokens("26", estimate(200100, values, ConstantOperator.createVarchar("中🙂abc")));
        assertTokens("10", estimate(200130, values, ConstantOperator.createVarchar("中🙂abc")));
        assertTokens("26", estimate(200140, values,
                ConstantOperator.createVarchar("provider"), ConstantOperator.createVarchar("中🙂abc")));
        assertTokens("16", estimate(200100, values, ConstantOperator.createVarchar("")));
        assertTokens("0", estimate(200100, scan(100, ColumnStatistic.unknown()),
                ConstantOperator.createNull(VarcharType.VARCHAR)));
    }

    @Test
    void testAnalyzedColumnsAliasesAndCommonExpressions() {
        OptExpression scan = scan(2.2, ColumnStatistic.builder().setAverageRowSize(0.5).build());
        assertTokens("54", estimate(200100, scan, TEXT));
        ColumnRefOperator alias = new ColumnRefOperator(3, VarcharType.VARCHAR, "alias", true);
        OptExpression project = OptExpression.create(new PhysicalProjectOperator(Map.of(alias, TEXT), Map.of()), scan);
        assertTokens("54", estimate(200100, project, alias));
        ScalarOperator concat = concat(ConstantOperator.createVarchar("x"), alias);
        assertTokens("57", estimate(200100, project, concat));
        ColumnRefOperator commonOutput = new ColumnRefOperator(4, VarcharType.VARCHAR, "common", true);
        OptExpression common = OptExpression.create(new PhysicalProjectOperator(Map.of(commonOutput, alias),
                Map.of(alias, concat(TEXT, ConstantOperator.createVarchar("xy")))), scan);
        assertTokens("60", estimate(200100, common, commonOutput));
    }

    @Test
    void testNativeStringStatisticsUseCharacterUnits() {
        String dataSizeExpression = "IFNULL(SUM(CHAR_LENGTH(`text`)), 0)";
        Assertions.assertEquals(dataSizeExpression,
                StatisticsCollectJob.fullAnalyzeGetDataSize("`text`", VarcharType.VARCHAR));
        Assertions.assertEquals(dataSizeExpression,
                new PrimitiveTypeColumnStats("text", VarcharType.VARCHAR).getFullDataSize());

        // Collected rows: "中🙂abc", NULL. CHAR_LENGTH totals 5, while UTF-8 totals 10 bytes.
        TStatisticData data = new TStatisticData().setRowCount(2).setNullCount(1).setDataSize(5);
        ColumnStatistic statistic = ColumnBasicStatsCacheLoader.buildColumnStatistics(
                data, "default_catalog", "test", "t", "text", VarcharType.VARCHAR);
        Assertions.assertEquals(2.5, statistic.getAverageRowSize());
        Assertions.assertEquals(0.5, statistic.getNullsFraction());

        OptExpression scan = scan(2, statistic);
        assertTokens("20", estimate(200130, scan, TEXT));
        assertTokens("52", estimate(200100, scan, TEXT));
    }

    @Test
    void testMissingOrUntrustedStatisticsRemainUnknown() {
        for (ColumnStatistic statistic : List.of(ColumnStatistic.unknown(),
                ColumnStatistic.builder().setAverageRowSize(Double.NaN).build(),
                ColumnStatistic.builder().setAverageRowSize(Double.POSITIVE_INFINITY).build(),
                ColumnStatistic.builder().setAverageRowSize(-1).build())) {
            assertUnknown(estimate(200100, scan(10, statistic), TEXT));
        }
        OptExpression scan = scan(10, ColumnStatistic.builder().setAverageRowSize(2).build());
        scan.setStatistics(Statistics.builder().setOutputRowCount(10).build());
        assertUnknown(estimate(200100, scan, TEXT));
        // Even a plausible CBO fallback width is not analyzed text-size evidence.
        scan.setStatistics(Statistics.builder().setOutputRowCount(10)
                .addColumnStatistic(TEXT, ColumnStatistic.builder().setAverageRowSize(2).build()).build());
        assertUnknown(estimate(200100, scan, TEXT));
    }

    @Test
    void testLocalLimitsAndBroadcastDoNotUnderestimate() {
        OptExpression scan = scan(10, ColumnStatistic.builder().setAverageRowSize(2).build());
        scan.getOp().setLimit(1);
        assertUnknown(estimate(200100, scan, TEXT));
        scan.getOp().setLimit(Operator.DEFAULT_LIMIT);
        OptExpression broadcast = OptExpression.create(
                new PhysicalDistributionOperator(DistributionSpec.createReplicatedDistributionSpec()), scan);
        assertUnknown(estimate(200100, broadcast, TEXT));
        OptExpression gather = OptExpression.create(
                new PhysicalDistributionOperator(DistributionSpec.createGatherDistributionSpec()), scan);
        assertTokens("240", estimate(200100, gather, TEXT));
    }

    @Test
    void testOverflowAndIncompleteAggregate() {
        AIInputTokenEstimate huge = estimate(200100,
                scan(1e20, ColumnStatistic.builder().setAverageRowSize(1e10).build()), TEXT);
        assertTokens("4000000001600000000000000000000", huge);
        Assertions.assertEquals(huge.getTokens().multiply(BigInteger.TWO), huge.add(huge).getTokens());
        Assertions.assertSame(huge, AIInputTokenEstimate.none().add(huge));
        AIInputTokenEstimate unknown = AIInputTokenEstimate.unknown("missing statistics");
        assertUnknown(huge.add(unknown));
        assertUnknown(unknown.add(huge));
        Assertions.assertNull(huge.add(unknown).getTokens());
    }

    @Test
    void testUnsupportedExpressionsAndCycles() {
        OptExpression scan = scan(10, ColumnStatistic.builder().setAverageRowSize(2).build());
        assertUnknown(estimate(200124, scan, TEXT));
        assertUnknown(estimate(200100, scan, new CallOperator("repeat", VarcharType.VARCHAR,
                List.of(TEXT, ConstantOperator.createInt(10)))));
        ColumnRefOperator other = new ColumnRefOperator(3, VarcharType.VARCHAR, "other", true);
        OptExpression cyclic = OptExpression.create(new PhysicalProjectOperator(Map.of(TEXT, other, other, TEXT), Map.of()),
                scan);
        assertUnknown(estimate(200100, cyclic, TEXT));
    }

    @Test
    void testConcatUdfIsNotTreatedAsBuiltin() {
        Function udf = Mockito.mock(Function.class);
        Mockito.when(udf.getBinaryType()).thenReturn(TFunctionBinaryType.SRJAR);
        assertUnknown(estimate(200100, values(),
                new CallOperator(FunctionSet.CONCAT, VarcharType.VARCHAR, List.of(), udf)));
    }

    private static CallOperator concat(ScalarOperator... arguments) {
        Function builtin = Mockito.mock(Function.class);
        Mockito.when(builtin.getBinaryType()).thenReturn(TFunctionBinaryType.BUILTIN);
        return new CallOperator(FunctionSet.CONCAT, VarcharType.VARCHAR, List.of(arguments), builtin);
    }

    private static OptExpression values() {
        return OptExpression.create(new PhysicalValuesOperator(List.of(), List.of(List.of()),
                Operator.DEFAULT_LIMIT, null, null));
    }

    private static OptExpression scan(double rows, ColumnStatistic statistic) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.isNativeTable()).thenReturn(true);
        Mockito.when(table.getBaseIndexMetaId()).thenReturn(1L);
        OptExpression expression = OptExpression.create(new PhysicalOlapScanOperator(table,
                Map.of(TEXT, new Column("text", VarcharType.VARCHAR)), DistributionSpec.createAnyDistributionSpec(),
                Operator.DEFAULT_LIMIT, null, 1, List.of(), List.of(), List.of(), List.of(), null, false, null));
        expression.setStatistics(Statistics.builder().setOutputRowCount(rows).setStatsSource(Statistics.StatsSource.ANALYZE)
                .addColumnStatistic(TEXT, statistic).build());
        return expression;
    }

    private static AIInputTokenEstimate estimate(long functionId, OptExpression input, ScalarOperator... arguments) {
        Function function = Mockito.mock(Function.class);
        Mockito.when(function.isAi()).thenReturn(true);
        Mockito.when(function.getFunctionId()).thenReturn(functionId);
        CallOperator call = new CallOperator("ai", VarcharType.VARCHAR, List.of(arguments), function);
        OptExpression ai = OptExpression.create(new PhysicalAIProjectOperator(Map.of(OUTPUT, call), Map.of()), input);
        return AIInputTokenEstimator.estimate(ai, false);
    }

    private static void assertTokens(String expected, AIInputTokenEstimate estimate) {
        Assertions.assertEquals(AIInputTokenEstimate.Status.ESTIMATED, estimate.getStatus(), estimate.toString());
        Assertions.assertEquals(new BigInteger(expected), estimate.getTokens());
    }

    private static void assertUnknown(AIInputTokenEstimate estimate) {
        Assertions.assertEquals(AIInputTokenEstimate.Status.UNKNOWN, estimate.getStatus(), estimate.toString());
    }
}
