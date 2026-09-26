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

import com.starrocks.builtins.VectorizedBuiltinFunctions;
import com.starrocks.builtins.VectorizedBuiltinFunctions.AICapability;
import com.starrocks.builtins.VectorizedBuiltinFunctions.AIFunctionDescriptor;
import com.starrocks.builtins.VectorizedBuiltinFunctions.AIPromptKind;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.thrift.TFunctionBinaryType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/** Reads the final physical plan without changing optimizer statistics or resolving model configuration. */
final class AIInputTokenEstimator {
    // Heuristic reserve for the BE's fixed system/user messages, not a measured provider token count
    // or an upper bound on billed tokens.
    private static final BigInteger CHAT_FRAMING_TOKENS = BigInteger.valueOf(16);

    private AIInputTokenEstimator() {
    }

    static AIInputTokenEstimate estimate(OptExpression expression, boolean unpartitioned) {
        PhysicalAIProjectOperator project = (PhysicalAIProjectOperator) expression.getOp();
        AIInputTokenEstimate total = AIInputTokenEstimate.none();
        for (ScalarOperator scalar : project.getColumnRefMap().values()) {
            if (scalar instanceof CallOperator call && call.getFunction() != null && call.getFunction().isAi()) {
                total = total.add(estimateCall(call, expression, unpartitioned));
            }
        }
        return total;
    }

    private static AIInputTokenEstimate estimateCall(CallOperator call, OptExpression expression, boolean unpartitioned) {
        AIFunctionDescriptor descriptor = VectorizedBuiltinFunctions.getAIFunctionDescriptor(
                call.getFunction().getFunctionId());
        if (descriptor == null || descriptor.promptKind() != AIPromptKind.PASSTHROUGH
                || descriptor.inputArguments().size() != 1) {
            return AIInputTokenEstimate.unknown("unsupported prompt template");
        }
        if (call.getChildren().stream().anyMatch(argument -> argument.getType().isMapType())) {
            return AIInputTokenEstimate.unknown("request options may change input tokens");
        }
        TextSize text = textSize(call.getChild(descriptor.inputArguments().get(0)), expression, new HashSet<>());
        if (text.isNull()) {
            return AIInputTokenEstimate.estimated(BigInteger.ZERO);
        }
        if (text.bytes() == null) {
            return AIInputTokenEstimate.unknown("unsupported expression or missing text statistics");
        }
        BigInteger rows = rows(expression.inputAt(0), unpartitioned);
        if (rows == null) {
            return AIInputTokenEstimate.unknown("unknown input cardinality or local execution limit");
        }
        // Use one estimated text token per UTF-8 byte (or byte proxy from statistics), not tokenizer output.
        BigInteger perRowTokens = text.bytes().setScale(0, RoundingMode.CEILING).toBigIntegerExact();
        if (descriptor.capability() == AICapability.CHAT) {
            perRowTokens = perRowTokens.add(CHAT_FRAMING_TOKENS);
        }
        return AIInputTokenEstimate.estimated(perRowTokens.multiply(rows));
    }

    private static BigInteger rows(OptExpression expression, boolean unpartitioned) {
        Operator operator = expression.getOp();
        // These are global bounds in the fragment actually built by PlanFragmentBuilder. A local TopN's
        // statistics also contain LIMIT, but its work is multiplied across instances and cannot be used here.
        if (unpartitioned && operator instanceof PhysicalLimitOperator) {
            return BigInteger.valueOf(operator.getLimit());
        }
        if (unpartitioned && operator instanceof PhysicalTopNOperator topN && topN.isSplit()
                && topN.getSortPhase() == SortPhase.FINAL && topN.getTopNType() == TopNType.ROW_NUMBER
                && !topN.isPerPipeline()
                && (topN.getPartitionByColumns() == null || topN.getPartitionByColumns().isEmpty()) && topN.hasLimit()) {
            return BigInteger.valueOf(topN.getLimit());
        }
        if (operator.hasLimit()) {
            return null;
        }
        if (operator instanceof PhysicalValuesOperator values && operator.getPredicate() == null) {
            return BigInteger.valueOf(values.getRows().size());
        }
        if (operator instanceof PhysicalOlapScanOperator scan && isBaseTableScan(scan)) {
            return statisticRows(expression.getStatistics());
        }
        if (!isPassThrough(operator) || expression.arity() != 1) {
            return null;
        }
        if (operator instanceof PhysicalDistributionOperator distribution) {
            if (distribution.getDistributionSpec().getType() == DistributionSpec.DistributionType.BROADCAST) {
                return null;
            }
            // Crossing an exchange never makes the source fragment's limits global.
            return rows(expression.inputAt(0), false);
        }
        BigInteger inputRows = rows(expression.inputAt(0), unpartitioned);
        if (inputRows == null || !(operator instanceof PhysicalFilterOperator)) {
            return inputRows;
        }
        BigInteger filteredRows = statisticRows(expression.getStatistics());
        return filteredRows == null ? null : inputRows.min(filteredRows);
    }

    private static BigInteger statisticRows(Statistics statistics) {
        if (statistics == null || statistics.isTableRowCountMayInaccurate()
                || !Double.isFinite(statistics.getOutputRowCount()) || statistics.getOutputRowCount() < 0) {
            return null;
        }
        return BigDecimal.valueOf(statistics.getOutputRowCount()).setScale(0, RoundingMode.CEILING).toBigIntegerExact();
    }

    private record TextSize(BigDecimal bytes, boolean isNull) {
        private static final TextSize UNKNOWN = new TextSize(null, false);
        private static final TextSize NULL = new TextSize(BigDecimal.ZERO, true);
    }

    private static TextSize textSize(ScalarOperator scalar, OptExpression expression, Set<ColumnRefOperator> resolving) {
        if (scalar instanceof ConstantOperator constant) {
            if (constant.isNull()) {
                return TextSize.NULL;
            }
            if (constant.getType().isStringType()) {
                return new TextSize(BigDecimal.valueOf(constant.getVarchar().getBytes(StandardCharsets.UTF_8).length), false);
            }
        } else if (scalar instanceof ColumnRefOperator column) {
            if (!resolving.add(column)) {
                return TextSize.UNKNOWN;
            }
            TextSize result = columnSize(column, expression, resolving);
            resolving.remove(column);
            return result;
        } else if (scalar instanceof CallOperator call && FunctionSet.CONCAT.equalsIgnoreCase(call.getFnName())
                && call.getFunction() != null && call.getFunction().getBinaryType() == TFunctionBinaryType.BUILTIN) {
            BigDecimal bytes = BigDecimal.ZERO;
            for (ScalarOperator child : call.getChildren()) {
                TextSize size = textSize(child, expression, resolving);
                if (size.isNull()) {
                    return TextSize.NULL;
                }
                if (size.bytes() == null) {
                    return TextSize.UNKNOWN;
                }
                bytes = bytes.add(size.bytes());
            }
            return new TextSize(bytes, false);
        }
        return TextSize.UNKNOWN;
    }

    private static TextSize columnSize(ColumnRefOperator column, OptExpression expression,
                                       Set<ColumnRefOperator> resolving) {
        Operator operator = expression.getOp();
        Projection projection = operator.getProjection();
        ScalarOperator source = projection == null ? null : projection.resolveColumnRef(column);
        if (source == null && operator instanceof PhysicalProjectOperator project) {
            source = project.getColumnRefMap().getOrDefault(column, project.getCommonSubOperatorMap().get(column));
        } else if (source == null && operator instanceof PhysicalAIProjectOperator project) {
            source = project.getColumnRefMap().getOrDefault(column, project.getCommonSubOperatorMap().get(column));
        }
        if (source != null && !source.equals(column)) {
            return textSize(source, expression, resolving);
        }
        if (operator instanceof PhysicalOlapScanOperator scan && isBaseTableScan(scan)) {
            Column metadata = scan.getColRefToColumnMetaMap().get(column);
            Statistics statistics = expression.getStatistics();
            if (metadata == null || metadata.isGeneratedColumn() || !metadata.getType().isStringType()
                    || scan.getGlobalDictsExpr().containsKey(column.getId()) || statistics == null
                    || statistics.getStatsSource() != Statistics.StatsSource.ANALYZE) {
                return TextSize.UNKNOWN;
            }
            ColumnStatistic statistic = statistics.getColumnStatistics().get(column);
            if (statistic == null || statistic.isUnknown() || !Double.isFinite(statistic.getAverageRowSize())
                    || statistic.getAverageRowSize() < 0) {
                return TextSize.UNKNOWN;
            }
            // StatisticsCollectJob and PrimitiveTypeColumnStats sum CHAR_LENGTH for native strings;
            // ColumnBasicStatsCacheLoader divides by all rows. Allow up to four UTF-8 bytes per character.
            return new TextSize(BigDecimal.valueOf(statistic.getAverageRowSize()).multiply(BigDecimal.valueOf(4)), false);
        }
        if ((isPassThrough(operator) || operator instanceof PhysicalLimitOperator
                || operator instanceof PhysicalTopNOperator) && expression.arity() == 1) {
            return textSize(column, expression.inputAt(0), new HashSet<>());
        }
        return TextSize.UNKNOWN;
    }

    private static boolean isBaseTableScan(PhysicalOlapScanOperator scan) {
        return scan.getTable() instanceof OlapTable table && table.isNativeTable() && !table.isMaterializedView()
                && scan.getSelectedIndexMetaId() == table.getBaseIndexMetaId();
    }

    private static boolean isPassThrough(Operator operator) {
        return operator instanceof PhysicalProjectOperator || operator instanceof PhysicalFilterOperator
                || operator instanceof PhysicalAIProjectOperator || operator instanceof PhysicalDistributionOperator;
    }
}
