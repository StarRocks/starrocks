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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.ast.expression.JoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCompressedValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
//import com.starrocks.sql.optimizer.operator.scalar.CompressedInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompressedInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LargeInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Transform large IN predicates to semi-join for better performance.
 *
 * Before:
 *   Filter(col IN (v1, v2, ..., vN))
 *     |
 *   Child
 *
 * After:
 *   LeftSemiJoin(col = const_col)
 *     |         |
 *   Child    Values(const_col: v1, v2, ..., vN)
 */
public class InPredicateToSemiJoinRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(InPredicateToSemiJoinRule.class);

    public InPredicateToSemiJoinRule() {
        super(RuleType.TF_IN_PREDICATE_TO_SEMI_JOIN,
              Pattern.create(OperatorType.LOGICAL_FILTER)
                     .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableInPredicateToSemiJoin()) {
            return false;
        }

        LogicalFilterOperator filterOp = (LogicalFilterOperator) input.getOp();
        ScalarOperator predicate = filterOp.getPredicate();
        
        // Don't transform if predicate contains OR
        if (containsOrPredicate(predicate)) {
            return false;
        }
        
        // Check if predicate contains exactly one LargeInPredicate and no NOT IN
        List<ScalarOperator> largeInPredicates = extractLargeInPredicates(predicate, context);
        
        // Must have exactly one LargeInPredicate
        if (largeInPredicates.size() != 1) {
            return false;
        }
        
        // Must not be NOT IN
        LargeInPredicateOperator largeIn = (LargeInPredicateOperator) largeInPredicates.get(0);
        if (largeIn.isNotIn()) {
            return false;
        }
        
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator filterOp = (LogicalFilterOperator) input.getOp();
        ScalarOperator predicate = filterOp.getPredicate();

        // Extract the single LargeInPredicate (we know there's exactly one from check())
        List<ScalarOperator> largeInPredicates = extractLargeInPredicates(predicate, context);
        ScalarOperator inPredicate = largeInPredicates.get(0);

        // Convert to Semi Join
        OptExpression result = convertToSemiJoin(input, inPredicate, context);

        return Lists.newArrayList(result);
    }

    /**
     * Check if predicate contains OR compound predicate
     */
    private boolean containsOrPredicate(ScalarOperator predicate) {
        if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compound = (CompoundPredicateOperator) predicate;
            if (compound.getCompoundType() == CompoundPredicateOperator.CompoundType.OR) {
                return true;
            }
        }

        // Recursively check children
        for (ScalarOperator child : predicate.getChildren()) {
            if (containsOrPredicate(child)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Extract all LargeInPredicates from the predicate tree
     */
    private List<ScalarOperator> extractLargeInPredicates(ScalarOperator predicate, OptimizerContext context) {
        List<ScalarOperator> result = new ArrayList<>();

        if (predicate instanceof LargeInPredicateOperator) {
            // Only extract LargeInPredicateOperator
            result.add(predicate);
        }

        // Recursively extract from children
        for (ScalarOperator child : predicate.getChildren()) {
            result.addAll(extractLargeInPredicates(child, context));
        }

        return result;
    }

    private OptExpression convertToSemiJoin(OptExpression input, ScalarOperator inPredicate,
                                           OptimizerContext context) {

        // 1. Create compressed constant table from LargeInPredicate
        LogicalCompressedValuesOperator compressedValuesOp = createCompressedConstantTable(inPredicate, context);
        OptExpression valuesExpr = OptExpression.create(compressedValuesOp);

        // 2. Remove LargeInPredicate from original Filter's predicate
        ScalarOperator originalPredicate = ((LogicalFilterOperator) input.getOp()).getPredicate();
        ScalarOperator remainingPredicate = removeInPredicate(originalPredicate, inPredicate);

        // 3. Create left side: either original child or Filter with remaining predicate
        OptExpression leftChild;
        if (remainingPredicate != null && !remainingPredicate.equals(ConstantOperator.TRUE)) {
            // Still have remaining predicates, keep the Filter
            LogicalFilterOperator newFilterOp = new LogicalFilterOperator(remainingPredicate);
            leftChild = OptExpression.create(newFilterOp, input.getInputs().get(0));
        } else {
            // No remaining predicates, remove LogicalFilter and use original child directly
            leftChild = input.getInputs().get(0);
        }

        // 4. Create Semi Join between the left side and Compressed Values
        ColumnRefOperator leftColumn = (ColumnRefOperator) inPredicate.getChild(0);
        ColumnRefOperator rightColumn = compressedValuesOp.getColumnRefSet().get(0);

        BinaryPredicateOperator joinPredicate = new BinaryPredicateOperator(
                BinaryType.EQ, leftColumn, rightColumn);

        LogicalJoinOperator semiJoinOp = new LogicalJoinOperator.Builder()
                .setJoinType(JoinOperator.LEFT_SEMI_JOIN)
                .setJoinHint("broadcast")
                .setOnPredicate(joinPredicate)
                .build();

        // 5. Build final Semi Join expression
        return OptExpression.create(semiJoinOp, leftChild, valuesExpr);
    }

    /**
     * Create a compressed constant table from LargeInPredicate for efficient serialization.
     * This avoids creating individual ConstantOperator objects and stores raw data instead.
     */
    private LogicalCompressedValuesOperator createCompressedConstantTable(ScalarOperator inPredicate,
                                                                        OptimizerContext context) {
        // Must be LargeInPredicateOperator
        LargeInPredicateOperator largeIn = (LargeInPredicateOperator) inPredicate;
        
        // Create column definition based on compatible type
        Type compatibleType = largeIn.getCompatibleType();
        ColumnRefOperator column = context.getColumnRefFactory().create(
                "const_value", compatibleType, false);

        // Use raw constant list directly without parsing
        String rawConstantList = largeIn.getRawConstantList();
        int constantCount = largeIn.getConstantCount();
        
        return new LogicalCompressedValuesOperator(
                Lists.newArrayList(column), 
                compatibleType,
                rawConstantList, 
                constantCount);
    }

    /**
     * Legacy method - create parsed constant table (memory intensive for large lists).
     * Kept for fallback scenarios.
     */
    private LogicalValuesOperator createConstantTable(ScalarOperator inPredicate,
                                                     OptimizerContext context) {
        // Must be LargeInPredicateOperator
        LargeInPredicateOperator largeIn = (LargeInPredicateOperator) inPredicate;
        
        // Create column definition based on compatible type
        Type compatibleType = largeIn.getCompatibleType();
        ColumnRefOperator column = context.getColumnRefFactory().create(
                "const_value", compatibleType, false);

        // Parse rawConstantList by splitting on comma
        String rawConstantList = largeIn.getRawConstantList();
        String[] constantStrings = rawConstantList.split(",");
        
        List<List<ScalarOperator>> rows = Lists.newArrayList();
        
        for (String constantStr : constantStrings) {
            String trimmedConstant = constantStr.trim();
            if (trimmedConstant.isEmpty()) {
                continue; // Skip empty strings
            }
            
            // Create ConstantOperator based on compatible type
            ConstantOperator constant = createConstantFromString(trimmedConstant, compatibleType);
            rows.add(Lists.newArrayList(constant));
        }

        return new LogicalValuesOperator(Lists.newArrayList(column), rows);
    }
    
    /**
     * Create ConstantOperator from string value based on the given type
     */
    private ConstantOperator createConstantFromString(String value, Type type) {
        try {
            switch (type.getPrimitiveType()) {
                case BIGINT:
                    return ConstantOperator.createBigint(Long.parseLong(value));
                case VARCHAR:
                    // Remove quotes if present
                    String strValue = value;
                    if ((strValue.startsWith("'") && strValue.endsWith("'")) ||
                        (strValue.startsWith("\"") && strValue.endsWith("\""))) {
                        strValue = strValue.substring(1, strValue.length() - 1);
                    }
                    return ConstantOperator.createVarchar(strValue);
                default:
                    LOG.warn("Unsupported type {} for constant {}, using VARCHAR", type, value);
                    return ConstantOperator.createVarchar(value);
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse constant {} as type {}, treating as NULL: {}", value, type, e.getMessage());
            return ConstantOperator.createNull(type);
        }
    }

    private ScalarOperator removeInPredicate(ScalarOperator predicate, ScalarOperator toRemove) {
        if (predicate.equals(toRemove)) {
            return null; // The entire predicate is the LargeInPredicate
        }

        // Extract all conjuncts (AND conditions) into a flat list
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        
        // Remove the LargeInPredicate from the conjuncts
        List<ScalarOperator> remainingConjuncts = new ArrayList<>();
        for (ScalarOperator conjunct : conjuncts) {
            if (!conjunct.equals(toRemove)) {
                remainingConjuncts.add(conjunct);
            }
        }
        
        // Reconstruct the predicate from remaining conjuncts
        if (remainingConjuncts.isEmpty()) {
            return null; // No remaining conditions
        } else if (remainingConjuncts.size() == 1) {
            return remainingConjuncts.get(0); // Single condition
        } else {
            return Utils.compoundAnd(remainingConjuncts); // Multiple conditions with AND
        }
    }
}
