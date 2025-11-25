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
// limitations under the License

package com.starrocks.sql.optimizer.rule.tvr;

import com.google.common.collect.Lists;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.sql.ast.expression.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrLazyOptExpression;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOptExpression;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOptMeta;

import java.util.List;

/**
 * TvrJoinRule transforms JOIN operators for incremental view maintenance (IVM).
 *
 * Supported Join Types:
 * - INNER JOIN / CROSS JOIN: Δ(A ⋈ B) = (ΔA ⋈ B_snapshot) ∪ (A_snapshot ⋈ ΔB)
 * - LEFT OUTER JOIN: Additional handling for NULL row changes
 * - RIGHT OUTER JOIN: Symmetric to LEFT OUTER JOIN
 * - FULL OUTER JOIN: Combination of both sides
 *
 * For retractable inputs (with DELETE/UPDATE operations), the op column (StreamRowOp)
 * is propagated through the join to correctly mark output rows.
 */

public class TvrJoinRule extends TvrTransformationRule {

    public TvrJoinRule() {
        super(RuleType.TF_TVR_JOIN, Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF), Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return isSupportedTvr(input);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator join = input.getOp().cast();
        OptExpression leftChildDelta = input.inputAt(0);
        TvrOptMeta leftOptMeta = leftChildDelta.getTvrMeta();
        OptExpression rightChildDelta = input.inputAt(1);
        TvrOptMeta rightOptMeta = rightChildDelta.getTvrMeta();

        List<ColumnRefOperator> joinOutputColRefs = input.getRowOutputInfo().getOutputColRefs();
        JoinOperator joinType = join.getJoinType();

        // Determine if we have retractable inputs
        boolean hasRetractableInput = !leftOptMeta.isAppendOnly() || !rightOptMeta.isAppendOnly();

        OptExpression deltaJoin;
        if (joinType.isInnerJoin() || joinType.isCrossJoin()) {
            // INNER JOIN / CROSS JOIN
            deltaJoin = transformInnerJoin(context, input, join, joinOutputColRefs,
                    leftOptMeta, rightOptMeta, hasRetractableInput);
        } else if (joinType.isLeftOuterJoin()) {
            // LEFT OUTER JOIN
            deltaJoin = transformLeftOuterJoin(context, input, join, joinOutputColRefs,
                    leftOptMeta, rightOptMeta, hasRetractableInput);
        } else if (joinType.isRightOuterJoin()) {
            // RIGHT OUTER JOIN: swap left and right, then apply left outer join logic
            deltaJoin = transformRightOuterJoin(context, input, join, joinOutputColRefs,
                    leftOptMeta, rightOptMeta, hasRetractableInput);
        } else if (joinType.isFullOuterJoin()) {
            // FULL OUTER JOIN
            deltaJoin = transformFullOuterJoin(context, input, join, joinOutputColRefs,
                    leftOptMeta, rightOptMeta, hasRetractableInput);
        } else {
            throw new IllegalStateException("Unsupported join type for TVR: " + joinType + " in " + input);
        }

        return Lists.newArrayList(deltaJoin);
    }

    private TvrOptMeta buildJoinTvrOptMeta(OptimizerContext context,
                                           LogicalJoinOperator join,
                                           TvrOptMeta leftOptMeta,
                                           TvrOptMeta rightOptMeta,
                                           List<ColumnRefOperator> joinOutputColRefs,
                                           TvrTableDeltaTrait joinDeltaTrait) {
        TvrOptExpression tvrLeftFrom = leftOptMeta.getFrom();
        TvrOptExpression tvrLeftTo = leftOptMeta.getTo();
        TvrOptExpression tvrRightFrom = rightOptMeta.getFrom();
        TvrOptExpression tvrRightTo = rightOptMeta.getTo();
        // from opt
        TvrLazyOptExpression fromJoin = TvrLazyOptExpression.of(() -> {
            LogicalJoinOperator newJoinOp = buildNewJoinOperator(join);
            OptExpressionWithOutput fromOpt = buildJoinOptExpression(context, joinOutputColRefs,
                    newJoinOp, tvrLeftFrom.optExpression(), tvrRightFrom.optExpression(), false);
            return new TvrOptExpression(tvrLeftFrom.tvrVersionRange(), fromOpt.optExpression());
        });
        // to opt
        TvrLazyOptExpression toJoin = TvrLazyOptExpression.of(() -> {
            LogicalJoinOperator newJoinOp = buildNewJoinOperator(join);
            OptExpressionWithOutput toOpt = buildJoinOptExpression(context, joinOutputColRefs,
                    newJoinOp, tvrLeftTo.optExpression(), tvrRightTo.optExpression(), false);
            return new TvrOptExpression(tvrLeftTo.tvrVersionRange(), toOpt.optExpression());
        });
        // root opt group
        return new TvrOptMeta(joinDeltaTrait, fromJoin, toJoin);
    }

    /**
     * Build the common join delta for INNER JOIN:
     * Δ(A ⋈ B) = (A_from ⋈ ΔB) ∪ (ΔA ⋈ B_to)
     *
     * For retractable inputs, the op column is propagated:
     * - (a, OP_INSERT) ⋈ b → (a⋈b, OP_INSERT)
     * - (a, OP_DELETE) ⋈ b → (a⋈b, OP_DELETE)
     */
    private List<OptExpressionWithOutput> buildCommonJoinDelta(OptimizerContext context,
                                                               LogicalJoinOperator join,
                                                               List<ColumnRefOperator> joinOutputColRefs,
                                                               TvrOptExpression tvrLeftFrom,
                                                               OptExpression rightDelta,
                                                               TvrOptExpression tvrRightTo,
                                                               OptExpression leftDelta) {
        // Branch 1: A_from ⋈ ΔB (left snapshot join with right delta)
        OptExpressionWithOutput deltaOutput1 =
                buildJoinOptExpression(context, joinOutputColRefs, join, tvrLeftFrom.optExpression(),
                        rightDelta, true);
        // Branch 2: ΔA ⋈ B_to (left delta join with right snapshot)
        OptExpressionWithOutput deltaOutput2 =
                buildJoinOptExpression(context, joinOutputColRefs, join, leftDelta,
                        tvrRightTo.optExpression(), true);
        return Lists.newArrayList(deltaOutput1, deltaOutput2);
    }

    /**
     * Transform INNER JOIN / CROSS JOIN for incremental computation.
     *
     * Formula: Δ(A ⋈ B) = (A_from ⋈ ΔB) ∪ (ΔA ⋈ B_to)
     *
     * For retractable inputs, the StreamRowOp is propagated through the join.
     */
    private OptExpression transformInnerJoin(OptimizerContext context,
                                              OptExpression input,
                                              LogicalJoinOperator join,
                                              List<ColumnRefOperator> joinOutputColRefs,
                                              TvrOptMeta leftOptMeta,
                                              TvrOptMeta rightOptMeta,
                                              boolean hasRetractableInput) {
        TvrOptExpression tvrLeftFrom = leftOptMeta.getFrom();
        TvrOptExpression tvrRightTo = rightOptMeta.getTo();
        OptExpression leftDelta = input.inputAt(0);
        OptExpression rightDelta = input.inputAt(1);

        // Build the delta branches
        List<OptExpressionWithOutput> commonJoinDelta =
                buildCommonJoinDelta(context, join, joinOutputColRefs, tvrLeftFrom, rightDelta, tvrRightTo, leftDelta);

        // Compute the output trait - merge left and right traits
        TvrTableDeltaTrait outputTrait = TvrTableDeltaTrait.merge(
                leftOptMeta.tvrDeltaTrait(), rightOptMeta.tvrDeltaTrait());

        // Build the join TvrOptMeta
        TvrOptMeta joinTvrOptMeta = buildJoinTvrOptMeta(context, join, leftOptMeta, rightOptMeta,
                joinOutputColRefs, outputTrait);

        // Return a union operator to merge the delta branches
        return buildUnionOperator(joinTvrOptMeta, joinOutputColRefs, commonJoinDelta);
    }

    /**
     * Transform LEFT OUTER JOIN for incremental computation.
     *
     * For append-only inputs:
     *   Δ(A ⟕ B) = (ΔA ⟕ B_to) ∪ (A_from ⋈ ΔB)
     *
     * For retractable inputs or when NULL rows may change:
     *   Additional branches are needed to handle:
     *   - Right INSERT making previously unmatched left rows now matched (retract NULL row, insert matched row)
     *   - Right DELETE making previously matched left rows now unmatched (retract matched row, insert NULL row)
     *
     * TODO: Implement full NULL row change tracking with join state table.
     */
    private OptExpression transformLeftOuterJoin(OptimizerContext context,
                                                  OptExpression input,
                                                  LogicalJoinOperator join,
                                                  List<ColumnRefOperator> joinOutputColRefs,
                                                  TvrOptMeta leftOptMeta,
                                                  TvrOptMeta rightOptMeta,
                                                  boolean hasRetractableInput) {
        TvrOptExpression tvrLeftFrom = leftOptMeta.getFrom();
        TvrOptExpression tvrRightTo = rightOptMeta.getTo();
        OptExpression leftDelta = input.inputAt(0);
        OptExpression rightDelta = input.inputAt(1);

        List<OptExpressionWithOutput> deltaBranches = Lists.newArrayList();

        // Branch 1: ΔA ⟕ B_to (new left rows do left outer join with right snapshot)
        LogicalJoinOperator leftOuterJoin = buildNewJoinOperator(join);
        OptExpressionWithOutput branch1 = buildJoinOptExpression(context, joinOutputColRefs,
                leftOuterJoin, leftDelta, tvrRightTo.optExpression(), true);
        deltaBranches.add(branch1);

        // Branch 2: A_from ⋈ ΔB (left snapshot inner join with right delta - matched rows)
        LogicalJoinOperator innerJoin = new LogicalJoinOperator.Builder()
                .withOperator(join)
                .setJoinType(JoinOperator.INNER_JOIN)
                .build();
        OptExpressionWithOutput branch2 = buildJoinOptExpression(context, joinOutputColRefs,
                innerJoin, tvrLeftFrom.optExpression(), rightDelta, true);
        deltaBranches.add(branch2);

        // Branch 3 & 4: NULL row changes when right side has INSERT/DELETE
        // These detect transitions between matched and unmatched states
        TvrOptExpression tvrRightFrom = rightOptMeta.getFrom();
        List<OptExpressionWithOutput> nullRowChangeBranches = buildNullRowChangesForLeftOuterJoin(
                context, join, joinOutputColRefs,
                tvrLeftFrom, tvrRightFrom, tvrRightTo, rightDelta);
        deltaBranches.addAll(nullRowChangeBranches);

        // LEFT OUTER JOIN output is always retractable because NULL rows may be retracted
        TvrTableDeltaTrait outputTrait = TvrTableDeltaTrait.merge(
                leftOptMeta.tvrDeltaTrait(), rightOptMeta.tvrDeltaTrait())
                .withOuterJoinRetractable();

        TvrOptMeta joinTvrOptMeta = buildJoinTvrOptMeta(context, join, leftOptMeta, rightOptMeta,
                joinOutputColRefs, outputTrait);

        return buildUnionOperator(joinTvrOptMeta, joinOutputColRefs, deltaBranches);
    }

    /**
     * Transform RIGHT OUTER JOIN for incremental computation.
     * This is symmetric to LEFT OUTER JOIN with left and right swapped.
     */
    private OptExpression transformRightOuterJoin(OptimizerContext context,
                                                   OptExpression input,
                                                   LogicalJoinOperator join,
                                                   List<ColumnRefOperator> joinOutputColRefs,
                                                   TvrOptMeta leftOptMeta,
                                                   TvrOptMeta rightOptMeta,
                                                   boolean hasRetractableInput) {
        TvrOptExpression tvrLeftTo = leftOptMeta.getTo();
        TvrOptExpression tvrRightFrom = rightOptMeta.getFrom();
        OptExpression leftDelta = input.inputAt(0);
        OptExpression rightDelta = input.inputAt(1);

        List<OptExpressionWithOutput> deltaBranches = Lists.newArrayList();

        // Branch 1: A_to ⟖ ΔB (left snapshot right outer join with right delta)
        LogicalJoinOperator rightOuterJoin = buildNewJoinOperator(join);
        OptExpressionWithOutput branch1 = buildJoinOptExpression(context, joinOutputColRefs,
                rightOuterJoin, tvrLeftTo.optExpression(), rightDelta, true);
        deltaBranches.add(branch1);

        // Branch 2: ΔA ⋈ B_from (left delta inner join with right snapshot - matched rows)
        LogicalJoinOperator innerJoin = new LogicalJoinOperator.Builder()
                .withOperator(join)
                .setJoinType(JoinOperator.INNER_JOIN)
                .build();
        OptExpressionWithOutput branch2 = buildJoinOptExpression(context, joinOutputColRefs,
                innerJoin, leftDelta, tvrRightFrom.optExpression(), true);
        deltaBranches.add(branch2);

        // Branch 3 & 4: NULL row changes when left side has INSERT/DELETE
        // Symmetric to LEFT OUTER JOIN, but tracking right side's NULL rows
        TvrOptExpression tvrLeftFrom = leftOptMeta.getFrom();
        List<OptExpressionWithOutput> nullRowChangeBranches = buildNullRowChangesForRightOuterJoin(
                context, join, joinOutputColRefs,
                tvrLeftFrom, tvrLeftTo, tvrRightFrom, leftDelta);
        deltaBranches.addAll(nullRowChangeBranches);

        // RIGHT OUTER JOIN output is always retractable
        TvrTableDeltaTrait outputTrait = TvrTableDeltaTrait.merge(
                leftOptMeta.tvrDeltaTrait(), rightOptMeta.tvrDeltaTrait())
                .withOuterJoinRetractable();

        TvrOptMeta joinTvrOptMeta = buildJoinTvrOptMeta(context, join, leftOptMeta, rightOptMeta,
                joinOutputColRefs, outputTrait);

        return buildUnionOperator(joinTvrOptMeta, joinOutputColRefs, deltaBranches);
    }

    /**
     * Transform FULL OUTER JOIN for incremental computation.
     * This combines both LEFT and RIGHT OUTER JOIN logic.
     */
    private OptExpression transformFullOuterJoin(OptimizerContext context,
                                                  OptExpression input,
                                                  LogicalJoinOperator join,
                                                  List<ColumnRefOperator> joinOutputColRefs,
                                                  TvrOptMeta leftOptMeta,
                                                  TvrOptMeta rightOptMeta,
                                                  boolean hasRetractableInput) {
        TvrOptExpression tvrLeftFrom = leftOptMeta.getFrom();
        TvrOptExpression tvrLeftTo = leftOptMeta.getTo();
        TvrOptExpression tvrRightFrom = rightOptMeta.getFrom();
        TvrOptExpression tvrRightTo = rightOptMeta.getTo();
        OptExpression leftDelta = input.inputAt(0);
        OptExpression rightDelta = input.inputAt(1);

        List<OptExpressionWithOutput> deltaBranches = Lists.newArrayList();

        // Branch 1: ΔA ⟗ B_to (left delta full outer join with right snapshot)
        LogicalJoinOperator fullOuterJoin = buildNewJoinOperator(join);
        OptExpressionWithOutput branch1 = buildJoinOptExpression(context, joinOutputColRefs,
                fullOuterJoin, leftDelta, tvrRightTo.optExpression(), true);
        deltaBranches.add(branch1);

        // Branch 2: A_from ⟖ ΔB (left snapshot right outer join with right delta)
        // This captures new right rows and their matches/non-matches
        LogicalJoinOperator rightOuterJoin = new LogicalJoinOperator.Builder()
                .withOperator(join)
                .setJoinType(JoinOperator.RIGHT_OUTER_JOIN)
                .build();
        OptExpressionWithOutput branch2 = buildJoinOptExpression(context, joinOutputColRefs,
                rightOuterJoin, tvrLeftFrom.optExpression(), rightDelta, true);
        deltaBranches.add(branch2);

        // Branch 3-6: NULL row changes from both sides
        // FULL OUTER JOIN needs to detect NULL row changes on both left and right
        // - Left side NULL changes (when right side changes): same as LEFT OUTER JOIN
        // - Right side NULL changes (when left side changes): same as RIGHT OUTER JOIN
        List<OptExpressionWithOutput> leftNullChanges = buildNullRowChangesForLeftOuterJoin(
                context, join, joinOutputColRefs,
                tvrLeftFrom, tvrRightFrom, tvrRightTo, rightDelta);
        deltaBranches.addAll(leftNullChanges);

        List<OptExpressionWithOutput> rightNullChanges = buildNullRowChangesForRightOuterJoin(
                context, join, joinOutputColRefs,
                tvrLeftFrom, tvrLeftTo, tvrRightFrom, leftDelta);
        deltaBranches.addAll(rightNullChanges);

        // FULL OUTER JOIN output is always retractable
        TvrTableDeltaTrait outputTrait = TvrTableDeltaTrait.merge(
                leftOptMeta.tvrDeltaTrait(), rightOptMeta.tvrDeltaTrait())
                .withOuterJoinRetractable();

        TvrOptMeta joinTvrOptMeta = buildJoinTvrOptMeta(context, join, leftOptMeta, rightOptMeta,
                joinOutputColRefs, outputTrait);

        return buildUnionOperator(joinTvrOptMeta, joinOutputColRefs, deltaBranches);
    }

    /**
     * Build branches to detect NULL row changes for LEFT OUTER JOIN.
     *
     * For LEFT OUTER JOIN A ⟕ B, NULL row changes occur when:
     * 1. Right INSERT causes unmatched→matched: a left row had no match, now has match
     *    - Detection: (A_from ANTI JOIN B_from) SEMI JOIN ΔB_insert
     *    - Output: DELETE(left+NULL), INSERT(left+right)
     *
     * 2. Right DELETE causes matched→unmatched: a left row had match, now has no match
     *    - Detection: (A_from SEMI JOIN ΔB_delete) ANTI JOIN B_to
     *    - Output: DELETE(left+right), INSERT(left+NULL)
     *
     * @param context optimizer context
     * @param join the original join operator
     * @param joinOutputColRefs output column references
     * @param tvrLeftFrom left snapshot at time t_from
     * @param tvrRightFrom right snapshot at time t_from
     * @param tvrRightTo right snapshot at time t_to
     * @param rightDelta delta of right side (contains both inserts and deletes with ops)
     * @return list of OptExpressionWithOutput for NULL row change branches
     */
    private List<OptExpressionWithOutput> buildNullRowChangesForLeftOuterJoin(
            OptimizerContext context,
            LogicalJoinOperator join,
            List<ColumnRefOperator> joinOutputColRefs,
            TvrOptExpression tvrLeftFrom,
            TvrOptExpression tvrRightFrom,
            TvrOptExpression tvrRightTo,
            OptExpression rightDelta) {

        List<OptExpressionWithOutput> nullRowChanges = Lists.newArrayList();

        // Get the join predicate (on-clause)
        // ScalarOperator joinPredicate = join.getOnPredicate();

        // Branch 3: Detect unmatched→matched transition (right INSERT)
        // Step 1: A_from ANTI JOIN B_from (left rows without any match before)
        // Step 2: The result SEMI JOIN ΔB (left rows that now have a match due to new right rows)
        // The final output should be LEFT OUTER JOIN to get the actual matched row
        //
        // Simplified approach: (A_from ANTI JOIN B_from) LEFT OUTER JOIN ΔB
        // This gives us left rows that:
        //   - Had no match before (ANTI JOIN B_from)
        //   - Now may or may not match ΔB (LEFT OUTER JOIN ΔB)
        // We then need only the matched ones, so we use INNER JOIN instead

        // Build: (A_from ANTI JOIN B_from) INNER JOIN ΔB
        // This finds left rows that had no match but now match the delta

        // Step 3a: A_from ANTI JOIN B_from
        LogicalJoinOperator antiJoin = new LogicalJoinOperator.Builder()
                .withOperator(join)
                .setJoinType(JoinOperator.LEFT_ANTI_JOIN)
                .build();
        OptExpression antiJoinExpr = OptExpression.create(antiJoin,
                tvrLeftFrom.optExpression(), tvrRightFrom.optExpression());

        // Step 3b: (A_from ANTI JOIN B_from) INNER JOIN ΔB
        // This gives us the newly matched rows - these need DELETE(null-row) + INSERT(matched-row)
        // Since the TvrRule framework handles ops propagation, we output this as the matched result
        // The BE layer will handle the ops based on the delta ops
        LogicalJoinOperator innerJoinWithDelta = new LogicalJoinOperator.Builder()
                .withOperator(join)
                .setJoinType(JoinOperator.INNER_JOIN)
                .build();
        OptExpressionWithOutput branch3 = buildJoinOptExpression(context, joinOutputColRefs,
                innerJoinWithDelta, antiJoinExpr, rightDelta, true);
        nullRowChanges.add(branch3);

        // Branch 4: Detect matched→unmatched transition (right DELETE)
        // Step 1: A_from SEMI JOIN ΔB (left rows that matched some deleted right rows)
        // Step 2: The result ANTI JOIN B_to (left rows that now have no match)
        //
        // Note: This requires knowing which delta rows are DELETEs.
        // In the TVR framework, the delta contains rows with ops, but we need to filter by op.
        // For now, we build the plan structure; the BE layer handles op filtering.

        // Build: (A_from SEMI JOIN ΔB) ANTI JOIN B_to
        // But for correct output, we want the NULL-padded output for unmatched rows
        // So we should build: (A_from SEMI JOIN ΔB) LEFT OUTER JOIN B_to, filter non-matched

        // Simplified: (A_from INNER JOIN ΔB) ANTI JOIN B_to
        // Then LEFT OUTER JOIN with NULLs on the right
        // This is complex; let's use a pragmatic approach:
        //
        // For the DELETE case, the result should be rows that:
        // - Were matched before (A_from INNER JOIN old_B_rows)
        // - Are unmatched now (ANTI JOIN B_to)
        //
        // Step 4a: A_from INNER JOIN ΔB (rows that were matched by the delta rows)
        LogicalJoinOperator innerJoinDelta = new LogicalJoinOperator.Builder()
                .withOperator(join)
                .setJoinType(JoinOperator.INNER_JOIN)
                .build();
        OptExpression innerJoinDeltaExpr = OptExpression.create(innerJoinDelta,
                tvrLeftFrom.optExpression(), rightDelta);

        // Step 4b: Extract just the left columns from the inner join result
        // Then ANTI JOIN with B_to to find rows that now have no match
        // For simplicity, we'll use: A_from SEMI JOIN ΔB (get affected left rows)
        LogicalJoinOperator semiJoinDelta = new LogicalJoinOperator.Builder()
                .withOperator(join)
                .setJoinType(JoinOperator.LEFT_SEMI_JOIN)
                .build();
        OptExpression semiJoinDeltaExpr = OptExpression.create(semiJoinDelta,
                tvrLeftFrom.optExpression(), rightDelta);

        // Step 4c: (A_from SEMI JOIN ΔB) ANTI JOIN B_to
        // This gives us left rows that matched deleted rows and now have no match
        LogicalJoinOperator antiJoinTo = new LogicalJoinOperator.Builder()
                .withOperator(join)
                .setJoinType(JoinOperator.LEFT_ANTI_JOIN)
                .build();

        // Build the ANTI JOIN first to get left-side-only rows
        OptExpression antiJoinToExpr = OptExpression.create(antiJoinTo,
                semiJoinDeltaExpr, tvrRightTo.optExpression());

        // For ANTI JOIN output, we need to pad with NULLs for LEFT OUTER JOIN semantics
        // The output schema should match the original LEFT OUTER JOIN output
        // Use LEFT OUTER JOIN with an impossible predicate to produce NULL-padded output
        // Alternative: Use a dedicated NULL-padding projection operator
        //
        // Pragmatic approach: LEFT OUTER JOIN (ANTI result) with B_to using FALSE predicate
        // This effectively outputs (left columns, NULL, NULL, ...) for all rows
        LogicalJoinOperator nullPadJoin = new LogicalJoinOperator.Builder()
                .withOperator(join)
                .setJoinType(JoinOperator.LEFT_OUTER_JOIN)
                .setOnPredicate(ConstantOperator.FALSE)  // Never matches, always outputs NULLs
                .build();
        OptExpressionWithOutput branch4 = buildJoinOptExpression(context, joinOutputColRefs,
                nullPadJoin, antiJoinToExpr, tvrRightTo.optExpression(), true);
        nullRowChanges.add(branch4);

        return nullRowChanges;
    }

    /**
     * Build branches to detect NULL row changes for RIGHT OUTER JOIN.
     * Symmetric to LEFT OUTER JOIN but with left and right swapped.
     *
     * For RIGHT OUTER JOIN A ⟖ B, NULL row changes occur when:
     * 1. Left INSERT causes unmatched→matched: a right row had no match, now has match
     *    - Detection: (B_from ANTI JOIN A_from) SEMI JOIN ΔA_insert
     *    - Output: DELETE(NULL+right), INSERT(left+right)
     *
     * 2. Left DELETE causes matched→unmatched: a right row had match, now has no match
     *    - Detection: (B_from SEMI JOIN ΔA_delete) ANTI JOIN A_to
     *    - Output: DELETE(left+right), INSERT(NULL+right)
     */
    private List<OptExpressionWithOutput> buildNullRowChangesForRightOuterJoin(
            OptimizerContext context,
            LogicalJoinOperator join,
            List<ColumnRefOperator> joinOutputColRefs,
            TvrOptExpression tvrLeftFrom,
            TvrOptExpression tvrLeftTo,
            TvrOptExpression tvrRightFrom,
            OptExpression leftDelta) {

        List<OptExpressionWithOutput> nullRowChanges = Lists.newArrayList();

        // Branch 3: Detect unmatched→matched transition (left INSERT)
        // Build: (B_from ANTI JOIN A_from) INNER JOIN ΔA
        // This finds right rows that had no match but now match the delta

        // Step 3a: B_from RIGHT ANTI JOIN A_from (right rows without any match before)
        // Note: We use RIGHT ANTI JOIN which is equivalent to swapping and using LEFT ANTI JOIN
        LogicalJoinOperator antiJoin = new LogicalJoinOperator.Builder()
                .withOperator(join)
                .setJoinType(JoinOperator.RIGHT_ANTI_JOIN)
                .build();
        OptExpression antiJoinExpr = OptExpression.create(antiJoin,
                tvrLeftFrom.optExpression(), tvrRightFrom.optExpression());

        // Step 3b: ΔA INNER JOIN (B_from ANTI JOIN A_from)
        // Swap order for RIGHT join semantics
        LogicalJoinOperator innerJoinWithDelta = new LogicalJoinOperator.Builder()
                .withOperator(join)
                .setJoinType(JoinOperator.INNER_JOIN)
                .build();
        OptExpressionWithOutput branch3 = buildJoinOptExpression(context, joinOutputColRefs,
                innerJoinWithDelta, leftDelta, antiJoinExpr, true);
        nullRowChanges.add(branch3);

        // Branch 4: Detect matched→unmatched transition (left DELETE)
        // Build: (B_from SEMI JOIN ΔA) ANTI JOIN A_to

        // Step 4a: B_from RIGHT SEMI JOIN ΔA (right rows that matched some deleted left rows)
        LogicalJoinOperator semiJoinDelta = new LogicalJoinOperator.Builder()
                .withOperator(join)
                .setJoinType(JoinOperator.RIGHT_SEMI_JOIN)
                .build();
        OptExpression semiJoinDeltaExpr = OptExpression.create(semiJoinDelta,
                leftDelta, tvrRightFrom.optExpression());

        // Step 4b: (Result from 4a) RIGHT ANTI JOIN A_to
        // Right rows that now have no match
        LogicalJoinOperator antiJoinTo = new LogicalJoinOperator.Builder()
                .withOperator(join)
                .setJoinType(JoinOperator.RIGHT_ANTI_JOIN)
                .build();
        OptExpression antiJoinToExpr = OptExpression.create(antiJoinTo,
                tvrLeftTo.optExpression(), semiJoinDeltaExpr);

        // NULL-pad the left side for unmatched right rows
        LogicalJoinOperator nullPadJoin = new LogicalJoinOperator.Builder()
                .withOperator(join)
                .setJoinType(JoinOperator.RIGHT_OUTER_JOIN)
                .setOnPredicate(ConstantOperator.FALSE)  // Never matches, always outputs NULLs
                .build();
        OptExpressionWithOutput branch4 = buildJoinOptExpression(context, joinOutputColRefs,
                nullPadJoin, tvrLeftTo.optExpression(), antiJoinToExpr, true);
        nullRowChanges.add(branch4);

        return nullRowChanges;
    }
}
