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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.DataSkewInfo;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.ReduceCastRule;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GroupByCountDistinctDataSkewEliminateRule extends TransformationRule {
    private GroupByCountDistinctDataSkewEliminateRule() {
        super(RuleType.TF_GROUP_BY_COUNT_DISTINCT_DATA_SKEW_ELIMINATE_RULE, Pattern.create(OperatorType.LOGICAL_AGGR,
                OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggOp = input.getOp().cast();
        return aggOp.checkGroupByCountDistinctWithSkewHint() || (aggOp.checkGroupByCountDistinct() &&
                context.getSessionVariable().isEnableDistinctColumnBucketization());
    }

    private static final GroupByCountDistinctDataSkewEliminateRule INSTANCE =
            new GroupByCountDistinctDataSkewEliminateRule();

    public static GroupByCountDistinctDataSkewEliminateRule getInstance() {
        return INSTANCE;
    }

    // compute the type of bucket column, since bucket column introduce extra cost, so we
    // choose just wide enough type to keep bucket number.
    private Type pickBucketType(int bucketNum) {
        Preconditions.checkArgument(0 < bucketNum && bucketNum <= 65536);
        return (bucketNum <= 256) ? Type.TINYINT : Type.SMALLINT;
    }

    private ScalarOperator createBucketColumn(ColumnRefOperator distinctColumn, int bucketNum) {
        CastOperator stringCol = new CastOperator(Type.VARCHAR, distinctColumn, true);
        Function hashFunc = Expr.getBuiltinFunction(FunctionSet.MURMUR_HASH3_32, new Type[] {Type.VARCHAR},
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Type type = pickBucketType(bucketNum);
        CallOperator callHashFuncOp =
                new CallOperator(FunctionSet.MURMUR_HASH3_32, hashFunc.getReturnType(),
                        Collections.singletonList(stringCol),
                        hashFunc);

        // sign of remainder keeps consistency with that of dividend, so divisor should be half of bucketNum.
        // for an example, when bucketNum=8; the remainder will be
        // -3, -2, -1, 0, 1, 2, 3, 4, NULL
        ConstantOperator bucketConstOp = new ConstantOperator(bucketNum / 2, Type.INT);
        CallOperator modBucketOp =
                new CallOperator(FunctionSet.MOD, Type.INT, Arrays.asList(callHashFuncOp, bucketConstOp));
        ScalarOperator op = new CastOperator(type, modBucketOp, true);
        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        return rewriter.rewrite(op, Collections.singletonList(new ReduceCastRule()));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggOp = input.getOp().cast();
        OptExpression child = input.inputAt(0);

        Map.Entry<ColumnRefOperator, CallOperator> aggregate = aggOp.getAggregations().entrySet().iterator().next();
        ColumnRefOperator aggColRef = aggregate.getKey();
        CallOperator aggCall = aggregate.getValue();

        List<ColumnRefOperator> groupBy = aggOp.getGroupingKeys();
        ColumnRefOperator distinctColRef = aggCall.getChild(0).cast();

        // create auxiliary group-by column:
        // cast(murmur_hash3_32(cast(distinct_column as varchar))%bucketNum as NarrowInteger)
        int bucketNum = context.getSessionVariable().getDistinctColumnBuckets();
        // bucketNum ranges from [4, 65536]
        bucketNum = Math.max(4, Math.min(65536, bucketNum));
        ScalarOperator bucketCol = createBucketColumn(distinctColRef, bucketNum);
        ColumnRefOperator bucketColRef =
                context.getColumnRefFactory().create(bucketCol, bucketCol.getType(), distinctColRef.isNullable());

        // first-stage: deduplicate (groupByColumn, distinctColumn)
        List<ColumnRefOperator> firstGroupBy = Lists.newArrayList(groupBy);
        firstGroupBy.add(distinctColRef);
        LogicalAggregationOperator firstAggOp =
                new LogicalAggregationOperator(AggType.LOCAL, firstGroupBy, Maps.newHashMap());

        Map<ColumnRefOperator, ScalarOperator> colRefMap = Maps.newHashMap();
        firstGroupBy.forEach(ref -> colRefMap.put(ref, ref));
        colRefMap.put(bucketColRef, bucketCol);
        // projection(groupByColumn, distinctColumn, hash(distinctColumn)%numBuckets as bucket)
        firstAggOp.setProjection(new Projection(colRefMap));
        int stage = 0;
        // AggSplitRule splits group-by-count-distinct query into 3-stage Agg operators + 1 shuffle, while
        // this Rule splits the query into 4-stage agg operators + 2 shuffle, in good cases of this rule, one extra
        // Agg operator and one extra shuffle operator always make the plan generated by this rule yields a
        // large cost than the plan generated by AggSplitRule, so for the plan generated by this rule:
        // 1. stage-3 and stage-4 operators: have a very low real cost that is negligible, so factor=0.5 are
        // assigned to these two Agg operators that are treated as one agg operators. the shuffle operator between
        // bridged stage-3 and stage-4 also should be neglected.
        // 2. stage-1 operators: would spend significant real cost, so we assigned 1.0.
        // 3. In CostModel: we reward stage-2 operators in good cases and punish bad cases.
        firstAggOp.setDistinctColumnDataSkew(new DataSkewInfo(distinctColRef, 1.0, ++stage));

        // second-stage:
        // select groupByColumn, bucket, multi_distinct_count(distinctColumn) as countPerBucket
        // from t group by groupByColumn, bucket
        List<ColumnRefOperator> secondGroupBy = Lists.newArrayList(groupBy);
        secondGroupBy.add(bucketColRef);
        Map<ColumnRefOperator, CallOperator> secondStageAggregations = Maps.newHashMap();
        CallOperator multiDistinctCountAgg = ScalarOperatorUtil.buildMultiCountDistinct(aggCall);
        secondStageAggregations.put(aggColRef, multiDistinctCountAgg);

        LogicalAggregationOperator secondAggOp =
                new LogicalAggregationOperator(AggType.GLOBAL, secondGroupBy, secondStageAggregations);
        secondAggOp.setDistinctColumnDataSkew(new DataSkewInfo(distinctColRef, 1.0, ++stage));
        secondAggOp.setPartitionByColumns(secondGroupBy);

        // third-stage/fourth Agg: select groupByColumn, sum(countPerBucket) from t group by groupByColumn
        Map<ColumnRefOperator, CallOperator> thirdAggregations = Maps.newHashMap();
        CallOperator sum = ScalarOperatorUtil.buildSum(aggColRef);
        thirdAggregations.put(aggColRef, sum);
        LogicalAggregationOperator thirdAggOp =
                new LogicalAggregationOperator(AggType.LOCAL, Lists.newArrayList(groupBy), thirdAggregations);
        thirdAggOp.setDistinctColumnDataSkew(new DataSkewInfo(distinctColRef, 0.5, ++stage));

        // create fourth-stage Agg operator
        LogicalAggregationOperator fourthAggOp = LogicalAggregationOperator.builder().withOperator(thirdAggOp)
                .setType(AggType.GLOBAL)
                .setSplit()
                .setPartitionByColumns(Lists.newArrayList(groupBy))
                .setProjection(aggOp.getProjection())
                .build();
        fourthAggOp.setDistinctColumnDataSkew(new DataSkewInfo(distinctColRef, 0.5, ++stage));

        OptExpression optExpression = child;
        optExpression = OptExpression.create(firstAggOp, optExpression);
        optExpression = OptExpression.create(secondAggOp, optExpression);
        optExpression = OptExpression.create(thirdAggOp, optExpression);
        optExpression = OptExpression.create(fourthAggOp, optExpression);
        return Lists.newArrayList(optExpression);
    }
}
