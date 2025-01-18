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

package com.starrocks.sql.optimizer.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.rule.implementation.AssertOneRowImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.CTEAnchorImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.CTEAnchorToNoCTEImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.CTEConsumeInlineImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.CTEConsumerReuseImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.CTEProduceImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.DeltaLakeScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.EsScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.ExceptImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.FileScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.FilterImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.HashAggImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.HashJoinImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.HiveScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.HudiScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.IcebergEqualityDeleteScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.IcebergMetadataScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.IcebergScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.IntersectImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.JDBCScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.KuduScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.LimitImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.MergeJoinImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.MetaScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.MysqlScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.NestLoopJoinImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.OdpsScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.OlapScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.PaimonScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.ProjectImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.RepeatImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.SchemaScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.TableFunctionImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.TableFunctionTableScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.TopNImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.UnionImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.ValuesImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.WindowImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.stream.StreamAggregateImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.stream.StreamJoinImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.stream.StreamScanImplementationRule;
import com.starrocks.sql.optimizer.rule.transformation.CastToEmptyRule;
import com.starrocks.sql.optimizer.rule.transformation.CollectCTEConsumeRule;
import com.starrocks.sql.optimizer.rule.transformation.CollectCTEProduceRule;
import com.starrocks.sql.optimizer.rule.transformation.CombinationRule;
import com.starrocks.sql.optimizer.rule.transformation.DistributionPruneRule;
import com.starrocks.sql.optimizer.rule.transformation.EliminateGroupByConstantRule;
import com.starrocks.sql.optimizer.rule.transformation.EliminateJoinWithConstantRule;
import com.starrocks.sql.optimizer.rule.transformation.EliminateLimitZeroRule;
import com.starrocks.sql.optimizer.rule.transformation.ExistentialApply2JoinRule;
import com.starrocks.sql.optimizer.rule.transformation.ExistentialApply2OuterJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.ExternalScanPartitionPruneRule;
import com.starrocks.sql.optimizer.rule.transformation.FineGrainedRangePredicateRule;
import com.starrocks.sql.optimizer.rule.transformation.GroupByCountDistinctDataSkewEliminateRule;
import com.starrocks.sql.optimizer.rule.transformation.InlineOneCTEConsumeRule;
import com.starrocks.sql.optimizer.rule.transformation.IntersectAddDistinctRule;
import com.starrocks.sql.optimizer.rule.transformation.JoinAssociativityRule;
import com.starrocks.sql.optimizer.rule.transformation.JoinCommutativityRule;
import com.starrocks.sql.optimizer.rule.transformation.JoinCommutativityWithoutInnerRule;
import com.starrocks.sql.optimizer.rule.transformation.JoinLeftAsscomRule;
import com.starrocks.sql.optimizer.rule.transformation.LimitPruneTabletsRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeApplyWithTableFunction;
import com.starrocks.sql.optimizer.rule.transformation.MergeLimitDirectRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeLimitWithLimitRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeLimitWithSortRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoFiltersRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.MinMaxCountOptOnScanRule;
import com.starrocks.sql.optimizer.rule.transformation.PartitionPruneRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneAggregateColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneAssertOneRowRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneCTEConsumeColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneCTEProduceRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneEmptyDirectRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneEmptyExceptRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneEmptyIntersectRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneEmptyJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneEmptyScanRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneEmptyUnionRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneEmptyWindowRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneExceptColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneFilterColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneGroupByKeysRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneHDFSScanColumnRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneIntersectColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneJoinColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneProjectColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneProjectEmptyRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneRepeatColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneScanColumnRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneTableFunctionColumnRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneTopNColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneTrueFilterRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneUKFKGroupByKeysRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneUKFKJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneUnionColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneValuesColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneWindowColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownAggToMetaScanRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownApplyAggFilterRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownApplyAggProjectFilterRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownApplyFilterRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownApplyLeftProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownApplyLeftRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownApplyProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownFlatJsonMetaToMetaScanRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownJoinOnClauseRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownLimitCTEAnchor;
import com.starrocks.sql.optimizer.rule.transformation.PushDownLimitDirectRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownLimitJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownLimitUnionRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateAggRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateCTEAnchor;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateCTEConsumeRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateExceptRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateIntersectRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateRepeatRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateScanRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateTableFunctionRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateToExternalTableScanRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateUnionRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateWindowRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownProjectLimitRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownProjectToCTEAnchorRule;
import com.starrocks.sql.optimizer.rule.transformation.QuantifiedApply2JoinRule;
import com.starrocks.sql.optimizer.rule.transformation.QuantifiedApply2OuterJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.ReorderIntersectRule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteBitmapCountDistinctRule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteCountIfFunction;
import com.starrocks.sql.optimizer.rule.transformation.RewriteDuplicateAggregateFnRule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteHllCountDistinctRule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteSimpleAggToHDFSScanRule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteSimpleAggToMetaScanRule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteSumByAssociativeRule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteToVectorPlanRule;
import com.starrocks.sql.optimizer.rule.transformation.ScalarApply2AnalyticRule;
import com.starrocks.sql.optimizer.rule.transformation.ScalarApply2JoinRule;
import com.starrocks.sql.optimizer.rule.transformation.SplitLimitRule;
import com.starrocks.sql.optimizer.rule.transformation.SplitMultiPhaseAggRule;
import com.starrocks.sql.optimizer.rule.transformation.SplitTopNRule;
import com.starrocks.sql.optimizer.rule.transformation.SplitTwoPhaseAggRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.AggregateJoinPushDownRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.AggregateJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.AggregateScanRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.AggregateTimeSeriesRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.OnlyJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.OnlyScanRule;
import com.starrocks.sql.optimizer.rule.transformation.pruner.CboTablePruneRule;

import java.util.List;

public class RuleSet {
    private static final List<Rule> ALL_IMPLEMENT_RULES = ImmutableList.of(
            new OlapScanImplementationRule(),
            new HiveScanImplementationRule(),
            new FileScanImplementationRule(),
            new IcebergScanImplementationRule(),
            new IcebergEqualityDeleteScanImplementationRule(),
            new HudiScanImplementationRule(),
            new DeltaLakeScanImplementationRule(),
            new PaimonScanImplementationRule(),
            new OdpsScanImplementationRule(),
            new IcebergMetadataScanImplementationRule(),
            new KuduScanImplementationRule(),
            new SchemaScanImplementationRule(),
            new MysqlScanImplementationRule(),
            new EsScanImplementationRule(),
            new MetaScanImplementationRule(),
            new JDBCScanImplementationRule(),
            new TableFunctionTableScanImplementationRule(),
            new HashAggImplementationRule(),
            new ProjectImplementationRule(),
            new TopNImplementationRule(),
            new AssertOneRowImplementationRule(),
            new WindowImplementationRule(),
            new UnionImplementationRule(),
            new ExceptImplementationRule(),
            new IntersectImplementationRule(),
            new ValuesImplementationRule(),
            new RepeatImplementationRule(),
            new FilterImplementationRule(),
            new TableFunctionImplementationRule(),
            new LimitImplementationRule(),
            new CTEAnchorImplementationRule(),
            new CTEAnchorToNoCTEImplementationRule(),
            new CTEConsumerReuseImplementationRule(),
            new CTEConsumeInlineImplementationRule(),
            new CTEProduceImplementationRule()
    );

    private final List<Rule> implementRules = Lists.newArrayList(ALL_IMPLEMENT_RULES);

    private final List<Rule> transformRules = Lists.newArrayList();

    public static final Rule MERGE_LIMIT_RULES = new CombinationRule(RuleType.GP_MERGE_LIMIT, ImmutableList.of(
            new PushDownProjectLimitRule(),
            new EliminateLimitZeroRule(), // should before MergeLimitWithSortRule
            new MergeLimitWithSortRule(),
            new SplitLimitRule(),
            new PushDownLimitJoinRule(),
            new PushDownLimitCTEAnchor(),
            new PushDownLimitUnionRule(),
            new MergeLimitWithLimitRule(),
            new PushDownLimitDirectRule(),
            new MergeLimitDirectRule()
    ));

    public static final Rule PARTITION_PRUNE_RULES = new CombinationRule(RuleType.GP_PARTITION_PRUNE, ImmutableList.of(
            new PartitionPruneRule(),
            new DistributionPruneRule(),
            new ExternalScanPartitionPruneRule(),
            new LimitPruneTabletsRule()
    ));

    public static final Rule VECTOR_REWRITE_RULES = new CombinationRule(RuleType.GP_VECTOR_REWRITE, ImmutableList.of(
            new RewriteToVectorPlanRule()
    ));

    public static final Rule PRUNE_COLUMNS_RULES = new CombinationRule(RuleType.GP_PRUNE_COLUMNS, ImmutableList.of(
            new PruneScanColumnRule(),
            new PruneHDFSScanColumnRule(),
            new PruneProjectColumnsRule(),
            new PruneFilterColumnsRule(),
            new PruneUKFKGroupByKeysRule(), // Put this before PruneAggregateColumnsRule
            new PruneAggregateColumnsRule(),
            new PruneGroupByKeysRule(),
            new PruneTopNColumnsRule(),
            new PruneJoinColumnsRule(),
            new PruneWindowColumnsRule(),
            new PruneUnionColumnsRule(),
            new PruneIntersectColumnsRule(),
            new PruneExceptColumnsRule(),
            new PruneRepeatColumnsRule(),
            new PruneValuesColumnsRule(),
            new PruneTableFunctionColumnRule(),
            new PruneCTEConsumeColumnsRule()
    ));

    public static final Rule PUSH_DOWN_PREDICATE_RULES =
            new CombinationRule(RuleType.GP_PUSH_DOWN_PREDICATE, ImmutableList.of(
                    new CastToEmptyRule(),
                    new PruneTrueFilterRule(),
                    new PushDownPredicateCTEAnchor(),
                    new PushDownPredicateScanRule(),
                    new PushDownPredicateAggRule(),
                    new PushDownPredicateWindowRule(),
                    new PushDownPredicateJoinRule(),
                    new PushDownJoinOnClauseRule(),
                    new PushDownPredicateProjectRule(),
                    new PushDownPredicateUnionRule(),
                    new PushDownPredicateExceptRule(),
                    new PushDownPredicateIntersectRule(),
                    new PushDownPredicateTableFunctionRule(),
                    new PushDownPredicateRepeatRule(),

                    new PushDownPredicateToExternalTableScanRule(),
                    new MergeTwoFiltersRule(),
                    new PushDownPredicateCTEConsumeRule()
            ));

    public static final Rule PUSH_DOWN_SUBQUERY_RULES =
            new CombinationRule(RuleType.GP_PUSH_DOWN_SUBQUERY, ImmutableList.of(
                    new MergeApplyWithTableFunction(),
                    new PushDownApplyLeftProjectRule(),
                    new PushDownApplyLeftRule()
            ));

    public static final Rule SUBQUERY_REWRITE_COMMON_RULES =
            new CombinationRule(RuleType.GP_SUBQUERY_REWRITE_COMMON, ImmutableList.of(
                    new PushDownApplyProjectRule(),
                    new PushDownApplyFilterRule(),
                    new PushDownApplyAggFilterRule(),
                    new PushDownApplyAggProjectFilterRule()
            ));

    public static final Rule SUBQUERY_REWRITE_TO_WINDOW_RULES =
            new CombinationRule(RuleType.GP_SUBQUERY_REWRITE_TO_WINDOW, ImmutableList.of(
                    new ScalarApply2AnalyticRule()
            ));

    public static final Rule SUBQUERY_REWRITE_TO_JOIN_RULES =
            new CombinationRule(RuleType.GP_SUBQUERY_REWRITE_TO_JOIN, ImmutableList.of(
                    new QuantifiedApply2JoinRule(),
                    new ExistentialApply2JoinRule(),
                    new ScalarApply2JoinRule(),
                    new ExistentialApply2OuterJoinRule(),
                    new QuantifiedApply2OuterJoinRule()
            ));

    public static final Rule PRUNE_ASSERT_ROW_RULES =
            new CombinationRule(RuleType.GP_PRUNE_ASSERT_ROW, ImmutableList.of(
                    new PruneAssertOneRowRule()
            ));

    public static final Rule PRUNE_UKFK_JOIN_RULES = new CombinationRule(RuleType.GP_PRUNE_UKFK_JOIN, ImmutableList.of(
            new PruneUKFKJoinRule()
    ));

    public static final Rule AGGREGATE_REWRITE_RULES =
            new CombinationRule(RuleType.GP_AGGREGATE_REWRITE, ImmutableList.of(
                    new RewriteBitmapCountDistinctRule(),
                    new RewriteHllCountDistinctRule(),
                    new RewriteDuplicateAggregateFnRule(),
                    new RewriteSimpleAggToMetaScanRule(),
                    new RewriteSumByAssociativeRule(),
                    new RewriteCountIfFunction()
            ));

    public static final Rule PRUNE_PROJECT_RULES = new CombinationRule(RuleType.GP_PRUNE_PROJECT, ImmutableList.of(
            new PruneProjectRule(),
            new PruneProjectEmptyRule(),
            new MergeTwoProjectRule(),
            new PushDownProjectToCTEAnchorRule()
    ));

    public static final Rule COLLECT_CTE_RULES = new CombinationRule(RuleType.GP_COLLECT_CTE, ImmutableList.of(
            new CollectCTEProduceRule(),
            new CollectCTEConsumeRule()
    ));

    public static final Rule INLINE_CTE_RULES = new CombinationRule(RuleType.GP_INLINE_CTE, ImmutableList.of(
            new InlineOneCTEConsumeRule(),
            new PruneCTEProduceRule()
    ));

    public static final Rule INTERSECT_REWRITE_RULES =
            new CombinationRule(RuleType.GP_INTERSECT_REWRITE, ImmutableList.of(
                    new IntersectAddDistinctRule(),
                    new ReorderIntersectRule()
            ));

    public static final Rule SINGLE_TABLE_MV_REWRITE_RULES =
            new CombinationRule(RuleType.GP_SINGLE_TABLE_MV_REWRITE, ImmutableList.of(
                    AggregateScanRule.getInstance(),
                    AggregateTimeSeriesRule.getInstance(),
                    OnlyScanRule.getInstance()
            ));

    public static final Rule MULTI_TABLE_MV_REWRITE_RULES =
            new CombinationRule(RuleType.GP_MULTI_TABLE_MV_REWRITE, ImmutableList.of(
                    AggregateJoinRule.getInstance(),
                    OnlyJoinRule.getInstance(),
                    AggregateJoinPushDownRule.getInstance()
            ));

    public static final Rule ALL_MV_REWRITE_RULES = new CombinationRule(RuleType.GP_ALL_MV_REWRITE, ImmutableList.of(
            AggregateJoinRule.getInstance(),
            OnlyJoinRule.getInstance(),
            AggregateJoinPushDownRule.getInstance(),
            AggregateScanRule.getInstance(),
            AggregateTimeSeriesRule.getInstance(),
            OnlyScanRule.getInstance()
    ));

    public static final Rule PRUNE_EMPTY_OPERATOR_RULES =
            new CombinationRule(RuleType.GP_PRUNE_EMPTY_OPERATOR, ImmutableList.of(
                    new PruneEmptyScanRule(),
                    PruneEmptyJoinRule.JOIN_LEFT_EMPTY,
                    PruneEmptyJoinRule.JOIN_RIGHT_EMPTY,
                    new PruneEmptyDirectRule(),
                    new PruneEmptyUnionRule(),
                    new PruneEmptyIntersectRule(),
                    new PruneEmptyExceptRule(),
                    new PruneEmptyWindowRule()
            ));

    public static final Rule SHORT_CIRCUIT_SET_RULES =
            new CombinationRule(RuleType.GP_SHORT_CIRCUIT_SET, ImmutableList.of(
                    new PruneTrueFilterRule(),
                    new PushDownPredicateProjectRule(),
                    new PushDownPredicateScanRule(),
                    new CastToEmptyRule(),
                    new PruneProjectColumnsRule(),
                    new PruneScanColumnRule(),
                    new PruneProjectEmptyRule(),
                    new MergeTwoProjectRule(),
                    new PruneProjectRule(),
                    new PartitionPruneRule(),
                    new DistributionPruneRule()));

    public static final Rule FINE_GRAINED_RANGE_PREDICATE_RULES =
            new CombinationRule(RuleType.GP_FINE_GRAINED_RANGE_PREDICATE, ImmutableList.of(
                    FineGrainedRangePredicateRule.INSTANCE,
                    FineGrainedRangePredicateRule.PROJECTION_INSTANCE));

    public static final Rule ELIMINATE_OP_WITH_CONSTANT_RULES =
            new CombinationRule(RuleType.GP_ELIMINATE_OP_WITH_CONSTANT, ImmutableList.of(
                    EliminateGroupByConstantRule.INSTANCE,
                    EliminateJoinWithConstantRule.ELIMINATE_JOIN_WITH_LEFT_SINGLE_VALUE_RULE,
                    EliminateJoinWithConstantRule.ELIMINATE_JOIN_WITH_RIGHT_SINGLE_VALUE_RULE
            ));

    public static final Rule META_SCAN_REWRITE_RULES =
            new CombinationRule(RuleType.GP_META_SCAN_REWRITE, ImmutableList.of(
                    new PushDownAggToMetaScanRule(),
                    new PushDownFlatJsonMetaToMetaScanRule(),
                    new RewriteSimpleAggToMetaScanRule(),
                    new RewriteSimpleAggToHDFSScanRule(),
                    new MinMaxCountOptOnScanRule()
            ));

    public RuleSet() {
        // Add common transform rule
        transformRules.add(SplitMultiPhaseAggRule.getInstance());
        transformRules.add(SplitTwoPhaseAggRule.getInstance());
        transformRules.add(GroupByCountDistinctDataSkewEliminateRule.getInstance());
        transformRules.add(SplitTopNRule.getInstance());
    }

    public void addJoinTransformationRules() {
        transformRules.add(JoinCommutativityRule.getInstance());
        transformRules.add(JoinAssociativityRule.INNER_JOIN_ASSOCIATIVITY_RULE);
    }

    public void addOuterJoinTransformationRules() {
        transformRules.add(JoinAssociativityRule.OUTER_JOIN_ASSOCIATIVITY_RULE);
        transformRules.add(JoinLeftAsscomRule.OUTER_JOIN_LEFT_ASSCOM_RULE);
    }

    public void addJoinCommutativityWithoutInnerRule() {
        transformRules.add(JoinCommutativityWithoutInnerRule.getInstance());
    }

    public void addCboTablePruneRule() {
        transformRules.add(CboTablePruneRule.getInstance());
    }

    public void addMultiTableMvRewriteRule() {
        transformRules.add(AggregateJoinRule.getInstance());
        transformRules.add(OnlyJoinRule.getInstance());
        transformRules.add(AggregateJoinPushDownRule.getInstance());
    }

    public List<Rule> getTransformRules() {
        return transformRules;
    }

    public List<Rule> getImplementRules() {
        return implementRules;
    }

    public void addRealtimeMVRules() {
        this.implementRules.add(StreamJoinImplementationRule.getInstance());
        this.implementRules.add(StreamAggregateImplementationRule.getInstance());
        this.implementRules.add(StreamScanImplementationRule.getInstance());
    }

    public void addHashJoinImplementationRule() {
        this.implementRules.add(HashJoinImplementationRule.getInstance());
    }

    public void addMergeJoinImplementationRule() {
        this.implementRules.add(MergeJoinImplementationRule.getInstance());
    }

    public void addNestLoopJoinImplementationRule() {
        this.implementRules.add(NestLoopJoinImplementationRule.getInstance());
    }

    public void addAutoJoinImplementationRule() {
        this.implementRules.add(HashJoinImplementationRule.getInstance());
        // TODO: implement merge join
        // this.implementRules.add(MergeJoinImplementationRule.getInstance());
        this.implementRules.add(NestLoopJoinImplementationRule.getInstance());
    }

    public void addSingleTableMvRewriteRule() {
        transformRules.addAll(SINGLE_TABLE_MV_REWRITE_RULES.predecessorRules());
    }

}
