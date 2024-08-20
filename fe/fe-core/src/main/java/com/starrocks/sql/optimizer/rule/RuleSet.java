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
import com.google.common.collect.Maps;
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
import com.starrocks.sql.optimizer.rule.transformation.ScalarApply2AnalyticRule;
import com.starrocks.sql.optimizer.rule.transformation.ScalarApply2JoinRule;
import com.starrocks.sql.optimizer.rule.transformation.SplitLimitRule;
import com.starrocks.sql.optimizer.rule.transformation.SplitMultiPhaseAggRule;
import com.starrocks.sql.optimizer.rule.transformation.SplitTopNRule;
import com.starrocks.sql.optimizer.rule.transformation.SplitTwoPhaseAggRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.AggregateJoinPushDownRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.AggregateJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.AggregateScanRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.OnlyJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.OnlyScanRule;
import com.starrocks.sql.optimizer.rule.transformation.pruner.CboTablePruneRule;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RuleSet {
    private static final Map<RuleSetType, List<Rule>> REWRITE_RULES = Maps.newHashMap();

    private static final List<Rule> ALL_IMPLEMENT_RULES = ImmutableList.of(
            new OlapScanImplementationRule(),
            new HiveScanImplementationRule(),
            new FileScanImplementationRule(),
            new IcebergScanImplementationRule(),
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

    private static final ImmutableList<Rule> MV_IMPLEMENT_RULES = ImmutableList.of(
            StreamJoinImplementationRule.getInstance(),
            StreamAggregateImplementationRule.getInstance()
    );

    private final List<Rule> implementRules = Lists.newArrayList(ALL_IMPLEMENT_RULES);

    private final List<Rule> transformRules = Lists.newArrayList();

    static {
        REWRITE_RULES.put(RuleSetType.MERGE_LIMIT, ImmutableList.of(
                new PushDownProjectLimitRule(),
                new EliminateLimitZeroRule(), // should before MergeLimitWithSortRule
                new MergeLimitWithSortRule(),
                new SplitLimitRule(),
                new PushDownLimitJoinRule(),
                new PushDownLimitCTEAnchor(),
                new PushDownLimitUnionRule(),
                new MergeLimitWithLimitRule(),
                PushDownLimitDirectRule.PROJECT,
                PushDownLimitDirectRule.ASSERT_ONE_ROW,
                PushDownLimitDirectRule.CTE_CONSUME,
                MergeLimitDirectRule.AGGREGATE,
                MergeLimitDirectRule.OLAP_SCAN,
                MergeLimitDirectRule.VIEW_SCAN,
                MergeLimitDirectRule.HIVE_SCAN,
                MergeLimitDirectRule.ICEBERG_SCAN,
                MergeLimitDirectRule.HUDI_SCAN,
                MergeLimitDirectRule.DELTALAKE_SCAN,
                MergeLimitDirectRule.FILE_SCAN,
                MergeLimitDirectRule.PAIMON_SCAN,
                MergeLimitDirectRule.ODPS_SCAN,
                MergeLimitDirectRule.KUDU_SCAN,
                MergeLimitDirectRule.SCHEMA_SCAN,
                MergeLimitDirectRule.MYSQL_SCAN,
                MergeLimitDirectRule.ES_SCAN,
                MergeLimitDirectRule.JDBC_SCAN,
                MergeLimitDirectRule.ICEBERG_METADATA_SCAN,
                MergeLimitDirectRule.WINDOW,
                MergeLimitDirectRule.INTERSECT,
                MergeLimitDirectRule.EXCEPT,
                MergeLimitDirectRule.VALUES,
                MergeLimitDirectRule.FILTER,
                MergeLimitDirectRule.TABLE_FUNCTION,
                MergeLimitDirectRule.TABLE_FUNCTION_TABLE_SCAN
        ));

        REWRITE_RULES.put(RuleSetType.PARTITION_PRUNE, ImmutableList.of(
                new PartitionPruneRule(),
                new DistributionPruneRule(),
                ExternalScanPartitionPruneRule.HIVE_SCAN,
                ExternalScanPartitionPruneRule.HUDI_SCAN,
                ExternalScanPartitionPruneRule.ICEBERG_SCAN,
                ExternalScanPartitionPruneRule.DELTALAKE_SCAN,
                ExternalScanPartitionPruneRule.FILE_SCAN,
                ExternalScanPartitionPruneRule.ES_SCAN,
                ExternalScanPartitionPruneRule.PAIMON_SCAN,
                ExternalScanPartitionPruneRule.ODPS_SCAN,
                ExternalScanPartitionPruneRule.KUDU_SCAN,
                new LimitPruneTabletsRule()
        ));

        REWRITE_RULES.put(RuleSetType.PRUNE_COLUMNS, ImmutableList.of(
                PruneScanColumnRule.OLAP_SCAN,
                PruneScanColumnRule.SCHEMA_SCAN,
                PruneScanColumnRule.MYSQL_SCAN,
                PruneScanColumnRule.ES_SCAN,
                PruneHDFSScanColumnRule.HIVE_SCAN,
                PruneHDFSScanColumnRule.ICEBERG_SCAN,
                PruneHDFSScanColumnRule.DELTALAKE_SCAN,
                PruneHDFSScanColumnRule.FILE_SCAN,
                PruneHDFSScanColumnRule.HUDI_SCAN,
                PruneHDFSScanColumnRule.TABLE_FUNCTION_TABLE_SCAN,
                PruneHDFSScanColumnRule.PAIMON_SCAN,
                PruneHDFSScanColumnRule.ODPS_SCAN,
                PruneScanColumnRule.KUDU_SCAN,
                PruneScanColumnRule.JDBC_SCAN,
                PruneScanColumnRule.BINLOG_SCAN,
                new PruneProjectColumnsRule(),
                new PruneFilterColumnsRule(),
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

        REWRITE_RULES.put(RuleSetType.PUSH_DOWN_PREDICATE, ImmutableList.of(
                new CastToEmptyRule(),
                new PruneTrueFilterRule(),
                new PushDownPredicateCTEAnchor(),
                PushDownPredicateScanRule.OLAP_SCAN,
                PushDownPredicateScanRule.HIVE_SCAN,
                PushDownPredicateScanRule.ICEBERG_SCAN,
                PushDownPredicateScanRule.HUDI_SCAN,
                PushDownPredicateScanRule.DELTALAKE_SCAN,
                PushDownPredicateScanRule.FILE_SCAN,
                PushDownPredicateScanRule.PAIMON_SCAN,
                PushDownPredicateScanRule.ICEBERG_METADATA_SCAN,
                PushDownPredicateScanRule.KUDU_SCAN,
                PushDownPredicateScanRule.SCHEMA_SCAN,
                PushDownPredicateScanRule.ES_SCAN,
                PushDownPredicateScanRule.META_SCAN,
                PushDownPredicateScanRule.BINLOG_SCAN,
                PushDownPredicateScanRule.TABLE_FUNCTION_TABLE_SCAN,
                PushDownPredicateScanRule.VIEW_SCAN,
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

                PushDownPredicateToExternalTableScanRule.MYSQL_SCAN,
                PushDownPredicateToExternalTableScanRule.JDBC_SCAN,
                PushDownPredicateToExternalTableScanRule.ODPS_SCAN,
                new MergeTwoFiltersRule(),
                new PushDownPredicateCTEConsumeRule()
        ));

        REWRITE_RULES.put(RuleSetType.PUSH_DOWN_SUBQUERY, ImmutableList.of(
                new MergeApplyWithTableFunction(),
                new PushDownApplyLeftProjectRule(),
                new PushDownApplyLeftRule()
        ));

        REWRITE_RULES.put(RuleSetType.SUBQUERY_REWRITE_COMMON, ImmutableList.of(
                new PushDownApplyProjectRule(),
                new PushDownApplyFilterRule(),
                new PushDownApplyAggFilterRule(),
                new PushDownApplyAggProjectFilterRule()
        ));

        REWRITE_RULES.put(RuleSetType.SUBQUERY_REWRITE_TO_WINDOW, ImmutableList.of(
                new ScalarApply2AnalyticRule()
        ));

        REWRITE_RULES.put(RuleSetType.SUBQUERY_REWRITE_TO_JOIN, ImmutableList.of(
                new QuantifiedApply2JoinRule(),
                new ExistentialApply2JoinRule(),
                new ScalarApply2JoinRule(),
                new ExistentialApply2OuterJoinRule(),
                new QuantifiedApply2OuterJoinRule()
        ));

        REWRITE_RULES.put(RuleSetType.PRUNE_ASSERT_ROW, ImmutableList.of(
                new PruneAssertOneRowRule()
        ));

        REWRITE_RULES.put(RuleSetType.PRUNE_UKFK_JOIN, ImmutableList.of(
                new PruneUKFKJoinRule()
        ));

        REWRITE_RULES.put(RuleSetType.AGGREGATE_REWRITE, ImmutableList.of(
                new RewriteBitmapCountDistinctRule(),
                new RewriteHllCountDistinctRule(),
                new RewriteDuplicateAggregateFnRule(),
                new RewriteSimpleAggToMetaScanRule(),
                new RewriteSumByAssociativeRule(),
                new RewriteCountIfFunction()
        ));

        REWRITE_RULES.put(RuleSetType.PRUNE_PROJECT, ImmutableList.of(
                new PruneProjectRule(),
                new PruneProjectEmptyRule(),
                new MergeTwoProjectRule(),
                new PushDownProjectToCTEAnchorRule()
        ));

        REWRITE_RULES.put(RuleSetType.COLLECT_CTE, ImmutableList.of(
                new CollectCTEProduceRule(),
                new CollectCTEConsumeRule()
        ));

        REWRITE_RULES.put(RuleSetType.INLINE_CTE, ImmutableList.of(
                new InlineOneCTEConsumeRule(),
                new PruneCTEProduceRule()
        ));

        REWRITE_RULES.put(RuleSetType.INTERSECT_REWRITE, ImmutableList.of(
                new IntersectAddDistinctRule(),
                new ReorderIntersectRule()
        ));

        REWRITE_RULES.put(RuleSetType.SINGLE_TABLE_MV_REWRITE, ImmutableList.of(
                AggregateScanRule.getInstance(),
                OnlyScanRule.getInstance()
        ));

        REWRITE_RULES.put(RuleSetType.MULTI_TABLE_MV_REWRITE, ImmutableList.of(
                AggregateJoinRule.getInstance(),
                OnlyJoinRule.getInstance(),
                AggregateJoinPushDownRule.getInstance()
        ));

        REWRITE_RULES.put(RuleSetType.ALL_MV_REWRITE, Stream.concat(
                        REWRITE_RULES.get(RuleSetType.MULTI_TABLE_MV_REWRITE).stream(),
                        REWRITE_RULES.get(RuleSetType.SINGLE_TABLE_MV_REWRITE).stream())
                .collect(Collectors.toList()));

        REWRITE_RULES.put(RuleSetType.PRUNE_EMPTY_OPERATOR, ImmutableList.of(
                PruneEmptyScanRule.OLAP_SCAN,
                PruneEmptyScanRule.HIVE_SCAN,
                PruneEmptyScanRule.HUDI_SCAN,
                PruneEmptyScanRule.ICEBERG_SCAN,
                PruneEmptyScanRule.PAIMON_SCAN,
                PruneEmptyScanRule.ODPS_SCAN,
                PruneEmptyScanRule.KUDU_SCAN,
                PruneEmptyJoinRule.JOIN_LEFT_EMPTY,
                PruneEmptyJoinRule.JOIN_RIGHT_EMPTY,
                new PruneEmptyDirectRule(),
                new PruneEmptyUnionRule(),
                new PruneEmptyIntersectRule(),
                new PruneEmptyExceptRule(),
                new PruneEmptyWindowRule()
        ));

        REWRITE_RULES.put(RuleSetType.SHORT_CIRCUIT_SET, ImmutableList.of(
                new PruneTrueFilterRule(),
                new PushDownPredicateProjectRule(),
                PushDownPredicateScanRule.OLAP_SCAN,
                new CastToEmptyRule(),
                new PruneProjectColumnsRule(),
                PruneScanColumnRule.OLAP_SCAN,
                new PruneProjectEmptyRule(),
                new MergeTwoProjectRule(),
                new PruneProjectRule(),
                new PartitionPruneRule(),
                new DistributionPruneRule()));

        REWRITE_RULES.put(RuleSetType.FINE_GRAINED_RANGE_PREDICATE, ImmutableList.of(
                FineGrainedRangePredicateRule.INSTANCE,
                FineGrainedRangePredicateRule.PROJECTION_INSTANCE));

        REWRITE_RULES.put(RuleSetType.ELIMINATE_OP_WITH_CONSTANT, ImmutableList.of(
                EliminateGroupByConstantRule.INSTANCE,
                EliminateJoinWithConstantRule.ELIMINATE_JOIN_WITH_LEFT_SINGLE_VALUE_RULE,
                EliminateJoinWithConstantRule.ELIMINATE_JOIN_WITH_RIGHT_SINGLE_VALUE_RULE
        ));

        REWRITE_RULES.put(RuleSetType.META_SCAN_REWRITE, ImmutableList.of(
                new PushDownAggToMetaScanRule(),
                new PushDownFlatJsonMetaToMetaScanRule(),
                new RewriteSimpleAggToMetaScanRule(),
                RewriteSimpleAggToHDFSScanRule.FILE_SCAN,
                RewriteSimpleAggToHDFSScanRule.HIVE_SCAN,
                RewriteSimpleAggToHDFSScanRule.ICEBERG_SCAN,
                new MinMaxCountOptOnScanRule()
        ));
    }

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
        transformRules.addAll(REWRITE_RULES.get(RuleSetType.MULTI_TABLE_MV_REWRITE));
    }

    public List<Rule> getTransformRules() {
        return transformRules;
    }

    public List<Rule> getImplementRules() {
        return implementRules;
    }

    public static List<Rule> getRewriteRulesByType(RuleSetType type) {
        return REWRITE_RULES.get(type);
    }

    public static List<Rule> getRewriteRulesByType(List<RuleSetType> types) {
        List<Rule> allRules = Lists.newArrayList();
        for (RuleSetType ruleSetType : types) {
            allRules.addAll(REWRITE_RULES.get(ruleSetType));
        }
        return allRules;
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
        transformRules.addAll(getRewriteRulesByType(RuleSetType.SINGLE_TABLE_MV_REWRITE));
    }

}
