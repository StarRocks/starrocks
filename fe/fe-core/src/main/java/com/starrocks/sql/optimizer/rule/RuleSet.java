// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
import com.starrocks.sql.optimizer.rule.implementation.IcebergScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.IntersectImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.JDBCScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.LimitImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.MergeJoinImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.MetaScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.MysqlScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.NestLoopJoinImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.OlapScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.ProjectImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.RepeatImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.SchemaScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.TableFunctionImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.TopNImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.UnionImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.ValuesImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.WindowImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.stream.StreamAggregateImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.stream.StreamJoinImplementationRule;
import com.starrocks.sql.optimizer.rule.transformation.CastToEmptyRule;
import com.starrocks.sql.optimizer.rule.transformation.CollectCTEConsumeRule;
import com.starrocks.sql.optimizer.rule.transformation.CollectCTEProduceRule;
import com.starrocks.sql.optimizer.rule.transformation.DistributionPruneRule;
import com.starrocks.sql.optimizer.rule.transformation.EliminateLimitZeroRule;
import com.starrocks.sql.optimizer.rule.transformation.ExistentialApply2JoinRule;
import com.starrocks.sql.optimizer.rule.transformation.ExistentialApply2OuterJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.ExternalScanPartitionPruneRule;
import com.starrocks.sql.optimizer.rule.transformation.GroupByCountDistinctDataSkewEliminateRule;
import com.starrocks.sql.optimizer.rule.transformation.InlineOneCTEConsumeRule;
import com.starrocks.sql.optimizer.rule.transformation.IntersectAddDistinctRule;
import com.starrocks.sql.optimizer.rule.transformation.JoinAssociativityRule;
import com.starrocks.sql.optimizer.rule.transformation.JoinCommutativityRule;
import com.starrocks.sql.optimizer.rule.transformation.JoinCommutativityWithOutInnerRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeApplyWithTableFunction;
import com.starrocks.sql.optimizer.rule.transformation.MergeLimitDirectRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeLimitWithLimitRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeLimitWithSortRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoFiltersRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PartitionPruneRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneAggregateColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneAssertOneRowRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneCTEConsumeColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneCTEProduceRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneExceptColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneExceptEmptyRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneFilterColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneHDFSScanColumnRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneIntersectColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneIntersectEmptyRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneJoinColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneProjectColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneProjectEmptyRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneRepeatColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneScanColumnRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneTableFunctionColumnRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneTopNColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneUnionColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneUnionEmptyRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneValuesColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneWindowColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownApplyAggFilterRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownApplyAggProjectFilterRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownApplyFilterRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownApplyLeftProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownApplyLeftRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownApplyProjectRule;
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
import com.starrocks.sql.optimizer.rule.transformation.PushDownProjectToCTEAnchorRule;
import com.starrocks.sql.optimizer.rule.transformation.QuantifiedApply2JoinRule;
import com.starrocks.sql.optimizer.rule.transformation.QuantifiedApply2OuterJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.ReorderIntersectRule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteBitmapCountDistinctRule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteDuplicateAggregateFnRule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteHllCountDistinctRule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteMultiDistinctByCTERule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteMultiDistinctRule;
import com.starrocks.sql.optimizer.rule.transformation.ScalarApply2AnalyticRule;
import com.starrocks.sql.optimizer.rule.transformation.ScalarApply2JoinRule;
import com.starrocks.sql.optimizer.rule.transformation.SplitAggregateRule;
import com.starrocks.sql.optimizer.rule.transformation.SplitLimitRule;
import com.starrocks.sql.optimizer.rule.transformation.SplitTopNRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.AggregateJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.AggregateScanRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.OnlyJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.OnlyScanRule;

import java.util.List;
import java.util.Map;

public class RuleSet {
    private static final Map<RuleSetType, List<Rule>> REWRITE_RULES = Maps.newHashMap();

    private static final List<Rule> ALL_IMPLEMENT_RULES = ImmutableList.of(
            new OlapScanImplementationRule(),
            new HiveScanImplementationRule(),
            new FileScanImplementationRule(),
            new IcebergScanImplementationRule(),
            new HudiScanImplementationRule(),
            new DeltaLakeScanImplementationRule(),
            new SchemaScanImplementationRule(),
            new MysqlScanImplementationRule(),
            new EsScanImplementationRule(),
            new MetaScanImplementationRule(),
            new JDBCScanImplementationRule(),
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

    static {
        REWRITE_RULES.put(RuleSetType.MERGE_LIMIT, ImmutableList.of(
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
                MergeLimitDirectRule.HIVE_SCAN,
                MergeLimitDirectRule.ICEBERG_SCAN,
                MergeLimitDirectRule.HUDI_SCAN,
                MergeLimitDirectRule.DELTALAKE_SCAN,
                MergeLimitDirectRule.FILE_SCAN,
                MergeLimitDirectRule.SCHEMA_SCAN,
                MergeLimitDirectRule.MYSQL_SCAN,
                MergeLimitDirectRule.ES_SCAN,
                MergeLimitDirectRule.JDBC_SCAN,
                MergeLimitDirectRule.WINDOW,
                MergeLimitDirectRule.INTERSECT,
                MergeLimitDirectRule.EXCEPT,
                MergeLimitDirectRule.VALUES,
                MergeLimitDirectRule.FILTER,
                MergeLimitDirectRule.TABLE_FUNCTION
        ));

        REWRITE_RULES.put(RuleSetType.PARTITION_PRUNE, ImmutableList.of(
                new PartitionPruneRule(),
                new DistributionPruneRule(),
                ExternalScanPartitionPruneRule.HIVE_SCAN,
                ExternalScanPartitionPruneRule.HUDI_SCAN,
                ExternalScanPartitionPruneRule.ICEBERG_SCAN,
                ExternalScanPartitionPruneRule.DELTALAKE_SCAN,
                ExternalScanPartitionPruneRule.FILE_SCAN,
                ExternalScanPartitionPruneRule.ES_SCAN
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
                PruneScanColumnRule.JDBC_SCAN,
                new PruneProjectColumnsRule(),
                new PruneFilterColumnsRule(),
                new PruneAggregateColumnsRule(),
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
                new PushDownPredicateCTEAnchor(),
                PushDownPredicateScanRule.OLAP_SCAN,
                PushDownPredicateScanRule.HIVE_SCAN,
                PushDownPredicateScanRule.ICEBERG_SCAN,
                PushDownPredicateScanRule.HUDI_SCAN,
                PushDownPredicateScanRule.DELTALAKE_SCAN,
                PushDownPredicateScanRule.FILE_SCAN,
                PushDownPredicateScanRule.SCHEMA_SCAN,
                PushDownPredicateScanRule.ES_SCAN,
                PushDownPredicateScanRule.META_SCAN,
                // Commented out because of UTs in `ExternalTableTest.java`
                // PushDownPredicateScanRule.MYSQL_SCAN,
                // PushDownPredicateScanRule.JDBC_SCAN,
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

        REWRITE_RULES.put(RuleSetType.AGGREGATE_REWRITE, ImmutableList.of(
                new RewriteBitmapCountDistinctRule(),
                new RewriteHllCountDistinctRule(),
                new RewriteDuplicateAggregateFnRule()
        ));

        REWRITE_RULES.put(RuleSetType.MULTI_DISTINCT_REWRITE, ImmutableList.of(
                new RewriteMultiDistinctByCTERule(),
                new RewriteMultiDistinctRule()
        ));

        REWRITE_RULES.put(RuleSetType.PRUNE_SET_OPERATOR, ImmutableList.of(
                new PruneUnionEmptyRule(),
                new PruneIntersectEmptyRule(),
                new PruneExceptEmptyRule()
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
                OnlyJoinRule.getInstance()
        ));
    }

    public RuleSet() {
        // Add common transform rule
        transformRules.add(SplitAggregateRule.getInstance());
        transformRules.add(GroupByCountDistinctDataSkewEliminateRule.getInstance());
        transformRules.add(SplitTopNRule.getInstance());
    }

    public void addJoinTransformationRules() {
        transformRules.add(JoinCommutativityRule.getInstance());
        transformRules.add(JoinAssociativityRule.getInstance());
    }

    public void addJoinCommutativityWithOutInnerRule() {
        transformRules.add(JoinCommutativityWithOutInnerRule.getInstance());
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

    public List<Rule> getRewriteRulesByType(RuleSetType type) {
        return REWRITE_RULES.get(type);
    }

    public void addRealtimeMVRules() {
        this.implementRules.add(StreamJoinImplementationRule.getInstance());
        this.implementRules.add(StreamAggregateImplementationRule.getInstance());
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
