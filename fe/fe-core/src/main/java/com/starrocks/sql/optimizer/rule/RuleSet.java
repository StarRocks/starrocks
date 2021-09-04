// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.rule.implementation.AssertOneRowImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.EsScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.ExceptImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.FilterImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.HashAggImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.HashJoinImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.HiveScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.IntersectImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.MysqlScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.OlapScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.ProjectImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.RepeatImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.SchemaScanImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.TableFunctionImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.TopNImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.UnionImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.ValuesImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.WindowImplementationRule;
import com.starrocks.sql.optimizer.rule.transformation.ApplyExceptionRule;
import com.starrocks.sql.optimizer.rule.transformation.CastToEmptyRule;
import com.starrocks.sql.optimizer.rule.transformation.DistributionPruneRule;
import com.starrocks.sql.optimizer.rule.transformation.EliminateLimitZeroRule;
import com.starrocks.sql.optimizer.rule.transformation.EsScanPartitionPruneRule;
import com.starrocks.sql.optimizer.rule.transformation.ExistentialApply2JoinRule;
import com.starrocks.sql.optimizer.rule.transformation.ExistentialApply2OuterJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.HiveScanPartitionPruneRule;
import com.starrocks.sql.optimizer.rule.transformation.JoinAssociativityRule;
import com.starrocks.sql.optimizer.rule.transformation.JoinCommutativityRule;
import com.starrocks.sql.optimizer.rule.transformation.JoinCommutativityWithOutInnerRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeApplyWithTableFunction;
import com.starrocks.sql.optimizer.rule.transformation.MergeLimitDirectRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeLimitWithLimitRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeLimitWithSortRule;
import com.starrocks.sql.optimizer.rule.transformation.MergePredicateScanRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoFiltersRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PartitionPruneRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneAggregateColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneAssertOneRowRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneExceptColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneFilterColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneIntersectColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneJoinColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneProjectColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneRepeatColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneScanColumnRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneTableFunctionColumnRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneTopNColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneUnionColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneValuesColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneWindowColumnsRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownApplyAggFilterRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownApplyAggProjectFilterRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownApplyFilterRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownApplyProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownAssertOneRowProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownJoinAggRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownJoinOnClauseRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownLimitDirectRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownLimitJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownLimitUnionRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateAggRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateDirectRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateExceptRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateIntersectRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateScanRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateTableFunctionRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateUnionRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateWindowRule;
import com.starrocks.sql.optimizer.rule.transformation.QuantifiedApply2JoinRule;
import com.starrocks.sql.optimizer.rule.transformation.QuantifiedApply2OuterJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteBitmapCountDistinctRule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteHllCountDistinctRule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteMultiDistinctRule;
import com.starrocks.sql.optimizer.rule.transformation.ScalarApply2JoinRule;
import com.starrocks.sql.optimizer.rule.transformation.ScalarOperatorsReuseRule;
import com.starrocks.sql.optimizer.rule.transformation.SplitAggregateRule;
import com.starrocks.sql.optimizer.rule.transformation.SplitTopNRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RuleSet {
    private static final Map<RuleSetType, List<Rule>> rewriteRules = Maps.newHashMap();

    private static final List<Rule> implementRules = ImmutableList.of(
            new OlapScanImplementationRule(),
            new HiveScanImplementationRule(),
            new SchemaScanImplementationRule(),
            new MysqlScanImplementationRule(),
            new EsScanImplementationRule(),
            new HashJoinImplementationRule(),
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
            new TableFunctionImplementationRule()
    );

    private final List<Rule> transformRules = Lists.newArrayList();

    static {
        rewriteRules.put(RuleSetType.MERGE_LIMIT, new ArrayList<Rule>() {{
            add(new EliminateLimitZeroRule());
            add(new MergeLimitWithLimitRule());
            add(new MergeLimitWithSortRule());
            add(new PushDownLimitDirectRule());
            add(new PushDownLimitUnionRule());
            add(new PushDownLimitJoinRule());
            add(MergeLimitDirectRule.AGGREGATE);
            add(MergeLimitDirectRule.OLAP_SCAN);
            add(MergeLimitDirectRule.SCHEMA_SCAN);
            add(MergeLimitDirectRule.HIVE_SCAN);
            add(MergeLimitDirectRule.MYSQL_SCAN);
            add(MergeLimitDirectRule.ES_SCAN);
            add(MergeLimitDirectRule.WINDOW);
            add(MergeLimitDirectRule.INTERSECT);
            add(MergeLimitDirectRule.EXCEPT);
            add(MergeLimitDirectRule.VALUES);
            add(MergeLimitDirectRule.FILTER);
            add(MergeLimitDirectRule.TABLE_FUNCTION);
        }});

        rewriteRules.put(RuleSetType.PARTITION_PRUNE, new ArrayList<Rule>() {{
            add(new PartitionPruneRule());
            add(new DistributionPruneRule());
            add(new HiveScanPartitionPruneRule());
            add(new EsScanPartitionPruneRule());
            add(new PruneProjectRule());
        }});

        rewriteRules.put(RuleSetType.PRUNE_COLUMNS, new ArrayList<Rule>() {{
            add(new MergeTwoProjectRule());
            add(PruneScanColumnRule.OLAP_SCAN);
            add(PruneScanColumnRule.SCHEMA_SCAN);
            add(PruneScanColumnRule.HIVE_SCAN);
            add(PruneScanColumnRule.MYSQL_SCAN);
            add(PruneScanColumnRule.ES_SCAN);
            add(new PruneProjectColumnsRule());
            add(new PruneFilterColumnsRule());
            add(new PruneAggregateColumnsRule());
            add(new PruneTopNColumnsRule());
            add(new PruneJoinColumnsRule());
            add(new PruneWindowColumnsRule());
            add(new PruneUnionColumnsRule());
            add(new PruneIntersectColumnsRule());
            add(new PruneExceptColumnsRule());
            add(new PruneRepeatColumnsRule());
            add(new PruneValuesColumnsRule());
            add(new PruneTableFunctionColumnRule());
        }});

        rewriteRules.put(RuleSetType.SCALAR_OPERATOR_REUSE, new ArrayList<Rule>() {{
            add(new ScalarOperatorsReuseRule());
        }});

        rewriteRules.put(RuleSetType.PUSH_DOWN_PREDICATE, new ArrayList<Rule>() {{
            add(new CastToEmptyRule());
            add(new PushDownPredicateDirectRule());
            add(PushDownPredicateScanRule.OLAP_SCAN);
            add(PushDownPredicateScanRule.ES_SCAN);
            add(new PushDownPredicateAggRule());
            add(new PushDownPredicateWindowRule());
            add(new PushDownPredicateJoinRule());
            add(new PushDownJoinOnClauseRule());
            add(new PushDownPredicateProjectRule());
            add(new PushDownPredicateUnionRule());
            add(new PushDownPredicateExceptRule());
            add(new PushDownPredicateIntersectRule());
            add(new PushDownPredicateTableFunctionRule());
            add(MergePredicateScanRule.HIVE_SCAN);
            add(MergePredicateScanRule.SCHEMA_SCAN);
            add(MergePredicateScanRule.MYSQL_SCAN);
            add(new MergeTwoFiltersRule());
        }});

        rewriteRules.put(RuleSetType.SUBQUERY_REWRITE, new ArrayList<Rule>() {{
            add(new MergeApplyWithTableFunction());
            add(new PushDownApplyProjectRule());
            add(new PushDownApplyFilterRule());
            add(new PushDownApplyAggFilterRule());
            add(new PushDownApplyAggProjectFilterRule());
            add(new QuantifiedApply2JoinRule());
            add(new ExistentialApply2JoinRule());
            add(new ScalarApply2JoinRule());
            add(new ExistentialApply2OuterJoinRule());
            add(new QuantifiedApply2OuterJoinRule());
            add(new ApplyExceptionRule());
        }});

        rewriteRules.put(RuleSetType.PRUNE_ASSERT_ROW, new ArrayList<Rule>() {{
            add(new PushDownAssertOneRowProjectRule());
            add(new PruneAssertOneRowRule());
        }});

        rewriteRules.put(RuleSetType.MULTI_DISTINCT_REWRITE, new ArrayList<Rule>() {{
            add(new RewriteBitmapCountDistinctRule());
            add(new RewriteHllCountDistinctRule());
            add(new RewriteMultiDistinctRule());
        }});
    }

    public RuleSet() {
        // Add common transform rule
        transformRules.add(SplitAggregateRule.getInstance());
        transformRules.add(SplitTopNRule.getInstance());
    }

    public void addJoinTransformationRules() {
        transformRules.add(JoinCommutativityRule.getInstance());
        transformRules.add(JoinAssociativityRule.getInstance());
    }

    public void addPushDownJoinToAggRule() {
        transformRules.add(PushDownJoinAggRule.getInstance());
    }

    public void addJoinCommutativityWithOutInnerRule() {
        transformRules.add(JoinCommutativityWithOutInnerRule.getInstance());
    }

    public List<Rule> getTransformRules() {
        return transformRules;
    }

    public List<Rule> getImplementRules() {
        return implementRules;
    }

    public List<Rule> getRewriteRulesByType(RuleSetType type) {
        return rewriteRules.get(type);
    }
}
