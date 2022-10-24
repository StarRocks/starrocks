// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.OutputInputProperty;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Refers to Waas F, Galindo-Legaria C. Counting, enumerating, and sampling of execution plans in a cost-based query optimizer.
 * This algorithm mainly contains count and unrank methods.
 * Count records the planCount of the properties of each GroupExpression.
 * Unrank mainly extracts the plan of Nth from the search space. Calculate localRank in Group/GroupExpression recursively
 */
public class EnumeratePlan {
    public static OptExpression extractNthPlan(PhysicalPropertySet requiredProperty, Group rootGroup, int nthExecPlan) {
        if (nthExecPlan < 0) {
            throw new StarRocksPlannerException("Can not extract " + nthExecPlan + " from Memo",
                    ErrorType.INTERNAL_ERROR);
        }
        // 1. count the valid plan in Memo and record in GroupExpression
        int planCount = countGroupValidPlan(rootGroup, requiredProperty);
        if (nthExecPlan > planCount) {
            throw new StarRocksPlannerException("plan count is " + planCount + ", can not extract the " +
                    nthExecPlan + "th plan", ErrorType.USER_ERROR);
        }
        // 2. use unranking method to extract the nth plan
        OptExpression expression = extractNthPlanImpl(requiredProperty, rootGroup, nthExecPlan);
        expression.setPlanCount(planCount);
        return expression;
    }

    public static OptExpression extractNthPlanImpl(PhysicalPropertySet requiredProperty, Group group, int nthExecPlan) {
        int planCountOfKGroupExpression = 0;
        List<OptExpression> childPlans = Lists.newArrayList();
        // 1. Calculate the GroupExpression of nthExecPlan in the Group
        GroupExpression chooseGroupExpression = null;
        for (GroupExpression physicalExpression : group.getSatisfyRequiredGroupExpressions(requiredProperty)) {
            if (planCountOfKGroupExpression + physicalExpression.getRequiredPropertyPlanCount(requiredProperty) >=
                    nthExecPlan) {
                chooseGroupExpression = physicalExpression;
                break;
            }
            planCountOfKGroupExpression += physicalExpression.getRequiredPropertyPlanCount(requiredProperty);
        }

        Preconditions.checkState(chooseGroupExpression != null);
        // 2. compute the local-rank in the chooseGroupExpression.
        int localRankOfGroupExpression = nthExecPlan - planCountOfKGroupExpression;
        // 3. compute use which output/input properties
        int planCountOfKProperties = 0;
        List<PhysicalPropertySet> inputProperties = Lists.newArrayList();
        for (Map.Entry<OutputInputProperty, Integer> entry : chooseGroupExpression
                .getPropertiesPlanCountMap(requiredProperty).entrySet()) {
            inputProperties = entry.getKey().getInputProperties();

            if (planCountOfKProperties + entry.getValue() >= localRankOfGroupExpression) {
                // 4. compute the localProperty-rank in the property.
                int localRankOfProperties = localRankOfGroupExpression - planCountOfKProperties;
                // 5. compute sub-ranks of children groups
                List<Integer> childRankList =
                        computeNthOfChildGroups(chooseGroupExpression, localRankOfProperties, inputProperties);
                // 6. computes the child group recursively
                for (int childIndex = 0; childIndex < chooseGroupExpression.arity(); ++childIndex) {
                    OptExpression childPlan = extractNthPlanImpl(inputProperties.get(childIndex),
                            chooseGroupExpression.inputAt(childIndex), childRankList.get(childIndex));
                    childPlans.add(childPlan);
                }
                break;
            }
            planCountOfKProperties += entry.getValue();
        }
        // 7. construct the OptExpression
        OptExpression chooseExpression = OptExpression.create(chooseGroupExpression.getOp(), childPlans);
        // record inputProperties at optExpression, used for planFragment builder to determine join type
        chooseExpression.setRequiredProperties(inputProperties);
        chooseExpression.setOutputProperty(chooseGroupExpression.getOutputProperty(requiredProperty));
        chooseExpression.setStatistics(group.hasConfidenceStatistic(requiredProperty) ?
                group.getConfidenceStatistic(requiredProperty) :
                group.getStatistics());

        // When build plan fragment, we need the output column of logical property
        chooseExpression.setLogicalProperty(group.getLogicalProperty());
        return chooseExpression;
    }

    // compute sub-ranks of children groups
    public static List<Integer> computeNthOfChildGroups(GroupExpression groupExpression, int localNth,
                                                        List<PhysicalPropertySet> inputProperties) {
        // 1. compute the plan count of each child group which satisfied the inputProperties
        List<Integer> childGroupExprCountList = Lists.newArrayList();
        for (int childIndex = 0; childIndex < groupExpression.arity(); ++childIndex) {
            PhysicalPropertySet childInputProperty = inputProperties.get(childIndex);
            Set<GroupExpression> childGroupExpressions =
                    groupExpression.inputAt(childIndex).getSatisfyRequiredGroupExpressions(childInputProperty);
            int childGroupExprPropertiesCount = childGroupExpressions.stream()
                    .mapToInt(childGroupExpression -> childGroupExpression
                            .getRequiredPropertyPlanCount(childInputProperty)).sum();
            childGroupExprCountList.add(childGroupExprPropertiesCount);
        }

        // 2. use the childGroupExprPropertiesCount to compute nth rank of each child group
        List<Integer> childNthList = Lists.newArrayList();
        // use this variable as R(i+1) to avoid the recursive calculation.
        AtomicInteger subsequentChildGroupRank = new AtomicInteger();
        for (int childIndex = groupExpression.arity() - 1; childIndex >= 0; --childIndex) {
            childNthList.add(computeNthOfChildGroup(childIndex, groupExpression.arity(), localNth,
                    childGroupExprCountList, subsequentChildGroupRank));
        }
        Collections.reverse(childNthList);
        return childNthList;
    }

    // compute sub-rank of child group
    public static int computeNthOfChildGroup(int childIndex, int childrenSize, int localNth,
                                             List<Integer> childGroupExprCountList,
                                             AtomicInteger subsequentChildGroupRank) {
        if (childIndex == 0) {
            return computeNthOfChildGroupImpl(childIndex, childrenSize, localNth, childGroupExprCountList,
                    subsequentChildGroupRank);
        } else {
            return (int) Math.ceil((computeNthOfChildGroupImpl(childIndex, childrenSize, localNth,
                    childGroupExprCountList, subsequentChildGroupRank) * 1.0) /
                    (computeKChildrenPlanCount(childIndex - 1, childGroupExprCountList) * 1.0));
        }
    }

    public static int computeNthOfChildGroupImpl(int childIndex, int childrenSize, int localNth,
                                                 List<Integer> childGroupExprCountList,
                                                 AtomicInteger subsequentChildGroupRank) {
        if (childIndex + 1 == childrenSize) {
            subsequentChildGroupRank.set(localNth);
            return localNth;
        } else {
            int kChildrenPlanCount = computeKChildrenPlanCount(childIndex, childGroupExprCountList);
            int result = subsequentChildGroupRank.get() % kChildrenPlanCount;
            // the localNth is start from 1, special treated.
            if (result == 0) {
                result = kChildrenPlanCount;
            }
            subsequentChildGroupRank.set(result);
            return result;
        }
    }

    // Compute the product of the plan count of the first k child nodes
    public static int computeKChildrenPlanCount(int k, List<Integer> childGroupExprCountList) {
        int result = 1;
        for (int i = 0; i <= k; ++i) {
            result *= childGroupExprCountList.get(i);
        }
        return result;
    }

    // Calculate the valid plan count for the group with required property.
    public static int countGroupValidPlan(Group group, PhysicalPropertySet requiredProperty) {
        int groupPlanCount = 0;
        for (GroupExpression physicalExpression : group.getSatisfyRequiredGroupExpressions(requiredProperty)) {
            if (physicalExpression.getInputs().isEmpty()) {
                // It's leaf node of the plan
                physicalExpression.addPlanCountOfProperties(OutputInputProperty.of(requiredProperty), 1);
            } else if (physicalExpression.hasValidSubPlan()) {
                // count the plan count of this group expression
                countGroupExpressionValidPlan(physicalExpression, requiredProperty);
            }
            groupPlanCount += physicalExpression.getRequiredPropertyPlanCount(requiredProperty);
        }
        return groupPlanCount;
    }

    // Calculate the valid plan count for the group expression with required property.
    public static int countGroupExpressionValidPlan(GroupExpression groupExpression,
                                                    PhysicalPropertySet requiredProperty) {
        int groupExpressionPlanCount = 0;
        for (List<PhysicalPropertySet> inputProperties : groupExpression.getRequiredInputProperties(requiredProperty)) {
            int childPlanCount = 1;
            for (int childIndex = 0; childIndex < groupExpression.arity(); ++childIndex) {
                childPlanCount *=
                        countGroupValidPlan(groupExpression.inputAt(childIndex), inputProperties.get(childIndex));
            }
            groupExpression.addPlanCountOfProperties(OutputInputProperty.of(requiredProperty, inputProperties),
                    childPlanCount);
            groupExpressionPlanCount += childPlanCount;
        }
        return groupExpressionPlanCount;
    }
}
