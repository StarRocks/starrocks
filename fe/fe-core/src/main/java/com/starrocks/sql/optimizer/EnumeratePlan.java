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

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.OutputPropertyGroup;
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

        // 1. get all possible output properties that satisfying the required property
        List<PhysicalPropertySet> outputProperties =
                rootGroup.getSatisfyRequiredPropertyGroupExpressions(requiredProperty);

        // 2. count the valid plan in Memo and record in GroupExpression
        List<Integer> planCounts = Lists.newArrayList();
        int totalPlanCount = 0;
        for (PhysicalPropertySet outputProperty : outputProperties) {
            int planCount = countGroupValidPlan(rootGroup, outputProperty);
            totalPlanCount += planCount;
            planCounts.add(planCount);
        }

        if (nthExecPlan > totalPlanCount) {
            throw new StarRocksPlannerException("plan count is " + totalPlanCount + ", can not extract the " +
                    nthExecPlan + "th plan", ErrorType.USER_ERROR);
        }
        // 3. use un-ranking method to extract the nth plan
        int outputPropertyIdx = 0;
        int remainingNth = nthExecPlan;
        for (; outputPropertyIdx < outputProperties.size(); outputPropertyIdx++) {
            if (remainingNth <= planCounts.get(outputPropertyIdx)) {
                break;
            }
            remainingNth -= planCounts.get(outputPropertyIdx);
        }
        OptExpression expression = extractNthPlanImpl(rootGroup, outputProperties.get(outputPropertyIdx), remainingNth);
        expression.setPlanCount(totalPlanCount);
        return expression;
    }

    public static OptExpression extractNthPlanImpl(Group group, PhysicalPropertySet outputProperty,
                                                   int nthExecPlan) {
        int planCountOfKGroupExpression = 0;
        List<OptExpression> childPlans = Lists.newArrayList();
        // 1. Calculate the GroupExpression of nthExecPlan in the Group
        GroupExpression chooseGroupExpression = null;
        Set<GroupExpression> groupExpressions = group.getSatisfyOutputPropertyGroupExpressions(outputProperty);
        for (GroupExpression groupExpression : groupExpressions) {
            if (planCountOfKGroupExpression + groupExpression.getOutputPropertyPlanCount(outputProperty) >=
                    nthExecPlan) {
                chooseGroupExpression = groupExpression;
                break;
            }
            planCountOfKGroupExpression += groupExpression.getOutputPropertyPlanCount(outputProperty);
        }

        Preconditions.checkState(chooseGroupExpression != null);
        // 2. compute the local-rank in the chooseGroupExpression.
        int localRankOfGroupExpression = nthExecPlan - planCountOfKGroupExpression;
        // 3. compute use which output/input properties
        int planCountOfKProperties = 0;
        List<PhysicalPropertySet> childrenOutputProperties = Lists.newArrayList();
        Set<Map.Entry<OutputPropertyGroup, Integer>> entries = chooseGroupExpression
                .getPropertiesPlanCountMap(outputProperty).entrySet();
        for (Map.Entry<OutputPropertyGroup, Integer> entry : entries) {
            OutputPropertyGroup outputPropertyGroup = entry.getKey();
            childrenOutputProperties = outputPropertyGroup.getChildrenOutputProperties();

            if (planCountOfKProperties + entry.getValue() >= localRankOfGroupExpression) {
                // 4. compute the localProperty-rank in the property.
                int localRankOfProperties = localRankOfGroupExpression - planCountOfKProperties;
                // 5. compute sub-ranks of children groups
                List<Integer> childRankList =
                        computeNthOfChildGroups(chooseGroupExpression, localRankOfProperties,
                                childrenOutputProperties);
                // 6. computes the child group recursively
                for (int childIndex = 0; childIndex < chooseGroupExpression.arity(); ++childIndex) {
                    OptExpression childPlan = extractNthPlanImpl(chooseGroupExpression.inputAt(childIndex),
                            childrenOutputProperties.get(childIndex), childRankList.get(childIndex));
                    childPlans.add(childPlan);
                }
                break;
            }
            planCountOfKProperties += entry.getValue();
        }
        // 7. construct the OptExpression
        OptExpression chooseExpression = OptExpression.create(chooseGroupExpression.getOp(), childPlans);
        // record childrenOutputProperties at optExpression, used for planFragment builder to determine join type
        chooseExpression.setRequiredProperties(childrenOutputProperties);
        chooseExpression.setStatistics(group.getStatistics());
        chooseExpression.setOutputProperty(outputProperty);

        // When build plan fragment, we need the output column of logical property
        chooseExpression.setLogicalProperty(group.getLogicalProperty());
        return chooseExpression;
    }

    // compute sub-ranks of children groups
    public static List<Integer> computeNthOfChildGroups(GroupExpression groupExpression, int localNth,
                                                        List<PhysicalPropertySet> childrenOutputProperties) {
        // 1. compute the plan count of each child group which satisfied the childrenOutputProperties
        List<Integer> childGroupExprCountList = Lists.newArrayList();
        for (int childIndex = 0; childIndex < groupExpression.arity(); ++childIndex) {
            PhysicalPropertySet childOutputProperty = childrenOutputProperties.get(childIndex);
            Set<GroupExpression> childGroupExpressions = groupExpression.inputAt(childIndex)
                    .getSatisfyOutputPropertyGroupExpressions(childOutputProperty);
            int childGroupExprPropertiesCount = childGroupExpressions.stream()
                    .mapToInt(childGroupExpression -> childGroupExpression
                            .getOutputPropertyPlanCount(childOutputProperty)).sum();
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
    public static int countGroupValidPlan(Group group, PhysicalPropertySet outputProperty) {
        int groupPlanCount = 0;
        for (GroupExpression groupExpression : group.getSatisfyOutputPropertyGroupExpressions(outputProperty)) {
            if (groupExpression.getInputs().isEmpty()) {
                // It's leaf node of the plan
                groupExpression.addPlanCountOfProperties(OutputPropertyGroup.of(outputProperty), 1);
            } else if (groupExpression.hasValidSubPlan()) {
                // count the plan count of this group expression
                countGroupExpressionValidPlan(groupExpression, outputProperty);
            }
            groupPlanCount += groupExpression.getOutputPropertyPlanCount(outputProperty);
        }
        return groupPlanCount;
    }

    // Calculate the valid plan count for the group expression with required property.
    private static void countGroupExpressionValidPlan(GroupExpression groupExpression,
                                                      PhysicalPropertySet outputProperty) {
        for (OutputPropertyGroup outputPropertyGroup : groupExpression.getChildrenOutputProperties(outputProperty)) {
            List<PhysicalPropertySet> childrenOutputProperties = outputPropertyGroup.getChildrenOutputProperties();

            int childPlanCount = 1;
            for (int childIndex = 0; childIndex < groupExpression.arity(); ++childIndex) {
                childPlanCount *= countGroupValidPlan(groupExpression.inputAt(childIndex),
                        childrenOutputProperties.get(childIndex));
            }

            groupExpression.addPlanCountOfProperties(outputPropertyGroup, childPlanCount);
        }
    }
}
