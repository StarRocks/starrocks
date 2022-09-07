// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A group is a set of logically equivalent logical and
 * physical expressions that produce the same output.
 * <p>
 * Two query trees are logically equivalent if
 * they output exactly the same result for any database.
 * <p>
 * Note that a group might not contain all equivalent expressions.
 * <p>
 * In the initial search space, each group includes only one logical expression,
 * which came from the initial query tree.
 */
public class Group {
    private static final Logger LOG = LogManager.getLogger(Group.class);

    private final int id;

    private final List<GroupExpression> logicalExpressions;
    private final List<GroupExpression> physicalExpressions;

    private boolean isExplored;

    private Statistics statistics;
    // confidence statistics record the statistics when group expression has lowest cost,
    // confidence statistics is the statistics in group with highest confidence for each physical property
    private final Map<PhysicalPropertySet, Statistics> confidenceStatistics;
    private final Map<PhysicalPropertySet, Pair<Double, GroupExpression>> lowestCostExpressions;
    // GroupExpressions in this Group which could satisfy the required property.
    private final Map<PhysicalPropertySet, Set<GroupExpression>> satisfyRequiredPropertyGroupExpressions;

    // All expressions in one group have same logical property.
    private LogicalProperty logicalProperty;

    public Group(int groupId) {
        this.id = groupId;
        logicalExpressions = Lists.newArrayList();
        physicalExpressions = Lists.newArrayList();
        lowestCostExpressions = Maps.newHashMap();
        satisfyRequiredPropertyGroupExpressions = Maps.newHashMap();
        confidenceStatistics = Maps.newHashMap();
        isExplored = false;
    }

    public int getId() {
        return id;
    }

    public boolean hasConfidenceStatistic(PhysicalPropertySet physicalPropertySet) {
        return confidenceStatistics.containsKey(physicalPropertySet);
    }

    public Statistics getConfidenceStatistic(PhysicalPropertySet physicalPropertySet) {
        return confidenceStatistics.get(physicalPropertySet);
    }

    public boolean isExplored() {
        return isExplored;
    }

    public void setExplored() {
        isExplored = true;
    }

    public Statistics getStatistics() {
        return statistics;
    }

    public void setStatistics(Statistics statistics) {
        this.statistics = statistics;
    }

    public List<GroupExpression> getLogicalExpressions() {
        return logicalExpressions;
    }

    public List<GroupExpression> getPhysicalExpressions() {
        return physicalExpressions;
    }

    // A valid group should at least has one logical expression
    public GroupExpression getFirstLogicalExpression() {
        return logicalExpressions.get(0);
    }

    public void addExpression(GroupExpression groupExpression) {
        if (groupExpression.getOp().isLogical()) {
            Preconditions.checkState(!logicalExpressions.contains(groupExpression));
            logicalExpressions.add(groupExpression);
        } else {
            Preconditions.checkState(!physicalExpressions.contains(groupExpression));
            physicalExpressions.add(groupExpression);
        }
        groupExpression.setGroup(this);
    }

    public double getCostLowerBound() {
        return -1000;
    }

    public void setBestExpression(GroupExpression expression, double cost, PhysicalPropertySet physicalPropertySet) {
        if (lowestCostExpressions.containsKey(physicalPropertySet)) {
            if (lowestCostExpressions.get(physicalPropertySet).first > cost) {
                lowestCostExpressions.put(physicalPropertySet, new Pair<>(cost, expression));
                confidenceStatistics.put(physicalPropertySet, statistics);
            }
        } else {
            lowestCostExpressions.put(physicalPropertySet, new Pair<>(cost, expression));
            confidenceStatistics.put(physicalPropertySet, statistics);
        }
    }

    public void setBestExpressionWithStatistics(GroupExpression expression, double cost,
                                                PhysicalPropertySet physicalPropertySet,
                                                Statistics newStatistics) {
        if (lowestCostExpressions.containsKey(physicalPropertySet)) {
            if (lowestCostExpressions.get(physicalPropertySet).first > cost) {
                lowestCostExpressions.put(physicalPropertySet, new Pair<>(cost, expression));
                statistics = newStatistics;
                confidenceStatistics.put(physicalPropertySet, newStatistics);
            }
        } else {
            lowestCostExpressions.put(physicalPropertySet, new Pair<>(cost, expression));
            statistics = newStatistics;
            confidenceStatistics.put(physicalPropertySet, newStatistics);
        }
    }

    public Set<GroupExpression> getSatisfyRequiredGroupExpressions(PhysicalPropertySet requiredProperty) {
        Set<GroupExpression> groupExpressions = satisfyRequiredPropertyGroupExpressions.get(requiredProperty);
        Preconditions.checkState(groupExpressions != null);
        return groupExpressions;
    }

    public void addSatisfyRequiredPropertyGroupExpression(PhysicalPropertySet outputProperty,
                                                          GroupExpression groupExpression) {
        if (!satisfyRequiredPropertyGroupExpressions.containsKey(outputProperty)) {
            satisfyRequiredPropertyGroupExpressions.put(outputProperty, Sets.newLinkedHashSet());
        }
        satisfyRequiredPropertyGroupExpressions.get(outputProperty).add(groupExpression);
    }

    public void addSatisfyRequiredPropertyGroupExpressions(PhysicalPropertySet outputProperty,
                                                           Set<GroupExpression> groupExpressions) {
        if (!satisfyRequiredPropertyGroupExpressions.containsKey(outputProperty)) {
            satisfyRequiredPropertyGroupExpressions.put(outputProperty, Sets.newLinkedHashSet());
        }
        satisfyRequiredPropertyGroupExpressions.get(outputProperty).addAll(groupExpressions);
    }

    public void replaceBestExpressionProperty(PhysicalPropertySet oldProperty, PhysicalPropertySet newProperty,
                                              double cost) {
        Pair<Double, GroupExpression> lowestExpression = lowestCostExpressions.get(oldProperty);
        lowestExpression.second
                .updatePropertyWithCost(newProperty, lowestExpression.second.getInputProperties(oldProperty), cost);
        lowestCostExpressions.remove(oldProperty);

        lowestCostExpressions.put(newProperty, lowestExpression);
    }

    public void replaceBestExpression(GroupExpression oldGroupExpression, GroupExpression newGroupExpression) {
        Map<PhysicalPropertySet, Pair<Double, GroupExpression>> needReplaceBestExpressions = Maps.newHashMap();
        for (Iterator<Map.Entry<PhysicalPropertySet, Pair<Double, GroupExpression>>> iterator =
                lowestCostExpressions.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<PhysicalPropertySet, Pair<Double, GroupExpression>> entry = iterator.next();
            Pair<Double, GroupExpression> pair = entry.getValue();
            if (pair.second.equals(oldGroupExpression)) {
                needReplaceBestExpressions.put(entry.getKey(), new Pair<>(pair.first, newGroupExpression));
                iterator.remove();
            }
        }
        lowestCostExpressions.putAll(needReplaceBestExpressions);
    }

    public void deleteBestExpression(GroupExpression groupExpression) {
        for (Iterator<Map.Entry<PhysicalPropertySet, Pair<Double, GroupExpression>>> iterator =
                lowestCostExpressions.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<PhysicalPropertySet, Pair<Double, GroupExpression>> entry = iterator.next();
            Pair<Double, GroupExpression> pair = entry.getValue();
            GroupExpression bestExpression = pair.second;
            if (bestExpression.equals(groupExpression)) {
                iterator.remove();
            }
        }

        // we need to delete the enforcer whose input property is satisfied by the deleted group expression.
        for (Iterator<Map.Entry<PhysicalPropertySet, Pair<Double, GroupExpression>>> iterator =
                lowestCostExpressions.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<PhysicalPropertySet, Pair<Double, GroupExpression>> entry = iterator.next();
            PhysicalPropertySet requiredProperty = entry.getKey();
            Pair<Double, GroupExpression> pair = entry.getValue();
            GroupExpression bestExpression = pair.second;
            // enforcer's child group is same with itself.
            if (bestExpression.arity() == 1 && bestExpression.inputAt(0) == bestExpression.getGroup()) {
                // the enforcer need to be deleted when its input property can not be satisfied by the group
                if (!lowestCostExpressions.keySet().containsAll(bestExpression.getInputProperties(requiredProperty))) {
                    iterator.remove();
                }
            }
        }
    }

    public GroupExpression getBestExpression(PhysicalPropertySet physicalPropertySet) {
        if (hasBestExpression(physicalPropertySet)) {
            return lowestCostExpressions.get(physicalPropertySet).second;
        }
        return null;
    }

    public boolean hasBestExpression(PhysicalPropertySet physicalPropertySet) {
        return lowestCostExpressions.containsKey(physicalPropertySet);
    }

    public LogicalProperty getLogicalProperty() {
        return logicalProperty;
    }

    public void setLogicalProperty(LogicalProperty logicalProperty) {
        this.logicalProperty = logicalProperty;
    }

    public void mergeGroup(Group other) {
        other.getLogicalExpressions().removeAll(logicalExpressions);
        other.getPhysicalExpressions().removeAll(physicalExpressions);
        logicalExpressions.addAll(other.getLogicalExpressions());
        physicalExpressions.addAll(other.getPhysicalExpressions());
        other.logicalExpressions.clear();
        other.physicalExpressions.clear();
        for (Map.Entry<PhysicalPropertySet, Pair<Double, GroupExpression>> entry : other.lowestCostExpressions
                .entrySet()) {
            GroupExpression bestGroupExpression = entry.getValue().second;
            // change the enforcer itself group and child group to dst group if enforcer's group is other.
            if (bestGroupExpression.getGroup() == other) {
                bestGroupExpression.setGroup(this);
                bestGroupExpression.getInputs().set(0, this);
            }
            setBestExpressionWithStatistics(bestGroupExpression, entry.getValue().first, entry.getKey(),
                    other.hasConfidenceStatistic(entry.getKey()) ? other.getConfidenceStatistic(entry.getKey()) :
                            other.statistics);
        }
        // If statistics is null, use other statistics
        if (statistics == null) {
            statistics = other.statistics;
        }
        other.satisfyRequiredPropertyGroupExpressions.forEach(this::addSatisfyRequiredPropertyGroupExpressions);
    }

    public void removeGroupExpression(GroupExpression groupExpression) {
        if (groupExpression.getOp().isLogical()) {
            logicalExpressions.remove(groupExpression);
        } else {
            physicalExpressions.remove(groupExpression);
        }
    }

    public boolean isEmpty() {
        return logicalExpressions.isEmpty();
    }

    // When a new group create, or after rewrite, which state should be valid
    public boolean isValidInitState() {
        return logicalExpressions.size() == 1 && physicalExpressions.isEmpty();
    }

    public OptExpression extractLogicalTree() {
        GroupExpression rootExpression = logicalExpressions.get(0);
        List<OptExpression> children = Lists.newArrayList();
        for (Group group : rootExpression.getInputs()) {
            OptExpression child = group.extractLogicalTree();
            children.add(child);
        }
        OptExpression result = OptExpression.create(rootExpression.getOp(), children);
        result.setLogicalProperty(rootExpression.getGroup().getLogicalProperty());
        result.attachGroupExpression(rootExpression);
        return result;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("->  ").append("Group: ").append(id).append('\n');
        for (GroupExpression expr : logicalExpressions) {
            sb.append(expr).append('\n');
        }
        return sb.toString();
    }

    public String toPrettyString(String headlineIndent, String detailIndent) {
        StringBuilder sb = new StringBuilder();
        sb.append(headlineIndent).append("Group: ").append(id).append("\n");
        for (GroupExpression expr : logicalExpressions) {
            sb.append(expr.toPrettyString(headlineIndent, detailIndent));
        }
        for (GroupExpression expr : physicalExpressions) {
            sb.append(expr.toPrettyString(headlineIndent, detailIndent));
        }
        return sb.toString();
    }

}
