// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    private boolean hasExplored = false;

    private final List<GroupExpression> logicalExpressions;
    private final List<GroupExpression> physicalExpressions;

    private Statistics statistics;
    // confidence statistics record the statistics when group expression has lowest cost,
    // confidence statistics is the statistics in group with highest confidence
    private Statistics confidenceStatistics;
    private final Map<PhysicalPropertySet, Pair<Double, GroupExpression>> lowestCostExpressions;
    // GroupExpressions in this Group which could satisfy the required property.
    private final Map<PhysicalPropertySet, Set<GroupExpression>> satisfyRequiredPropertyGroupExpressions;

    // All expressions in one group have same logical property.
    private LogicalProperty logicalProperty;

    private double costLowerBound = -1000;

    public Group(int groupId) {
        this.id = groupId;
        logicalExpressions = Lists.newArrayList();
        physicalExpressions = Lists.newArrayList();
        lowestCostExpressions = Maps.newHashMap();
        satisfyRequiredPropertyGroupExpressions = Maps.newHashMap();
    }

    public int getId() {
        return id;
    }

    public boolean hasExplored() {
        return hasExplored;
    }

    public void setHasExplored() {
        hasExplored = true;
    }

    public Statistics getStatistics() {
        return statistics;
    }

    public Statistics getConfidenceStatistics() {
        return confidenceStatistics;
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
            logicalExpressions.add(groupExpression);
        } else {
            physicalExpressions.add(groupExpression);
        }
        groupExpression.setGroup(this);
    }

    public double getCostLowerBound() {
        return costLowerBound;
    }

    public void setBestExpression(GroupExpression expression, double cost, PhysicalPropertySet physicalPropertySet) {
        if (lowestCostExpressions.containsKey(physicalPropertySet)) {
            if (lowestCostExpressions.get(physicalPropertySet).first > cost) {
                lowestCostExpressions.put(physicalPropertySet, new Pair<>(cost, expression));
                confidenceStatistics = statistics;
            }
        } else {
            lowestCostExpressions.put(physicalPropertySet, new Pair<>(cost, expression));
            confidenceStatistics = statistics;
        }
    }

    public void setBestExpressionWithStatistics(GroupExpression expression, double cost,
                                                PhysicalPropertySet physicalPropertySet,
                                                Statistics newStatistics) {
        if (lowestCostExpressions.containsKey(physicalPropertySet)) {
            if (lowestCostExpressions.get(physicalPropertySet).first > cost) {
                lowestCostExpressions.put(physicalPropertySet, new Pair<>(cost, expression));
                statistics = newStatistics;
                confidenceStatistics = newStatistics;
            }
        } else {
            lowestCostExpressions.put(physicalPropertySet, new Pair<>(cost, expression));
            statistics = newStatistics;
            confidenceStatistics = newStatistics;
        }
    }

    public Set<GroupExpression> getSatisfyRequiredGroupExpressions(PhysicalPropertySet requiredProperty) {
        return satisfyRequiredPropertyGroupExpressions.get(requiredProperty);
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
                .setPropertyWithCost(newProperty, lowestExpression.second.getInputProperties(oldProperty), cost);
        lowestCostExpressions.remove(oldProperty);

        lowestCostExpressions.put(newProperty, lowestExpression);
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
        for (Map.Entry<PhysicalPropertySet, Pair<Double, GroupExpression>> entry : other.lowestCostExpressions
                .entrySet()) {
            setBestExpressionWithStatistics(entry.getValue().second, entry.getValue().first, entry.getKey(),
                    other.confidenceStatistics != null ? other.confidenceStatistics : other.statistics);
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
        for (GroupExpression expr : physicalExpressions) {
            sb.append(expr).append('\n');
        }
        return sb.toString();
    }

}
