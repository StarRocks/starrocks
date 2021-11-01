// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A group-expression is the same as an expression except
 * it takes groups as inputs while expressions take other expressions as inputs.
 * <p>
 * With group-expressions, a group can be re-defined as
 * a set of logically equivalent group-expressions.
 * <p>
 * With group-expressions, we could reduces
 * the number of transformations, storage overhead, and repeated cost estimations.
 */
public class GroupExpression {
    // The group this group expression belong to,
    // will set by setGroup method
    private Group group;
    private final List<Group> inputs;
    private final Operator op;
    private final BitSet ruleMasks = new BitSet(RuleType.NUM_RULES.ordinal() + 1);
    private boolean statsDerived = false;
    private final Map<PhysicalPropertySet, Pair<Double, List<PhysicalPropertySet>>> lowestCostTable;
    private final Set<Pair<PhysicalPropertySet, List<PhysicalPropertySet>>> validOutputInputProperties;
    private Map<Pair<PhysicalPropertySet, List<PhysicalPropertySet>>, Integer> propertiesPlanCountMap;

    public GroupExpression(Operator op, List<Group> inputs) {
        this.op = op;
        this.inputs = inputs;
        this.lowestCostTable = Maps.newHashMap();
        this.validOutputInputProperties = Sets.newLinkedHashSet();
        this.propertiesPlanCountMap = Maps.newLinkedHashMap();
    }

    public Group getGroup() {
        return group;
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    public List<Group> getInputs() {
        return inputs;
    }

    public int arity() {
        return inputs.size();
    }

    public Group inputAt(int i) {
        return inputs.get(i);
    }

    public Operator getOp() {
        return op;
    }

    public boolean isStatsDerived() {
        return statsDerived;
    }

    public boolean isUnused() {
        return hasEmptyRootGroup() || hasEmptyChildGroup();
    }

    private boolean hasEmptyChildGroup() {
        return inputs.stream().anyMatch(Group::isEmpty);
    }

    public boolean hasEmptyRootGroup() {
        return group.isEmpty();
    }

    public void setStatsDerived() {
        statsDerived = true;
    }

    public void setRuleExplored(Rule rule) {
        ruleMasks.set(rule.type().ordinal());
    }

    public boolean hasRuleExplored(Rule rule) {
        return ruleMasks.get(rule.type().ordinal());
    }

    public void addValidOutputInputProperties(PhysicalPropertySet outputProperty,
                                              List<PhysicalPropertySet> inputProperties) {
        validOutputInputProperties.add(new Pair<>(outputProperty, inputProperties));
    }

    public List<List<PhysicalPropertySet>> getRequiredInputProperties(PhysicalPropertySet requiredProperty) {
        List<List<PhysicalPropertySet>> result = Lists.newArrayList();
        for (Pair<PhysicalPropertySet, List<PhysicalPropertySet>> outputInputProperty : validOutputInputProperties) {
            if (outputInputProperty.first.equals(requiredProperty)) {
                result.add(outputInputProperty.second);
            }
        }
        return result;
    }

    public boolean hasValidSubPlan() {
        return !validOutputInputProperties.isEmpty();
    }

    public void addPlanCountOfProperties(Pair<PhysicalPropertySet, List<PhysicalPropertySet>> properties, int count) {
        propertiesPlanCountMap.put(properties, count);
    }

    public Map<Pair<PhysicalPropertySet, List<PhysicalPropertySet>>, Integer> getPropertiesPlanCountMap(
            PhysicalPropertySet requiredProperty) {
        Map<Pair<PhysicalPropertySet, List<PhysicalPropertySet>>, Integer> result = Maps.newLinkedHashMap();
        propertiesPlanCountMap.entrySet().stream().filter(entry -> entry.getKey().first.equals(requiredProperty))
                .forEach(entry -> result.put(entry.getKey(), entry.getValue()));
        return result;
    }

    public int getRequiredPropertyPlanCount(PhysicalPropertySet requiredProperty) {
        return propertiesPlanCountMap.entrySet().stream().filter(entry -> entry.getKey().first.equals(requiredProperty))
                .mapToInt(Map.Entry::getValue).sum();
    }

    /**
     * Retrieves the lowest cost satisfying a given set of properties
     *
     * @param require PropertySet that needs to be satisfied
     * @return Lowest cost to satisfy that PropertySet
     */
    public double getCost(PhysicalPropertySet require) {
        Preconditions.checkState(lowestCostTable.containsKey(require));
        return lowestCostTable.get(require).first;
    }

    /**
     * Gets the input properties needed for a given required properties
     *
     * @param require PhysicalPropertySet that needs to be satisfied
     * @return List of children input physical properties required
     */
    public List<PhysicalPropertySet> getInputProperties(PhysicalPropertySet require) {
        Preconditions.checkState(lowestCostTable.containsKey(require));
        return lowestCostTable.get(require).second;
    }

    /**
     * Add a mapping from output_properties to
     * a pair of lowest cost and vector of children input properties.
     *
     * @param outputProperties PropertySet that is satisfied
     * @param inputProperties  List of children input properties required
     * @param cost             Cost
     */
    public void setPropertyWithCost(PhysicalPropertySet outputProperties,
                                    List<PhysicalPropertySet> inputProperties,
                                    double cost) {
        if (lowestCostTable.containsKey(outputProperties)) {
            if (lowestCostTable.get(outputProperties).first > cost) {
                lowestCostTable.put(outputProperties, new Pair<>(cost, inputProperties));
            }
        } else {
            lowestCostTable.put(outputProperties, new Pair<>(cost, inputProperties));
        }
    }

    // This function will drive input group logical property first,
    // then derive itself's logical property
    public void deriveLogicalPropertyRecursively() {
        for (Group group : inputs) {
            group.getFirstLogicalExpression().deriveLogicalPropertyRecursively();
        }

        deriveLogicalPropertyItself();
    }

    // This function assume the child group logical property has been derived
    public void deriveLogicalPropertyItself() {
        ExpressionContext context = new ExpressionContext(this);
        context.deriveLogicalProperty();
        getGroup().setLogicalProperty(context.getRootProperty());
    }

    public ColumnRefSet getChildOutputColumns(int index) {
        return inputAt(index).getLogicalProperty().getOutputColumns();
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, inputs);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof GroupExpression)) {
            return false;
        }
        GroupExpression rhs = (GroupExpression) obj;
        if (this == rhs) {
            return true;
        }
        if (!op.equals(rhs.getOp())) {
            return false;
        }
        if (arity() != rhs.arity()) {
            return false;
        }
        for (int i = 0; i < arity(); ++i) {
            if (inputs.get(i).getId() != (rhs.inputAt(i).getId())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n {root group ");
        if (group != null) {
            sb.append(group.getId());
        } else {
            sb.append(-1);
        }
        sb.append("\t root operator: ")
                .append(op).append('\n')
                .append("\t child group id ");
        for (Group input : inputs) {
            sb.append("\t").append(input.getId());
        }
        sb.append("}");
        return sb.toString();
    }
}
