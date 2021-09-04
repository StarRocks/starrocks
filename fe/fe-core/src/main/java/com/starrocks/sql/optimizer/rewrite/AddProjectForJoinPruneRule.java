// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoin;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProject;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * For prune join unnecessary predicate/on-predicate column in outputs
 */
public class AddProjectForJoinPruneRule implements PhysicalOperatorTreeRewriteRule {

    private final ColumnRefSet requiredColumns;

    public AddProjectForJoinPruneRule(ColumnRefSet requiredColumns) {
        this.requiredColumns = requiredColumns;
    }

    @Override
    public OptExpression rewrite(OptExpression root, ColumnRefFactory factory) {
        return addProject(root, (ColumnRefSet) requiredColumns.clone(), factory);
    }

    private OptExpression addProject(OptExpression root, ColumnRefSet usedColumns, ColumnRefFactory factory) {
        usedColumns.union(((PhysicalOperator) root.getOp()).getUsedColumns());

        if (OperatorType.PHYSICAL_PROJECT != root.getOp().getOpType()
                && root.getInputs().stream().anyMatch(d -> d.getOp() instanceof PhysicalHashJoin)) {
            // check child output and add project
            for (int i = 0; i < root.arity(); ++i) {
                OptExpression child = root.getInputs().get(i);

                if (!(child.getOp() instanceof PhysicalHashJoin)) {
                    continue;
                }

                if (child.getOutputColumns().getStream().allMatch(usedColumns::contains)) {
                    continue;
                }

                Map<ColumnRefOperator, ScalarOperator> projections = Maps.newHashMap();
                for (int id : child.getOutputColumns().getColumnIds()) {
                    if (!usedColumns.contains(id)) {
                        continue;
                    }

                    ColumnRefOperator ref = factory.getColumnRef(id);
                    projections.put(ref, ref);
                }

                // For count agg node or cross join node, there are empty projections, we need to add the smallest column
                if (projections.isEmpty()) {
                    ColumnRefOperator ref = Utils.findSmallestColumnRef(child.getOutputColumns().getStream().
                            mapToObj(factory::getColumnRef).collect(Collectors.toList()));
                    projections.put(ref, ref);
                }

                PhysicalProject projects = new PhysicalProject(projections, Maps.newHashMap());

                OptExpression opt = OptExpression.create(projects, child);
                opt.setStatistics(child.getStatistics());
                ColumnRefSet outputProperty = new ColumnRefSet();
                projections.keySet().forEach(outputProperty::union);
                opt.setLogicalProperty(new LogicalProperty(outputProperty));
                root.getInputs().set(i, opt);
            }
        }

        for (int i = 0; i < root.arity(); ++i) {
            root.setChild(i, addProject(root.inputAt(i), usedColumns, factory));
        }

        return root;
    }
}
