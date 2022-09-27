// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;
import java.util.stream.Collectors;

public class UseSortAggregateRule extends OptExpressionVisitor<Void, Void> implements TreeRewriteRule {
    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        if (!ConnectContext.get().getSessionVariable().isEnableSortAggregate()) {
            return root;
        }
        root.getOp().accept(this, root, null);
        return root;
    }

    @Override
    public Void visit(OptExpression optExpression, Void context) {
        for (OptExpression opt : optExpression.getInputs()) {
            opt.getOp().accept(this, opt, context);
        }

        return null;
    }

    @Override
    public Void visitPhysicalHashAggregate(OptExpression optExpression, Void context) {
        if (optExpression.getInputs().get(0).getOp().getOpType() != OperatorType.PHYSICAL_OLAP_SCAN) {
            return visit(optExpression, context);
        }

        PhysicalOlapScanOperator scan = (PhysicalOlapScanOperator) optExpression.getInputs().get(0).getOp();

        PhysicalHashAggregateOperator agg = (PhysicalHashAggregateOperator) optExpression.getOp();

        if (!agg.getType().isGlobal()) {
            return null;
        }

        OlapTable table = (OlapTable) scan.getTable();
        if (table.getKeysType() == KeysType.PRIMARY_KEYS) {
            return null;
        }

        for (ColumnRefOperator groupBy : agg.getGroupBys()) {
            if (!scan.getColRefToColumnMetaMap().containsKey(groupBy)) {
                return null;
            }

            if (!scan.getColRefToColumnMetaMap().get(groupBy).isKey()) {
                return null;
            }
        }

        List<Column> groupBys = agg.getGroupBys().stream().map(s -> scan.getColRefToColumnMetaMap().get(s)).collect(
                Collectors.toList());

        for (Column column : ((OlapTable) scan.getTable()).getSchemaByIndexId(scan.getSelectedIndexId())) {
            if (!groupBys.contains(column)) {
                break;
            }
            groupBys.remove(column);
        }

        if (groupBys.isEmpty()) {
            agg.setUseSortAgg(true);
            scan.setSortedResult(true);
        }

        return null;
    }
}
