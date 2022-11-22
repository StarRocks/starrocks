// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
<<<<<<< HEAD
=======
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
>>>>>>> 5dba41fcc ([BugFix] fix output column of anti join (#13760))
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

<<<<<<< HEAD
=======
import java.util.Map;
import java.util.stream.Collectors;

>>>>>>> 5dba41fcc ([BugFix] fix output column of anti join (#13760))
public abstract class LogicalOperator extends Operator {

    protected LogicalOperator(OperatorType opType) {
        super(opType);
    }

    protected LogicalOperator(OperatorType operatorType, long limit, ScalarOperator predicate, Projection projection) {
        super(operatorType, limit, predicate, projection);
    }

    @Override
    public boolean isLogical() {
        return true;
    }

    public abstract ColumnRefSet getOutputColumns(ExpressionContext expressionContext);
<<<<<<< HEAD
=======

    public ColumnRefOperator getSmallestColumn(ColumnRefFactory columnRefFactory, OptExpression opt) {
        return Utils.findSmallestColumnRef(
                getOutputColumns(new ExpressionContext(opt)).getStream().
                        mapToObj(columnRefFactory::getColumnRef).collect(Collectors.toList()));
    }

    // lineage means the merge of operator's column ref map, which is used to track
    // what does the ColumnRefOperator come from.
    public Map<ColumnRefOperator, ScalarOperator> getLineage(
            ColumnRefFactory refFactory, ExpressionContext expressionContext) {
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = Maps.newHashMap();
        if (projection != null) {
            columnRefMap.putAll(projection.getColumnRefMap());
        } else {
            ColumnRefSet refSet = getOutputColumns(expressionContext);
            for (int columnId : refSet.getColumnIds()) {
                ColumnRefOperator columnRef = refFactory.getColumnRef(columnId);
                columnRefMap.put(columnRef, columnRef);
            }
        }
        return columnRefMap;
    }
>>>>>>> 5dba41fcc ([BugFix] fix output column of anti join (#13760))
}
