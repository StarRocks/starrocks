package com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;

public class DateTruncReplaceChecker implements PredicateReplaceChecker {

    private final CallOperator mvDateTrunc;

    public DateTruncReplaceChecker(CallOperator mvDateTrunc) {
        this.mvDateTrunc = mvDateTrunc;
    }

    @Override
    public boolean canReplace(ScalarOperator operator) {
        if (operator.isConstantRef() && operator.getType().getPrimitiveType() == PrimitiveType.DATETIME) {
            ConstantOperator sliced = ScalarOperatorFunctions.dateTrunc(
                    ((ConstantOperator) mvDateTrunc.getChild(0)),
                    (ConstantOperator) operator);
            return sliced.equals(operator);
        }
        return false;
    }
}
