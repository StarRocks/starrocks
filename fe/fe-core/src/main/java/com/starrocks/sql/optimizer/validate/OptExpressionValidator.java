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


package com.starrocks.sql.optimizer.validate;

import com.starrocks.catalog.Type;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;

import java.util.List;
import java.util.Map;

public class OptExpressionValidator extends OptExpressionVisitor<OptExpression, Void> {

    private final boolean needValidate;

    private final long sqlMode;

    public OptExpressionValidator() {
        needValidate = needValidate();
        sqlMode = ConnectContext.get() == null ? 0 : ConnectContext.get().getSessionVariable().getSqlMode();
    }

    public void validate(OptExpression root) {
        root.initRowOutputInfo();
        visit(root, null);
    }

    @Override
    public OptExpression visitLogicalTableScan(OptExpression optExpression, Void context) {
        return commonValidate(optExpression);
    }

    @Override
    public OptExpression visitLogicalProject(OptExpression optExpression, Void context) {
        if (needValidate) {
            Map<ColumnRefOperator, ScalarOperator> map = ((LogicalProjectOperator) optExpression.getOp())
                    .getColumnRefMap();
            validateProjectionMap(map);
        }
        validateChildOpt(optExpression);
        return optExpression;
    }

    @Override
    public OptExpression visitLogicalFilter(OptExpression optExpression, Void context) {
        return commonValidate(optExpression);
    }

    @Override
    public OptExpression visitLogicalLimit(OptExpression optExpression, Void context) {
        LogicalLimitOperator limit = optExpression.getOp().cast();
        if (limit.hasOffset() && limit.isLocal()) {
            ErrorReport.reportValidateException(ErrorCode.ERR_PLAN_VALIDATE_ERROR,
                    ErrorType.INTERNAL_ERROR, optExpression, "offset limit transfer error, must be gather operator");
        }
        return commonValidate(optExpression);
    }

    @Override
    public OptExpression visitLogicalAggregate(OptExpression optExpression, Void context) {
        return commonValidate(optExpression);
    }

    @Override
    public OptExpression visitLogicalTopN(OptExpression optExpression, Void context) {
        return commonValidate(optExpression);
    }

    @Override
    public OptExpression visitLogicalJoin(OptExpression optExpression, Void context) {
        if (needValidate) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();
            validateScalarOperator(joinOperator.getOnPredicate());
        }
        return commonValidate(optExpression);
    }

    @Override
    public OptExpression visitLogicalApply(OptExpression optExpression, Void context) {
        return commonValidate(optExpression);
    }

    @Override
    public OptExpression visitLogicalAssertOneRow(OptExpression optExpression, Void context) {
        return commonValidate(optExpression);
    }

    @Override
    public OptExpression visitLogicalWindow(OptExpression optExpression, Void context) {
        return commonValidate(optExpression);
    }

    @Override
    public OptExpression visitLogicalUnion(OptExpression optExpression, Void context) {
        LogicalUnionOperator unionOperator = (LogicalUnionOperator) optExpression.getOp();
        List<ColumnRefOperator> resultCols = unionOperator.getOutputColumnRefOp();
        for (List<ColumnRefOperator> childCols : unionOperator.getChildOutputColumns()) {
            if (resultCols.size() != childCols.size()) {
                ErrorReport.reportValidateException(ErrorCode.ERR_PLAN_VALIDATE_ERROR,
                        ErrorType.INTERNAL_ERROR, optExpression, "input cols size not equal with output cols size");
            }
            for (int i = 0; i < resultCols.size(); i++) {
                Type outputType = resultCols.get(i).getType();
                Type inputType = childCols.get(i).getType();
                if (outputType.getPrimitiveType() != inputType.getPrimitiveType()) {
                    ErrorReport.reportValidateException(ErrorCode.ERR_PLAN_VALIDATE_ERROR,
                            ErrorType.INTERNAL_ERROR, optExpression, "input cols type not equal with output cols type");
                }
            }
        }
        validateChildOpt(optExpression);
        return optExpression;
    }

    @Override
    public OptExpression visitLogicalExcept(OptExpression optExpression, Void context) {
        return commonValidate(optExpression);
    }


    @Override
    public OptExpression visitLogicalIntersect(OptExpression optExpression, Void context) {
        return commonValidate(optExpression);
    }

    @Override
    public OptExpression visitLogicalValues(OptExpression optExpression, Void context) {
        return commonValidate(optExpression);
    }


    @Override
    public OptExpression visitLogicalRepeat(OptExpression optExpression, Void context) {
        return commonValidate(optExpression);
    }

    @Override
    public OptExpression visitLogicalTableFunction(OptExpression optExpression, Void context) {
        return commonValidate(optExpression);
    }

    @Override
    public OptExpression visitLogicalTreeAnchor(OptExpression optExpression, Void context) {
        return commonValidate(optExpression);
    }

    @Override
    public OptExpression visitLogicalCTEAnchor(OptExpression optExpression, Void context) {
        return commonValidate(optExpression);
    }

    @Override
    public OptExpression visitLogicalCTEProduce(OptExpression optExpression, Void context) {
        return commonValidate(optExpression);
    }

    @Override
    public OptExpression visitLogicalCTEConsume(OptExpression optExpression, Void context) {
        return commonValidate(optExpression);
    }

    private OptExpression commonValidate(OptExpression optExpression) {
        if (needValidate) {
            validateOperator(optExpression.getOp());
        }
        validateChildOpt(optExpression);
        return optExpression;
    }

    private void validateChildOpt(OptExpression optExpression) {
        for (int i = 0; i < optExpression.arity(); i++) {
            OptExpression childOpt = optExpression.inputAt(i);
            childOpt.getOp().accept(this, childOpt, null);
        }
    }


    private void validateOperator(Operator operator) {
        if (operator.getProjection() != null) {
            Map<ColumnRefOperator, ScalarOperator> map = operator.getProjection().getColumnRefMap();
            validateProjectionMap(map);
        }
        validateScalarOperator(operator.getPredicate());
    }

    private void validateProjectionMap(Map<ColumnRefOperator, ScalarOperator> map) {
        for (ScalarOperator scalarOperator : map.values()) {
            validateScalarOperator(scalarOperator);
        }
    }

    private void validateScalarOperator(ScalarOperator scalarOperator) {
        if (scalarOperator == null) {
            return;
        }

        TypeValidator typeValidator = new TypeValidator();
        scalarOperator.accept(typeValidator, null);
    }


    private class TypeValidator extends BaseScalarOperatorShuttle {

        @Override
        public ScalarOperator visitCall(CallOperator call, Void context) {
            String fnName = call.getFnName();
            if (needDateValidate()
                    && ("str_to_date".equals(fnName) || "str2date".equals(fnName))
                    && call.getChild(0).isConstantRef()) {
                checkDateType((ConstantOperator) call.getChild(0), Type.DATETIME);
            } else {
                super.visitCall(call, context);
            }
            return call;
        }

        @Override
        public ScalarOperator visitCastOperator(CastOperator operator, Void context) {
            ScalarOperator child = operator.getChild(0);
            if (needDateValidate()
                    && child.isConstantRef()
                    && operator.getType().isDateType()) {
                checkDateType((ConstantOperator) child, operator.getType());
            } else {
                super.visitCastOperator(operator, context);
            }
            return operator;
        }

        private void checkDateType(ConstantOperator constant, Type toType) {
            try {
                constant.castTo(toType);
            } catch (Exception e) {
                ErrorReport.reportValidateException(ErrorCode.ERR_INVALID_DATE_ERROR,
                        ErrorType.USER_ERROR, toType, constant.getValue());
            }
        }

        private boolean needDateValidate() {
            return (sqlMode & SqlModeHelper.MODE_FORBID_INVALID_DATE) > 0;
        }
    }

    private boolean needValidate() {
        if (ConnectContext.get() == null) {
            return false;
        }

        return (ConnectContext.get().getSessionVariable().getSqlMode() & SqlModeHelper.MODE_FORBID_INVALID_DATE) > 0;
    }
}
