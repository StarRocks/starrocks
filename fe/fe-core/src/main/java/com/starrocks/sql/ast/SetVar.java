// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Strings;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.system.HeartbeatFlags;
import com.starrocks.thrift.TTabletInternalParallelMode;
import org.apache.commons.lang3.StringUtils;

// change one variable.
public class SetVar implements ParseNode {

    private String variable;
    private SetType type;
    private Expr expression;
    private LiteralExpr resolvedExpression;

    public SetVar() {
    }

    public SetVar(SetType type, String variable, Expr expression) {
        this.type = type;
        this.variable = variable;
        this.expression = expression;
        if (expression instanceof LiteralExpr) {
            this.resolvedExpression = (LiteralExpr) expression;
        }
    }

    public SetVar(String variable, Expr unevaluatedExpression) {
        this.type = SetType.DEFAULT;
        this.variable = variable;
        this.expression = unevaluatedExpression;
        if (unevaluatedExpression instanceof LiteralExpr) {
            this.resolvedExpression = (LiteralExpr) unevaluatedExpression;
        }
    }

    public String getVariable() {
        return variable;
    }

    public SetType getType() {
        return type;
    }

    public void setType(SetType type) {
        this.type = type;
    }

    public Expr getExpression() {
        return expression;
    }

    public void setExpression(Expr expression) {
        this.expression = expression;
    }

    public LiteralExpr getResolvedExpression() {
        return resolvedExpression;
    }

    public void setResolvedExpression(LiteralExpr resolvedExpression) {
        this.resolvedExpression = resolvedExpression;
    }

    // Value can be null. When value is null, means to set variable to DEFAULT.
    public void analyze() {
        if (type == null) {
            type = SetType.DEFAULT;
        }

        if (Strings.isNullOrEmpty(variable)) {
            throw new SemanticException("No variable name in set statement.");
        }

        if (type == SetType.GLOBAL) {
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        "ADMIN");
            }
        }

        if (expression == null) {
            return;
        }

        // For the case like "set character_set_client = utf8", we change SlotRef to StringLiteral.
        if (expression instanceof SlotRef) {
            expression = new StringLiteral(((SlotRef) expression).getColumnName());
        }

        expression = Expr.analyzeAndCastFold(expression);

        if (!expression.isConstant()) {
            throw new SemanticException("Set statement only support constant expr.");
        }

        resolvedExpression = (LiteralExpr) expression;

        if (variable.equalsIgnoreCase(GlobalVariable.DEFAULT_ROWSET_TYPE)) {
            if (!HeartbeatFlags.isValidRowsetType(resolvedExpression.getStringValue())) {
                throw new SemanticException("Invalid rowset type, now we support {alpha, beta}.");
            }
        }

        if (getVariable().equalsIgnoreCase("prefer_join_method")) {
            String value = getResolvedExpression().getStringValue();
            if (!value.equalsIgnoreCase("broadcast") && !value.equalsIgnoreCase("shuffle")) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_VALUE_FOR_VAR, "prefer_join_method", value);
            }
        }

        // Check variable load_mem_limit value is valid
        if (getVariable().equalsIgnoreCase(SessionVariable.LOAD_MEM_LIMIT)) {
            checkNonNegativeLongVariable(SessionVariable.LOAD_MEM_LIMIT);
        }

        if (getVariable().equalsIgnoreCase(SessionVariable.QUERY_MEM_LIMIT)) {
            checkNonNegativeLongVariable(SessionVariable.QUERY_MEM_LIMIT);
        }

        try {
            // Check variable time_zone value is valid
            if (getVariable().equalsIgnoreCase(SessionVariable.TIME_ZONE)) {
                this.expression = new StringLiteral(
                        TimeUtils.checkTimeZoneValidAndStandardize(getResolvedExpression().getStringValue()));
                this.resolvedExpression = (LiteralExpr) this.expression;
            }

            if (getVariable().equalsIgnoreCase(SessionVariable.EXEC_MEM_LIMIT)) {
                this.expression = new StringLiteral(
                        Long.toString(ParseUtil.analyzeDataVolumn(getResolvedExpression().getStringValue())));
                this.resolvedExpression = (LiteralExpr) this.expression;
            }
        } catch (UserException e) {
            throw new SemanticException(e.getMessage());
        }

        if (getVariable().equalsIgnoreCase(SessionVariable.SQL_SELECT_LIMIT)) {
            checkNonNegativeLongVariable(SessionVariable.SQL_SELECT_LIMIT);
        }

        if (getVariable().equalsIgnoreCase(SessionVariable.RESOURCE_GROUP)) {
            String wgName = getResolvedExpression().getStringValue();
            if (!StringUtils.isEmpty(wgName)) {
                ResourceGroup wg = GlobalStateMgr.getCurrentState().getResourceGroupMgr().chooseResourceGroupByName(wgName);
                if (wg == null) {
                    throw new SemanticException("resource group not exists: " + wgName);
                }
            }
        }

        if (getVariable().equalsIgnoreCase(SessionVariable.TABLET_INTERNAL_PARALLEL_MODE)) {
            validateTabletInternalParallelModeValue(getResolvedExpression().getStringValue());
        }
    }

    private void checkNonNegativeLongVariable(String field) {
        String value = getResolvedExpression().getStringValue();
        try {
            long num = Long.parseLong(value);
            if (num < 0) {
                throw new SemanticException(field + " must be equal or greater than 0.");
            }
        } catch (NumberFormatException ex) {
            throw new SemanticException(field + " is not a number");
        }
    }

    private void validateTabletInternalParallelModeValue(String val) {
        try {
            TTabletInternalParallelMode.valueOf(val.toUpperCase());
        } catch (Exception ignored) {
            throw new SemanticException("Invalid tablet_internal_parallel_mode, now we support {auto, force_split}.");
        }
    }
}
