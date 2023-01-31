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


package com.starrocks.sql.ast;

import com.google.common.base.Strings;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.system.HeartbeatFlags;
import com.starrocks.thrift.TTabletInternalParallelMode;
import com.starrocks.thrift.TWorkGroup;
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
            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            if (globalStateMgr.isUsingNewPrivilege()) {
                if (!PrivilegeManager.checkSystemAction(ConnectContext.get(), PrivilegeType.OPERATE)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "OPERATE");
                }
            } else {
                if (!globalStateMgr.getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
                }
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
            checkRangeLongVariable(SessionVariable.LOAD_MEM_LIMIT, 0L, null);
        }

        if (getVariable().equalsIgnoreCase(SessionVariable.QUERY_MEM_LIMIT)) {
            checkRangeLongVariable(SessionVariable.QUERY_MEM_LIMIT, 0L, null);
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
                checkRangeLongVariable(SessionVariable.EXEC_MEM_LIMIT, SessionVariable.MIN_EXEC_MEM_LIMIT, null);
            }
        } catch (UserException e) {
            throw new SemanticException(e.getMessage());
        }

        if (getVariable().equalsIgnoreCase(SessionVariable.SQL_SELECT_LIMIT)) {
            checkRangeLongVariable(SessionVariable.SQL_SELECT_LIMIT, 0L, null);
        }

        if (getVariable().equalsIgnoreCase(SessionVariable.QUERY_TIMEOUT)) {
            checkRangeLongVariable(SessionVariable.QUERY_TIMEOUT, 1L, (long) SessionVariable.MAX_QUERY_TIMEOUT);
        }

        if (getVariable().equalsIgnoreCase(SessionVariable.NEW_PLANNER_OPTIMIZER_TIMEOUT)) {
            checkRangeLongVariable(SessionVariable.NEW_PLANNER_OPTIMIZER_TIMEOUT, 1L, null);
        }

        if (getVariable().equalsIgnoreCase(SessionVariable.RESOURCE_GROUP)) {
            String rgName = getResolvedExpression().getStringValue();
            if (!StringUtils.isEmpty(rgName)) {
                TWorkGroup wg =
                        GlobalStateMgr.getCurrentState().getResourceGroupMgr().chooseResourceGroupByName(rgName);
                if (wg == null) {
                    throw new SemanticException("resource group not exists: " + rgName);
                }
            }
        } else if (getVariable().equalsIgnoreCase(SessionVariable.RESOURCE_GROUP_ID) ||
                getVariable().equalsIgnoreCase(SessionVariable.RESOURCE_GROUP_ID_V2)) {
            long rgID = getResolvedExpression().getLongValue();
            if (rgID > 0) {
                TWorkGroup wg =
                        GlobalStateMgr.getCurrentState().getResourceGroupMgr().chooseResourceGroupByID(rgID);
                if (wg == null) {
                    throw new SemanticException("resource group not exists: " + rgID);
                }
            }
        }

        if (getVariable().equalsIgnoreCase(SessionVariable.TABLET_INTERNAL_PARALLEL_MODE)) {
            validateTabletInternalParallelModeValue(getResolvedExpression().getStringValue());
        }
    }

    private void checkRangeLongVariable(String field, Long min, Long max) {
        String value = getResolvedExpression().getStringValue();
        try {
            long num = Long.parseLong(value);
            if (min != null && num < min) {
                throw new SemanticException(String.format("%s must be equal or greater than %d.", field, min));
            }
            if (max != null && num > max) {
                throw new SemanticException(String.format("%s must be equal or smaller than %d.", field, max));
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
