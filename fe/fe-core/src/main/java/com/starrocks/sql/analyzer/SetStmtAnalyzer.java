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

package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.Subquery;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.common.util.CompressionUtils;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.monitor.unit.TimeValue;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SessionVariableConstants;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetNamesVar;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetUserPropertyVar;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.system.HeartbeatFlags;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TTabletInternalParallelMode;
import com.starrocks.thrift.TWorkGroup;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class SetStmtAnalyzer {
    public static void analyze(SetStmt setStmt, ConnectContext session) {
        List<SetListItem> setVars = setStmt.getSetListItems();
        for (SetListItem var : setVars) {
            if (var instanceof SystemVariable) {
                analyzeSystemVariable((SystemVariable) var);
            } else if (var instanceof UserVariable) {
                analyzeUserVariable((UserVariable) var);
            } else if (var instanceof SetUserPropertyVar) {
                analyzeSetUserPropertyVar((SetUserPropertyVar) var);
            } else if (var instanceof SetNamesVar) {
                analyzeSetNames((SetNamesVar) var);
            } else if (var instanceof SetPassVar) {
                analyzeSetPassVar((SetPassVar) var, session);
            }
        }
    }

    private static void analyzeSystemVariable(SystemVariable var) {
        String variable = var.getVariable();
        if (Strings.isNullOrEmpty(variable)) {
            throw new SemanticException("No variable name in set statement.");
        }

        Expr unResolvedExpression = var.getUnResolvedExpression();
        LiteralExpr resolvedExpression;

        if (unResolvedExpression == null) {
            // SET var = DEFAULT
            resolvedExpression = new StringLiteral(VariableMgr.getDefaultValue(var.getVariable()));
        } else if (unResolvedExpression instanceof SlotRef) {
            resolvedExpression = new StringLiteral(((SlotRef) unResolvedExpression).getColumnName());
        } else {
            Expr e = Expr.analyzeAndCastFold(unResolvedExpression);
            if (!e.isConstant()) {
                throw new SemanticException("Set statement only support constant expr.");
            }
            resolvedExpression = (LiteralExpr) e;
        }

        if (variable.equalsIgnoreCase(GlobalVariable.DEFAULT_ROWSET_TYPE)) {
            if (!HeartbeatFlags.isValidRowsetType(resolvedExpression.getStringValue())) {
                throw new SemanticException("Invalid rowset type, now we support {alpha, beta}.");
            }
        }

        if (variable.equalsIgnoreCase("prefer_join_method")) {
            String value = resolvedExpression.getStringValue();
            if (!value.equalsIgnoreCase("broadcast") && !value.equalsIgnoreCase("shuffle")) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_VALUE_FOR_VAR, "prefer_join_method", value);
            }
        }

        // Check variable load_mem_limit value is valid
        if (variable.equalsIgnoreCase(SessionVariable.LOAD_MEM_LIMIT)) {
            checkRangeLongVariable(resolvedExpression, SessionVariable.LOAD_MEM_LIMIT, 0L, null);
        }

        if (variable.equalsIgnoreCase(SessionVariable.QUERY_MEM_LIMIT)) {
            checkRangeLongVariable(resolvedExpression, SessionVariable.QUERY_MEM_LIMIT, 0L, null);
        }

        try {
            // Check variable time_zone value is valid
            if (variable.equalsIgnoreCase(SessionVariable.TIME_ZONE)) {
                resolvedExpression = new StringLiteral(
                        TimeUtils.checkTimeZoneValidAndStandardize(resolvedExpression.getStringValue()));
            }

            if (variable.equalsIgnoreCase(SessionVariable.EXEC_MEM_LIMIT)) {
                resolvedExpression = new StringLiteral(
                        Long.toString(ParseUtil.analyzeDataVolume(resolvedExpression.getStringValue())));
                checkRangeLongVariable(resolvedExpression, SessionVariable.EXEC_MEM_LIMIT,
                        SessionVariable.MIN_EXEC_MEM_LIMIT, null);
            }
        } catch (UserException e) {
            throw new SemanticException(e.getMessage());
        }

        if (variable.equalsIgnoreCase(SessionVariable.SQL_SELECT_LIMIT)) {
            checkRangeLongVariable(resolvedExpression, SessionVariable.SQL_SELECT_LIMIT, 0L, null);
        }

        if (variable.equalsIgnoreCase(SessionVariable.QUERY_TIMEOUT)) {
            checkRangeLongVariable(resolvedExpression, SessionVariable.QUERY_TIMEOUT,
                    1L, (long) SessionVariable.MAX_QUERY_TIMEOUT);
        }

        if (variable.equalsIgnoreCase(SessionVariable.NEW_PLANNER_OPTIMIZER_TIMEOUT)) {
            checkRangeLongVariable(resolvedExpression, SessionVariable.NEW_PLANNER_OPTIMIZER_TIMEOUT, 1L, null);
        }

        if (variable.equalsIgnoreCase(SessionVariable.RESOURCE_GROUP)) {
            String rgName = resolvedExpression.getStringValue();
            if (!StringUtils.isEmpty(rgName)) {
                TWorkGroup wg =
                        GlobalStateMgr.getCurrentState().getResourceGroupMgr().chooseResourceGroupByName(rgName);
                if (wg == null) {
                    throw new SemanticException("resource group not exists: " + rgName);
                }
            }
        } else if (variable.equalsIgnoreCase(SessionVariable.RESOURCE_GROUP_ID) ||
                variable.equalsIgnoreCase(SessionVariable.RESOURCE_GROUP_ID_V2)) {
            long rgID = resolvedExpression.getLongValue();
            if (rgID > 0) {
                TWorkGroup wg =
                        GlobalStateMgr.getCurrentState().getResourceGroupMgr().chooseResourceGroupByID(rgID);
                if (wg == null) {
                    throw new SemanticException("resource group not exists: " + rgID);
                }
            }
        }

        if (variable.equalsIgnoreCase(SessionVariable.TABLET_INTERNAL_PARALLEL_MODE)) {
            validateTabletInternalParallelModeValue(resolvedExpression.getStringValue());
        }

        if (variable.equalsIgnoreCase(SessionVariable.DEFAULT_TABLE_COMPRESSION)) {
            String compressionName = resolvedExpression.getStringValue();
            TCompressionType compressionType = CompressionUtils.getCompressTypeByName(compressionName);
            if (compressionType == null) {
                throw new SemanticException(String.format("Unsupported compression type: %s, supported list is %s",
                        compressionName, StringUtils.join(CompressionUtils.getSupportedCompressionNames(), ",")));
            }
        }

        if (variable.equalsIgnoreCase(SessionVariable.ADAPTIVE_DOP_MAX_BLOCK_ROWS_PER_DRIVER_SEQ)) {
            checkRangeLongVariable(resolvedExpression, SessionVariable.ADAPTIVE_DOP_MAX_BLOCK_ROWS_PER_DRIVER_SEQ, 1L, null);
        }

        if (variable.equalsIgnoreCase(SessionVariable.CBO_EQ_BASE_TYPE)) {
            String baseType = resolvedExpression.getStringValue();
            if (!baseType.equalsIgnoreCase(SessionVariableConstants.VARCHAR) &&
                    !baseType.equalsIgnoreCase(SessionVariableConstants.DECIMAL)) {
                throw new SemanticException(String.format("Unsupported cbo_eq_base_type: %s, " +
                        "supported list is {varchar, decimal}", baseType));
            }
        }

        // follower_query_forward_mode
        if (variable.equalsIgnoreCase(SessionVariable.FOLLOWER_QUERY_FORWARD_MODE)) {
            String queryFollowerForwardMode = resolvedExpression.getStringValue();
            if (!EnumUtils.isValidEnumIgnoreCase(SessionVariable.FollowerQueryForwardMode.class, queryFollowerForwardMode)) {
                String supportedList = StringUtils.join(
                        EnumUtils.getEnumList(SessionVariable.FollowerQueryForwardMode.class), ",");
                throw new SemanticException(String.format("Unsupported follower query forward mode: %s, " +
                        "supported list is %s", queryFollowerForwardMode, supportedList));
            }
        }

        // query_debug_options
        if (variable.equalsIgnoreCase(SessionVariable.QUERY_DEBUG_OPTIONS)) {
            String queryDebugOptions = resolvedExpression.getStringValue();
            try {
                QueryDebugOptions.read(queryDebugOptions);
            } catch (Exception e) {
                throw new SemanticException(String.format("Unsupported query_debug_options: %s, " +
                        "it should be the `QueryDebugOptions` class's json deserialized string", queryDebugOptions));
            }
        }

        // big_query_profile_threshold
        if (variable.equalsIgnoreCase(SessionVariable.BIG_QUERY_PROFILE_THRESHOLD)) {
            String timeStr = resolvedExpression.getStringValue();
            TimeValue timeValue = TimeValue.parseTimeValue(timeStr, null);
            if (timeValue == null) {
                throw new SemanticException(String.format("failed to parse time value %s", timeStr));
            }
        }

        // cbo_materialized_view_rewrite_candidate_limit
        if (variable.equalsIgnoreCase(SessionVariable.CBO_MATERIALIZED_VIEW_REWRITE_CANDIDATE_LIMIT)) {
            checkRangeIntVariable(resolvedExpression, SessionVariable.CBO_MATERIALIZED_VIEW_REWRITE_CANDIDATE_LIMIT,
                    1, null);
        }
        // cbo_materialized_view_rewrite_rule_output_limit
        if (variable.equalsIgnoreCase(SessionVariable.CBO_MATERIALIZED_VIEW_REWRITE_RULE_OUTPUT_LIMIT)) {
            checkRangeIntVariable(resolvedExpression, SessionVariable.CBO_MATERIALIZED_VIEW_REWRITE_RULE_OUTPUT_LIMIT,
                    1, null);
        }
        // cbo_materialized_view_rewrite_related_mvs_limit
        if (variable.equalsIgnoreCase(SessionVariable.CBO_MATERIALIZED_VIEW_REWRITE_RELATED_MVS_LIMIT)) {
            checkRangeIntVariable(resolvedExpression, SessionVariable.CBO_MATERIALIZED_VIEW_REWRITE_RELATED_MVS_LIMIT,
                    1, null);
        }

        var.setResolvedExpression(resolvedExpression);
    }

    private static void checkRangeLongVariable(LiteralExpr resolvedExpression, String field, Long min, Long max) {
        String value = resolvedExpression.getStringValue();
        try {
            long num = Long.parseLong(value);
            if (min != null && num < min) {
                throw new SemanticException(String.format("%s must be equal or greater than %d", field, min));
            }
            if (max != null && num > max) {
                throw new SemanticException(String.format("%s must be equal or smaller than %d", field, max));
            }
        } catch (NumberFormatException ex) {
            throw new SemanticException(field + " is not a number");
        }
    }

    private static void checkRangeIntVariable(LiteralExpr resolvedExpression, String field, Integer min, Integer max) {
        String value = resolvedExpression.getStringValue();
        try {
            int num = Integer.parseInt(value);
            if (min != null && num < min) {
                throw new SemanticException(String.format("%s must be equal or greater than %d", field, min));
            }
            if (max != null && num > max) {
                throw new SemanticException(String.format("%s must be equal or smaller than %d", field, max));
            }
        } catch (NumberFormatException ex) {
            throw new SemanticException(field + " is not a number");
        }
    }

    private static void validateTabletInternalParallelModeValue(String val) {
        try {
            TTabletInternalParallelMode.valueOf(val.toUpperCase());
        } catch (Exception ignored) {
            throw new SemanticException("Invalid tablet_internal_parallel_mode, now we support {auto, force_split}");
        }
    }

    private static void analyzeUserVariable(UserVariable var) {
        if (var.getVariable().length() > 64) {
            throw new SemanticException("User variable name '" + var.getVariable() + "' is illegal");
        }

        Expr expression = var.getUnevaluatedExpression();
        if (expression instanceof NullLiteral) {
            var.setEvaluatedExpression(NullLiteral.create(Type.STRING));
        } else {
            Expr foldedExpression = Expr.analyzeAndCastFold(expression);
            if (foldedExpression instanceof LiteralExpr) {
                var.setEvaluatedExpression((LiteralExpr) foldedExpression);
            } else {
                SelectList selectList = new SelectList(Lists.newArrayList(
                        new SelectListItem(var.getUnevaluatedExpression(), null)), false);

                List<Expr> row = Lists.newArrayList(NullLiteral.create(Type.NULL));
                List<List<Expr>> rows = new ArrayList<>();
                rows.add(row);
                ValuesRelation valuesRelation = new ValuesRelation(rows, Lists.newArrayList(""));
                valuesRelation.setNullValues(true);

                SelectRelation selectRelation = new SelectRelation(selectList, valuesRelation, null, null, null);
                QueryStatement queryStatement = new QueryStatement(selectRelation);
                Analyzer.analyze(queryStatement, ConnectContext.get());

                Expr variableResult = queryStatement.getQueryRelation().getOutputExpression().get(0);

                //can not apply to numeric types or complex type are not supported
                if (variableResult.getType().isOnlyMetricType() || variableResult.getType().isFunctionType()
                        || variableResult.getType().isComplexType()) {
                    throw new SemanticException("Can't set variable with type " + variableResult.getType());
                }

                ((SelectRelation) queryStatement.getQueryRelation()).getSelectList().getItems().set(0,
                        new SelectListItem(new CastExpr(Type.VARCHAR, variableResult), null));

                Subquery subquery = new Subquery(queryStatement);
                subquery.setType(variableResult.getType());
                var.setUnevaluatedExpression(subquery);
            }
        }
    }

    private static void analyzeSetUserPropertyVar(SetUserPropertyVar setUserPropertyVar) {
        if (Strings.isNullOrEmpty(setUserPropertyVar.getPropertyKey())) {
            throw new SemanticException("User property key is null");
        }

        if (setUserPropertyVar.getPropertyValue() == null) {
            throw new SemanticException("User property value is null");
        }

        if (!setUserPropertyVar.getPropertyKey().equals("max_user_connections")) {
            throw new SemanticException("Unknown property key: " + setUserPropertyVar.getPropertyKey());
        }
    }

    private static void analyzeSetNames(SetNamesVar var) {
        String charset = var.getCharset();

        if (Strings.isNullOrEmpty(charset)) {
            charset = SetNamesVar.DEFAULT_NAMES;
        } else {
            charset = charset.toLowerCase();
        }
        // utf8-superset transform to utf8
        if (charset.startsWith(SetNamesVar.DEFAULT_NAMES)) {
            charset = SetNamesVar.DEFAULT_NAMES;
        }

        if (!charset.equalsIgnoreCase(SetNamesVar.DEFAULT_NAMES) && !charset.equalsIgnoreCase(SetNamesVar.GBK_NAMES)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_UNKNOWN_CHARACTER_SET, charset);
        }
        // be is not supported yet,so Display unsupported information to the user
        if (!charset.equalsIgnoreCase(SetNamesVar.DEFAULT_NAMES)) {
            throw new SemanticException("charset name " + charset + " is not supported yet");
        }

        var.setCharset(charset);
    }

    private static void analyzeSetPassVar(SetPassVar var, ConnectContext session) {
        try {
            UserIdentity userIdentity = var.getUserIdent();
            if (userIdentity == null) {
                userIdentity = session.getCurrentUserIdentity();
            }
            userIdentity.analyze();
            var.setUserIdent(userIdentity);
            var.setPasswdBytes(MysqlPassword.checkPassword(var.getPasswdParam()));

        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }
    }
}
