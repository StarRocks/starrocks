// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.Subquery;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetVar;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.ast.ValuesRelation;

import java.util.ArrayList;
import java.util.List;

public class SetStmtAnalyzer {
    public static void analyze(SetStmt setStmt, ConnectContext session) {
        List<SetVar> setVars = setStmt.getSetVars();
        for (SetVar var : setVars) {
            if (var instanceof UserVariable) {
                if (var.getVariable().length() > 64) {
                    throw new SemanticException("User variable name '" + var.getVariable() + "' is illegal");
                }

                Expr expression = var.getExpression();
                if (expression instanceof NullLiteral) {
                    var.setResolvedExpression(NullLiteral.create(Type.STRING));
                } else {
                    Expr foldedExpression = Expr.analyzeAndCastFold(expression);
                    if (foldedExpression instanceof LiteralExpr) {
                        var.setResolvedExpression((LiteralExpr) foldedExpression);
                    } else {
                        SelectList selectList = new SelectList(Lists.newArrayList(
                                new SelectListItem(var.getExpression(), null)), false);

                        ArrayList<Expr> row = Lists.newArrayList(NullLiteral.create(Type.NULL));
                        List<ArrayList<Expr>> rows = new ArrayList<>();
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
                        var.setExpression(subquery);
                    }
                }
<<<<<<< HEAD
            } else {
                //TODO: Unify the analyze logic of other types of SetVar from the original definition
                var.analyze();
            }
        }
    }
=======
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
            checkRangeLongVariable(resolvedExpression, SessionVariable.ADAPTIVE_DOP_MAX_BLOCK_ROWS_PER_DRIVER_SEQ, 1L,
                    null);
        }

        if (variable.equalsIgnoreCase(SessionVariable.CHOOSE_EXECUTE_INSTANCES_MODE)) {
            SessionVariableConstants.ChooseInstancesMode mode =
                    Enums.getIfPresent(SessionVariableConstants.ChooseInstancesMode.class,
                            StringUtils.upperCase(resolvedExpression.getStringValue())).orNull();
            if (mode == null) {
                String legalValues = Joiner.on(" | ").join(SessionVariableConstants.ChooseInstancesMode.values());
                throw new IllegalArgumentException("Legal values of choose_execute_instances_mode are " + legalValues);
            }
        }

        if (variable.equalsIgnoreCase(SessionVariable.COMPUTATION_FRAGMENT_SCHEDULING_POLICY)) {
            String policy = resolvedExpression.getStringValue();
            SessionVariableConstants.ComputationFragmentSchedulingPolicy computationFragmentSchedulingPolicy =
                    Enums.getIfPresent(SessionVariableConstants.ComputationFragmentSchedulingPolicy.class,
                            StringUtils.upperCase(policy)).orNull();
            if (computationFragmentSchedulingPolicy == null) {
                String supportedList = Joiner.on(",").join(SessionVariableConstants.ComputationFragmentSchedulingPolicy.values());
                throw new SemanticException(String.format("Unsupported computation_fragment_scheduling_policy: %s, " +
                        "supported list is %s", policy, supportedList));
            }
        }

        // materialized_view_rewrite_mode
        if (variable.equalsIgnoreCase(SessionVariable.MATERIALIZED_VIEW_REWRITE_MODE)) {
            String rewriteModeName = resolvedExpression.getStringValue();
            if (!EnumUtils.isValidEnumIgnoreCase(SessionVariable.MaterializedViewRewriteMode.class, rewriteModeName)) {
                String supportedList = StringUtils.join(
                        EnumUtils.getEnumList(SessionVariable.MaterializedViewRewriteMode.class), ",");
                throw new SemanticException(String.format("Unsupported materialized view rewrite mode: %s, " +
                        "supported list is %s", rewriteModeName, supportedList));
            }
        }

        if (variable.equalsIgnoreCase(SessionVariable.CBO_EQ_BASE_TYPE)) {
            String baseType = resolvedExpression.getStringValue();
            if (!baseType.equalsIgnoreCase(SessionVariableConstants.VARCHAR) &&
                    !baseType.equalsIgnoreCase(SessionVariableConstants.DECIMAL) &&
                    !baseType.equalsIgnoreCase(SessionVariableConstants.DOUBLE)) {
                throw new SemanticException(String.format("Unsupported cbo_eq_base_type: %s, " +
                        "supported list is {varchar, decimal, double}", baseType));
            }
        }

        // follower_query_forward_mode
        if (variable.equalsIgnoreCase(SessionVariable.FOLLOWER_QUERY_FORWARD_MODE)) {
            String queryFollowerForwardMode = resolvedExpression.getStringValue();
            if (!EnumUtils.isValidEnumIgnoreCase(SessionVariable.FollowerQueryForwardMode.class,
                    queryFollowerForwardMode)) {
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
        // big_query_profile_threshold
        if (variable.equalsIgnoreCase(SessionVariable.BIG_QUERY_PROFILE_THRESHOLD)) {
            String timeStr = resolvedExpression.getStringValue();
            TimeValue timeValue = TimeValue.parseTimeValue(timeStr, null);
            if (timeValue == null) {
                throw new SemanticException(String.format("failed to parse time value %s", timeStr));
            }
        }
        // catalog
        if (variable.equalsIgnoreCase(SessionVariable.CATALOG)) {
            String catalog = resolvedExpression.getStringValue();
            if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalog)) {
                throw new SemanticException(String.format("Unknown catalog %s", catalog));
            }
        }
        // connector sink compression codec
        if (variable.equalsIgnoreCase(SessionVariable.CONNECTOR_SINK_COMPRESSION_CODEC)) {
            String codec = resolvedExpression.getStringValue();
            if (CompressionUtils.getConnectorSinkCompressionType(codec).isEmpty()) {
                throw new SemanticException(String.format("Unsupported compression codec %s." +
                        " Use any of (uncompressed, snappy, lz4, zstd, gzip)", codec));
            }
        }
        // check plan mode
        if (variable.equalsIgnoreCase(SessionVariable.PLAN_MODE)) {
            PlanMode.fromName(resolvedExpression.getStringValue());
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

    public static void analyzeUserVariable(UserVariable var) {
        if (var.getVariable().length() > 64) {
            throw new SemanticException("User variable name '" + var.getVariable() + "' is illegal");
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
        UserIdentity userIdentity = var.getUserIdent();
        if (userIdentity == null) {
            userIdentity = session.getCurrentUserIdentity();
        }
        userIdentity.analyze();
        var.setUserIdent(userIdentity);
        var.setPasswdBytes(MysqlPassword.checkPassword(var.getPasswdParam()));
    }

    private static boolean checkUserVariableType(Type type) {
        if (type.isArrayType()) {
            ArrayType arrayType = (ArrayType) type;
            PrimitiveType itemPrimitiveType = arrayType.getItemType().getPrimitiveType();
            if (itemPrimitiveType == PrimitiveType.BOOLEAN ||
                    itemPrimitiveType.isDateType() || itemPrimitiveType.isNumericType() ||
                    itemPrimitiveType.isCharFamily()) {
                return true;
            }
        } else if (type.isScalarType()) {
            PrimitiveType primitiveType = type.getPrimitiveType();
            if (primitiveType == PrimitiveType.BOOLEAN ||
                    primitiveType.isDateType() || primitiveType.isNumericType() ||
                    primitiveType.isCharFamily() || primitiveType.isJsonType()) {
                return true;
            }
        }

        return false;
    }

    public static void calcuteUserVariable(UserVariable userVariable) {
        Expr expression = userVariable.getUnevaluatedExpression();
        if (expression instanceof NullLiteral) {
            userVariable.setEvaluatedExpression(NullLiteral.create(Type.STRING));
        } else {
            Expr foldedExpression;
            foldedExpression = Expr.analyzeAndCastFold(expression);

            if (foldedExpression.isLiteral()) {
                userVariable.setEvaluatedExpression(foldedExpression);
            } else {
                SelectList selectList = new SelectList(Lists.newArrayList(
                        new SelectListItem(userVariable.getUnevaluatedExpression(), null)), false);

                List<Expr> row = Lists.newArrayList(NullLiteral.create(Type.STRING));
                List<List<Expr>> rows = new ArrayList<>();
                rows.add(row);
                ValuesRelation valuesRelation = new ValuesRelation(rows, Lists.newArrayList(""));
                valuesRelation.setNullValues(true);

                SelectRelation selectRelation = new SelectRelation(selectList, valuesRelation, null, null, null);
                QueryStatement queryStatement = new QueryStatement(selectRelation);
                Analyzer.analyze(queryStatement, ConnectContext.get());

                Expr variableResult = queryStatement.getQueryRelation().getOutputExpression().get(0);

                Type type = variableResult.getType();
                // can not apply to metric types or complex type except array type
                if (!checkUserVariableType(type)) {
                    throw new SemanticException("Can't set variable with type " + variableResult.getType());
                }

                ((SelectRelation) queryStatement.getQueryRelation()).getSelectList().getItems()
                        .set(0, new SelectListItem(variableResult, null));
                Subquery subquery = new Subquery(queryStatement);
                subquery.setType(variableResult.getType());
                userVariable.setUnevaluatedExpression(subquery);
            }
        }
    }
>>>>>>> dc40504bac ([BugFix] fix an issue that user-defined variables sql unable to handle variable dependencies. (#48483))
}
