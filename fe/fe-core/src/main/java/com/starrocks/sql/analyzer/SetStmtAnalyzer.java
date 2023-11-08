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
<<<<<<< HEAD
=======
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SessionVariableConstants;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
>>>>>>> 3fcdc4e1f4 ([Enhancement] support decimal eq string cast flag (#34208))
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
            checkRangeLongVariable(resolvedExpression, SessionVariable.ADAPTIVE_DOP_MAX_BLOCK_ROWS_PER_DRIVER_SEQ, 1L, null);
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
                    !baseType.equalsIgnoreCase(SessionVariableConstants.DECIMAL)) {
                throw new SemanticException(String.format("Unsupported cbo_eq_base_type: %s, " +
                        "supported list is {varchar, decimal}", baseType));
            }
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
>>>>>>> 3fcdc4e1f4 ([Enhancement] support decimal eq string cast flag (#34208))
            } else {
                //TODO: Unify the analyze logic of other types of SetVar from the original definition
                var.analyze();
            }
        }
    }
}
