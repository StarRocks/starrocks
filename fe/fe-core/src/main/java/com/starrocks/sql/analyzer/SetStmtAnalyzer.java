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
                        if (!variableResult.getType().canApplyToNumeric() || variableResult.getType().isComplexType()) {
                            throw new SemanticException("Can't set variable with type " + variableResult.getType());
                        }

                        ((SelectRelation) queryStatement.getQueryRelation()).getSelectList().getItems().set(0,
                                new SelectListItem(new CastExpr(Type.VARCHAR, variableResult), null));

                        Subquery subquery = new Subquery(queryStatement);
                        subquery.setType(variableResult.getType());
                        var.setExpression(subquery);
                    }
                }
            } else {
                //TODO: Unify the analyze logic of other types of SetVar from the original definition
                var.analyze();
            }
        }
    }
}
