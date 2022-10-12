// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.SetType;
import com.starrocks.analysis.SetVar;
import com.starrocks.analysis.Subquery;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.ArrayList;
import java.util.List;

public class UserVariable extends SetVar {

    public UserVariable(String variable, Expr value) {
        super(SetType.USER, variable, value);
    }

    @Override
    public void analyze() {
        if (getVariable().length() > 64) {
            throw new SemanticException("User variable name '" + getVariable() + "' is illegal");
        }

        Expr expression = getExpression();
        if (expression instanceof NullLiteral) {
            setResolvedExpression(NullLiteral.create(Type.STRING));
        } else {
            Expr foldedExpression = Expr.analyzeAndCastFold(expression);
            if (foldedExpression instanceof LiteralExpr) {
                setResolvedExpression((LiteralExpr) foldedExpression);
            } else {
                SelectList selectList = new SelectList(Lists.newArrayList(
                        new SelectListItem(getExpression(), null)), false);

                ArrayList<Expr> row = Lists.newArrayList(NullLiteral.create(Type.NULL));
                List<ArrayList<Expr>> rows = new ArrayList<>();
                rows.add(row);
                ValuesRelation valuesRelation = new ValuesRelation(rows, Lists.newArrayList(""));
                valuesRelation.setNullValues(true);

                SelectRelation selectRelation = new SelectRelation(selectList, valuesRelation, null, null, null);
                QueryStatement queryStatement = new QueryStatement(selectRelation);
                Analyzer.analyze(queryStatement, ConnectContext.get());

                Expr variableResult = queryStatement.getQueryRelation().getOutputExpression().get(0);

                //BITMAP/HLL/PERCENTILE/ARRAY types are not supported,
                if (variableResult.getType().isOnlyMetricType() || variableResult.getType().isComplexType()) {
                    throw new SemanticException("Can't set variable with type " + variableResult.getType());
                }

                ((SelectRelation) queryStatement.getQueryRelation()).getSelectList().getItems().set(0,
                        new SelectListItem(new CastExpr(Type.VARCHAR, variableResult), null));

                Subquery subquery = new Subquery(queryStatement);
                subquery.setType(variableResult.getType());
                setExpression(subquery);
            }
        }
    }
}
