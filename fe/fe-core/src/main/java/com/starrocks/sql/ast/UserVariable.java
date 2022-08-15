// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.SelectList;
import com.starrocks.analysis.SelectListItem;
import com.starrocks.analysis.SetType;
import com.starrocks.analysis.SetVar;
import com.starrocks.analysis.Subquery;
import com.starrocks.catalog.Type;
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
                SelectList selectList = new SelectList();
                SelectListItem item = new SelectListItem(new CastExpr(Type.VARCHAR, getExpression()), null);
                selectList.addItem(item);


                ArrayList<Expr> row = new ArrayList<>();
                List<String> columnNames = new ArrayList<>();
                row.add(NullLiteral.create(Type.NULL));
                columnNames.add("");
                List<ArrayList<Expr>> rows = new ArrayList<>();
                rows.add(row);
                ValuesRelation valuesRelation = new ValuesRelation(rows, columnNames);
                valuesRelation.setNullValues(true);

                SelectRelation selectRelation = new SelectRelation(selectList, valuesRelation, null, null, null);
                QueryStatement queryStatement = new QueryStatement(selectRelation);
                setExpression(new Subquery(queryStatement));
            }
        }
    }
}
