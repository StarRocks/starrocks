// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.rewrite;

import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.StringLiteral;

/*
 * Replace date format on function from_unixtime/date_format, move from
 * FunctionCall analyze for avoid create view bug
 */
@Deprecated
public class ReplaceDateFormatRule implements ExprRewriteRule {
    public static ExprRewriteRule INSTANCE = new ReplaceDateFormatRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer) {
        if (!(expr instanceof FunctionCallExpr)) {
            return expr;
        }

        FunctionCallExpr fn = ((FunctionCallExpr) expr);
        if (fn.getFnName().getFunction().equalsIgnoreCase("from_unixtime")
                || fn.getFnName().getFunction().equalsIgnoreCase("date_format")) {
            // if has only one child, it has default time format: yyyy-MM-dd HH:mm:ss.SSSSSS
            if (fn.getChildren().size() > 1) {
                StringLiteral fmtLiteral = (StringLiteral) fn.getChild(1);
                StringLiteral newLiteral;

                switch (fmtLiteral.getStringValue()) {
                    case "yyyyMMdd":
                        newLiteral = new StringLiteral("%Y%m%d");
                        break;
                    case "yyyy-MM-dd":
                        newLiteral = new StringLiteral("%Y-%m-%d");
                        break;
                    case "yyyy-MM-dd HH:mm:ss":
                        newLiteral = new StringLiteral("%Y-%m-%d %H:%i:%s");
                        break;
                    default:
                        return expr;
                }

                FunctionCallExpr newFunction = (FunctionCallExpr) fn.clone();
                newFunction.getChildren().set(1, newLiteral);
                return newFunction;
            }
        }

        return expr;
    }
}
