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
package com.starrocks.connector.parser.pinot;

import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.connector.parser.BaseFunctionCallTransformer;
import com.starrocks.connector.parser.PlaceholderExpr;
import com.starrocks.sql.analyzer.SemanticException;

import java.math.BigDecimal;
import java.util.List;

public class Pinot2SRFunctionCallTransformer extends BaseFunctionCallTransformer {

    public Pinot2SRFunctionCallTransformer() { super(); }

    @Override
    public Expr convert(String fnName, List<Expr> children) {
        //for some functions, the arguments may need some format or number alignment
        List<Expr> processedChildren = preprocessChildren(fnName, children);

        Expr result = convertRegisterFn(fnName, processedChildren);

        if (result == null) {
            ComplexFunctionCallTransformer complexFunctionCallTransformer = new ComplexFunctionCallTransformer();
            result = complexFunctionCallTransformer.transform(fnName, children.toArray(new Expr[0]));
        }
        return result;
    }

    private static List<Expr> preprocessChildren(String fnName, List<Expr> children) {
        switch (fnName) {
            case "todatetime":
            case "fromdatetime":
                if (children.size() < 2 || children.size() > 3) {
                    throw new SemanticException("The todatetime/fromdatetime function must include between " +
                            "2 and 3 parameters, inclusive.");
                }
                // transform date format
                children.set(1, transformDateFormat(children.get(1)));
                break;

            default:
                if (fnName.equalsIgnoreCase("percentiletdigest")) {
                    children.set(1, transformPercentileValue(children.get(1)));
                }
                break;
        }
        return children;
    }

    private static Expr transformDateFormat(Expr child) {
        if (child instanceof StringLiteral) {
            String value = ((StringLiteral) child).getValue();
            // convert to strftime format
            if (PinotParserUtils.isJavaDateFormat(value)) {
                String formatValue = PinotParserUtils.convertToStrftimeFormat(value);
                return new StringLiteral(formatValue);
            }
        }
        return child;
    }

    private static Expr transformPercentileValue(Expr child) {
        if (child instanceof IntLiteral) {
            double value = ((IntLiteral) child).getValue();
            return new DecimalLiteral(new BigDecimal(Double.toString(value / 100)));
        } else if (child instanceof DecimalLiteral) {
            BigDecimal value = ((DecimalLiteral) child).getValue();
            return new DecimalLiteral(value.divide(new BigDecimal(100)));
        }

        return child;
    }

    @Override
    protected void registerAllFunctionTransformer() {
        registerDateFunctionTransformer();
        registerStringFunctionTransformer();
        registerAggregateFunctionTransformer();
        // todo: support more function transform
    }

    private static void registerStringFunctionTransformer() {
        // regexp_like -> regexp
        registerFunctionTransformer("regexp_like", 2, "regexp", List.of(Expr.class, Expr.class));
    }

    private static void registerDateFunctionTransformer() {
        // todatetime -> date_format
        registerFunctionTransformer("todatetime", 2, "date_format", List.of(Expr.class, Expr.class));

        // todatetime (time, pattern, timeZone) -> convert_tz, str_to_date
        registerFunctionTransformer("todatetime", 3, new FunctionCallExpr("convert_tz", List.of(
                new FunctionCallExpr("date_format", List.of(
                        new PlaceholderExpr(1, Expr.class), new PlaceholderExpr(2, Expr.class))),
                new VariableExpr("time_zone"),
                new PlaceholderExpr(3, Expr.class)
        )));

        // fromdatetime -> str_to_date
        registerFunctionTransformer("fromdatetime", 2, new FunctionCallExpr("unix_timestamp",
                List.of(new FunctionCallExpr("str_to_date", List.of(new PlaceholderExpr(1, Expr.class),
                        new PlaceholderExpr(2, Expr.class))))));
    }

    private static void registerAggregateFunctionTransformer() {
        //distinctcounthll -> approx_count_distinct
        registerFunctionTransformer("distinctcounthll", 1, "approx_count_distinct", List.of(Expr.class));

        //percentiletdigest -> percentile_approx
        registerFunctionTransformer("percentiletdigest", 2, "percentile_approx", List.of(Expr.class, Expr.class));

        //percentiletdigest -> percentile_approx
        registerFunctionTransformer("percentiletdigest", 3, "percentile_approx", List.of(Expr.class, Expr.class, Expr.class));
    }
}
