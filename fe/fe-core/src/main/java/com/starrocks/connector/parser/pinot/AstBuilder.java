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

import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.HintNode;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.StarRocksParser;
import com.starrocks.sql.parser.SyntaxSugars;
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.IdentityHashMap;
import java.util.List;

public class AstBuilder extends com.starrocks.sql.parser.AstBuilder {

    protected AstBuilder(long sqlMode) {
        super(sqlMode);
    }

    public AstBuilder(long sqlMode, IdentityHashMap<ParserRuleContext, List<HintNode>> hintMap) {
        super(sqlMode, hintMap);
    }

    @Override
    public ParseNode visitFunctionCallExpression(StarRocksParser.FunctionCallExpressionContext ctx) {
        return visitChildren(ctx);
    }

    public ParseNode visitSimpleFunctionCall(StarRocksParser.SimpleFunctionCallContext context) {
        String fullFunctionName = getQualifiedName(context.qualifiedName()).toString();
        NodePosition pos = createPos(context);

        List<Expr> arguments = visit(context.expression(), Expr.class);
        FunctionName fnName = FunctionName.createFnName(fullFunctionName);
        String functionName = fnName.getFunction();

        Pinot2SRFunctionCallTransformer transformer = new Pinot2SRFunctionCallTransformer();
        Expr convertedFunctionCall = transformer.convert(functionName, arguments);

        if (convertedFunctionCall != null) {
            if (functionName.equalsIgnoreCase("fromdatetime")) {
                ArithmeticExpr toMillis = new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY,
                        convertedFunctionCall, new IntLiteral(1000));
                return toMillis;
            }
            return convertedFunctionCall;
        } else {
            FunctionCallExpr functionCallExpr = new FunctionCallExpr(fnName,
                    new FunctionParams(false, arguments), pos);
            if (context.over() != null) {
                return buildOverClause(functionCallExpr, context.over(), pos);
            }
            return SyntaxSugars.parse(functionCallExpr);
        }
    }
}
