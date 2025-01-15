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

import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.HintNode;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.ast.Identifier;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.StarRocksParser;
import com.starrocks.sql.parser.SyntaxSugars;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

import static java.util.stream.Collectors.toList;

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

        Expr convertedFunctionCall = Pinot2SRFunctionCallTransformer.convert(functionName, arguments);

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

    private AnalyticExpr buildOverClause(FunctionCallExpr functionCallExpr, StarRocksParser.OverContext context,
                                         NodePosition pos) {
        functionCallExpr.setIsAnalyticFnCall(true);
        List<OrderByElement> orderByElements = new ArrayList<>();
        if (context.ORDER() != null) {
            orderByElements = visit(context.sortItem(), OrderByElement.class);
        }
        List<Expr> partitionExprs = visit(context.partition, Expr.class);
        return new AnalyticExpr(functionCallExpr, partitionExprs, orderByElements,
                (AnalyticWindow) visitIfPresent(context.windowFrame()),
                context.bracketHint() == null ? null : context.bracketHint().identifier().stream()
                        .map(RuleContext::getText).collect(toList()), pos);
    }

    private ParseNode visitIfPresent(ParserRuleContext context) {
        if (context != null) {
            return visit(context);
        } else {
            return null;
        }
    }

    private QualifiedName getQualifiedName(StarRocksParser.QualifiedNameContext context) {
        List<String> parts = new ArrayList<>();
        NodePosition pos = createPos(context);
        for (ParseTree c : context.children) {
            if (c instanceof TerminalNode) {
                TerminalNode t = (TerminalNode) c;
                if (t.getSymbol().getType() == StarRocksParser.DOT_IDENTIFIER) {
                    parts.add(t.getText().substring(1));
                }
            } else if (c instanceof StarRocksParser.IdentifierContext) {
                StarRocksParser.IdentifierContext identifierContext = (StarRocksParser.IdentifierContext) c;
                Identifier identifier = (Identifier) visit(identifierContext);
                parts.add(identifier.getValue());
            }
        }

        return QualifiedName.of(parts, pos);
    }
}
