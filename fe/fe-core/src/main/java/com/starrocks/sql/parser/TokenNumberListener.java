// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.parser;

import org.antlr.v4.runtime.tree.TerminalNode;

public class TokenNumberListener extends StarRocksBaseListener {

    private final int maxTokensNum;
    private final int maxExprChildCount;

    public TokenNumberListener(int maxTokensNum, int maxExprChildCount) {
        this.maxTokensNum = maxTokensNum;
        this.maxExprChildCount = maxExprChildCount;
    }

    @Override
    public void visitTerminal(TerminalNode node) {
        int index = node.getSymbol().getTokenIndex();
        if (index >= maxTokensNum) {
            throw new OperationNotAllowedException("Statement exceeds maximum length limit, please consider modify " +
                    "parse_tokens_limit variable.");
        }
    }

    @Override
    public void exitExpressionList(StarRocksParser.ExpressionListContext ctx) {
        long childCount = ctx.children.stream().filter(child -> child instanceof StarRocksParser.ExpressionContext).count();
        if (childCount > maxExprChildCount) {
            throw new OperationNotAllowedException(String.format("Expression child number %d exceeded the maximum %d",
                    childCount, maxExprChildCount));
        }
    }

    @Override
    public void exitExpressionsWithDefault(StarRocksParser.ExpressionsWithDefaultContext ctx) {
        long childCount = ctx.expressionOrDefault().size();
        if (childCount > maxExprChildCount) {
            throw new OperationNotAllowedException(String.format("Expression child number %d exceeded the maximum %d",
                    childCount, maxExprChildCount));
        }
    }

    @Override
    public void exitInsertStatement(StarRocksParser.InsertStatementContext ctx) {
        long childCount = ctx.expressionsWithDefault().size();
        if (childCount > maxExprChildCount) {
            throw new OperationNotAllowedException(String.format("Expression child number %d exceeded the maximum %d",
                    childCount, maxExprChildCount));
        }
    }
}