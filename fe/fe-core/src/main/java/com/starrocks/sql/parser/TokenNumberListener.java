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