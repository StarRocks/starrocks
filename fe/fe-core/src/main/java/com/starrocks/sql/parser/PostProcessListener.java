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

import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

public class PostProcessListener extends StarRocksBaseListener {

    private final int maxTokensNum;
    private final int maxExprChildCount;

    public PostProcessListener(int maxTokensNum, int maxExprChildCount) {
        this.maxTokensNum = maxTokensNum;
        this.maxExprChildCount = maxExprChildCount;
    }

    @Override
    public void visitTerminal(TerminalNode node) {
        Token token = node.getSymbol();
        int index = token.getTokenIndex();
        if (index >= maxTokensNum) {
            throw new ParsingException(PARSER_ERROR_MSG.tokenExceedLimit());
        }
    }

    @Override
    public void exitExpressionList(StarRocksParser.ExpressionListContext ctx) {
        long childCount = ctx.children.stream().filter(child -> child instanceof StarRocksParser.ExpressionContext).count();
        if (childCount > maxExprChildCount) {
            NodePosition pos = new NodePosition(ctx.start, ctx.stop);
            throw new ParsingException(PARSER_ERROR_MSG.exprsExceedLimit(childCount, maxExprChildCount), pos);
        }
    }

    @Override
    public void exitExpressionsWithDefault(StarRocksParser.ExpressionsWithDefaultContext ctx) {
        long childCount = ctx.expressionOrDefault().size();
        if (childCount > maxExprChildCount) {
            NodePosition pos = new NodePosition(ctx.start, ctx.stop);
            throw new ParsingException(PARSER_ERROR_MSG.argsOfExprExceedLimit(childCount, maxExprChildCount), pos);
        }
    }

    @Override
    public void exitInsertStatement(StarRocksParser.InsertStatementContext ctx) {
        long childCount = ctx.expressionsWithDefault().size();
        if (childCount > maxExprChildCount) {
            NodePosition pos = new NodePosition(ctx.start, ctx.stop);
            throw new ParsingException(PARSER_ERROR_MSG.insertRowsExceedLimit(childCount, maxExprChildCount), pos);
        }
    }
}