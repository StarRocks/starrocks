// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.parser;

import org.antlr.v4.runtime.tree.TerminalNode;

public class TokenNumberListener extends StarRocksBaseListener {

    private int maxTokensNum;

    public TokenNumberListener(int maxTokensNum) {
        this.maxTokensNum = maxTokensNum;
    }

    @Override
    public void visitTerminal(TerminalNode node) {
        int index = node.getSymbol().getTokenIndex();
        if (index >= maxTokensNum) {
            throw new OperationNotAllowedException("Statement exceeds maximum length limit, please consider modify " +
                    "parse_tokens_limit variable.");
        }
    }
}