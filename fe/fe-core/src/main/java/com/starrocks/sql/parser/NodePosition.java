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

import java.io.Serializable;
import javax.validation.constraints.NotNull;

// Used to record element position in the sql. ParserRuleContext records the input start and end token,
// and we can transform their line and col info to NodePosition.
public class NodePosition implements Serializable {

    public static final NodePosition ZERO = new NodePosition(0, 0);

    private static final long serialVersionUID = 5619050719810060066L;

    private final int line;

    private final int col;

    private final int endLine;

    private final int endCol;

    public NodePosition(@NotNull TerminalNode node) {
        this(node.getSymbol().getLine(), node.getSymbol().getCharPositionInLine());
    }

    public NodePosition(@NotNull Token token) {
        this(token.getLine(), token.getLine());
    }

    public NodePosition(@NotNull Token start, @NotNull Token end) {
        this(start.getLine(), start.getCharPositionInLine(), end.getLine(), end.getCharPositionInLine());
    }

    public NodePosition(int line, int col) {
        this(line, col, line, col);
    }

    public NodePosition(int line, int col, int endLine, int endCol) {
        this.line = line;
        this.col = col;
        this.endLine = endLine;
        this.endCol = endCol;
    }

    public int getLine() {
        return line;
    }

    public int getCol() {
        return col;
    }

    public int getEndLine() {
        return endLine;
    }

    public int getEndCol() {
        return endCol;
    }

    public boolean isZero() {
        return line == 0 && col == 0 && endLine == 0 && endCol == 0;
    }


}
