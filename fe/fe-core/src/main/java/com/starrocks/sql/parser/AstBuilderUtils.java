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

import com.google.common.base.Preconditions;
import com.starrocks.sql.ast.Identifier;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;

public class AstBuilderUtils {
    public static Identifier getIdentifier(
            com.starrocks.sql.parser.StarRocksParser.IdentifierContext identifierContext) {
        if (identifierContext instanceof com.starrocks.sql.parser.StarRocksParser.BackQuotedIdentifierContext) {
            Identifier backQuotedIdentifier = new Identifier(identifierContext.getText().replace("`", ""),
                    createPos(identifierContext));
            backQuotedIdentifier.setBackQuoted(true);
            return backQuotedIdentifier;
        } else {
            return new Identifier(identifierContext.getText(), createPos(identifierContext));
        }
    }

    protected static NodePosition createPos(ParserRuleContext context) {
        Preconditions.checkState(context != null);
        return createPos(context.start, context.stop);
    }

    static NodePosition createPos(Token start, Token stop) {
        if (start == null) {
            return NodePosition.ZERO;
        }

        if (stop == null) {
            return new NodePosition(start.getLine(), start.getCharPositionInLine());
        }

        return new NodePosition(start, stop);
    }
}
