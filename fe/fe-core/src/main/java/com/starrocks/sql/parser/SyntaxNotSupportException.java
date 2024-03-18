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

public class SyntaxNotSupportException extends RuntimeException {
    private final Token startToken;
    private final Token offendingToken;

    public SyntaxNotSupportException(Token startToken, Token offendingToken) {
        this.startToken = startToken;
        this.offendingToken = offendingToken;
    }

    public Token getStartToken() {
        return startToken;
    }

    public Token getOffendingToken() {
        return offendingToken;
    }
}
