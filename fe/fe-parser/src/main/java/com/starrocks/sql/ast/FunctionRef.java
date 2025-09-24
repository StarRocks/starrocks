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

package com.starrocks.sql.ast;

import com.google.common.base.Preconditions;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

/**
 * FunctionRef is used to represent all Functions (UDF) with same FunctionName.
 * This description for Function is made when creating AST with unanalyzed state and made with
 * analyzed state if given a Function Object.
 */
public class FunctionRef implements ParseNode {
    private final NodePosition pos;
    private final QualifiedName fnName;
    private final String alias;

    public FunctionRef(QualifiedName fnName, String alias, NodePosition pos) {
        Preconditions.checkArgument(fnName != null && !fnName.getParts().isEmpty(), "Function name is not valid");
        this.fnName = fnName;
        this.pos = pos;
        this.alias = alias;
    }

    public QualifiedName getFnName() {
        return fnName;
    }

    public String getDbName() {
        List<String> parts = fnName.getParts();
        if (parts.size() == 2) {
            return parts.get(0);
        } else {
            return null;
        }
    }

    public String getFunctionName() {
        List<String> parts = fnName.getParts();
        String functionName = parts.get(parts.size() - 1);
        return functionName.toLowerCase();
    }

    public String getAlias() {
        return this.alias;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
