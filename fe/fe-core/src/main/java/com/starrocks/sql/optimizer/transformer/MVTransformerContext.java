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
package com.starrocks.sql.optimizer.transformer;

import com.google.common.collect.Maps;
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.util.Box;

import java.util.Map;

public class MVTransformerContext {
    // Map from operator to AST tree which is used for text based mv rewrite
    // Use Box to ensure the identity of the operator because the operator may be different even
    // if `Operator`'s equals method is true.
    private final Map<Box<Operator>, ParseNode> opToASTMap = Maps.newHashMap();

    public MVTransformerContext() {
    }

    /**
     * Register the AST tree for the given operator in transformer stage
     * @param op input operator
     * @param ast AST tree of this operator
     */
    public void registerOpAST(Operator op, ParseNode ast) {
        if (op == null) {
            return;
        }
        opToASTMap.put(Box.of(op), ast);
    }

    /**
     * Check whether the AST tree for the given operator is registered in transformer stage
     */
    public boolean hasOpAST(Operator op) {
        return opToASTMap.containsKey(Box.of(op));
    }

    /**
     * Check whether the operator to AST tree map is empty or not.
     */
    public boolean isOpASTEmpty() {
        return opToASTMap.isEmpty();
    }

    /**
     * Get the AST tree for the given operator in transformer stage
     */
    public ParseNode getOpAST(Operator op) {
        return opToASTMap.get(Box.of(op));
    }
}
