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

package com.starrocks.sql.spm;

import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.ast.QueryRelation;

import java.util.List;

// replace variables to SPMFunctions
public class SPMPlaceholderBuilder {
    private final PlaceholderBuilder builder = new PlaceholderBuilder();

    private long placeholderID = 0;

    public QueryRelation build(QueryRelation query) {
        return (QueryRelation) query.accept(builder, null);
    }

    private class PlaceholderBuilder extends SPMUpdateExprVisitor<Void> {
        //        @Override
        //        public ParseNode visitInPredicate(InPredicate node, Void context) {
        //            if (!node.isLiteralChildren()) {
        //                return node;
        //            }
        //            return new InPredicate(node.getChild(0),
        //                    List.of(SPMFunctions.newFunc(SPMFunctions.CONST_LIST_FUNC, placeholderID++, node.getChildren())),
        //                    node.isNotIn(), node.getPos());
        //        }

        @Override
        public ParseNode visitLiteral(LiteralExpr node, Void context) {
            return SPMFunctions.newFunc(SPMFunctions.CONST_VAR_FUNC, placeholderID++, List.of(node));
        }
    }
}
