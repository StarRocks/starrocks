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

import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class MergeWhenNotMatchedInsertClause extends MergeWhenClause {
    private final List<String> targetColumnNames; // nullable: omitted or INSERT *
    private final List<Expr> values;              // nullable: INSERT *
    private final boolean isStar;

    public MergeWhenNotMatchedInsertClause(Expr optionalCondition,
                                           List<String> targetColumnNames,
                                           List<Expr> values,
                                           boolean isStar,
                                           NodePosition pos) {
        super(optionalCondition, pos);
        this.targetColumnNames = targetColumnNames;
        this.values = values;
        this.isStar = isStar;
    }

    public List<String> getTargetColumnNames() {
        return targetColumnNames;
    }

    public List<Expr> getValues() {
        return values;
    }

    public boolean isStar() {
        return isStar;
    }
}
