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

package com.starrocks.planner.expression;

import com.starrocks.thrift.TDictQueryExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.type.Type;

import java.util.List;

/**
 * Execution-plan dict_query expression.
 */
public class ExecDictQuery extends ExecExpr {
    private final TDictQueryExpr dictQueryExpr;

    public ExecDictQuery(Type type, TDictQueryExpr dictQueryExpr, List<ExecExpr> children) {
        super(type, children);
        this.dictQueryExpr = dictQueryExpr;
    }

    private ExecDictQuery(ExecDictQuery other) {
        super(other.type, other.cloneChildren());
        this.dictQueryExpr = other.dictQueryExpr;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public TDictQueryExpr getDictQueryExpr() {
        return dictQueryExpr;
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public TExprNodeType getNodeType() {
        return TExprNodeType.DICT_QUERY_EXPR;
    }

    @Override
    public void toThrift(TExprNode node) {
        node.setDict_query_expr(dictQueryExpr);
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecDictQuery(this, context);
    }

    @Override
    public ExecDictQuery clone() {
        return new ExecDictQuery(this);
    }
}
