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


package com.starrocks.analysis;

import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TDictQueryExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.util.List;

// dict_mapping(STRING dict_table_name, ANY... keys [, STRING value_column_name])
public class DictQueryExpr extends FunctionCallExpr {

    private TDictQueryExpr dictQueryExpr;

    public DictQueryExpr(List<Expr> params) throws SemanticException {
        super(FunctionSet.DICT_MAPPING, params);
    }

    public DictQueryExpr(List<Expr> params, TDictQueryExpr dictQueryExpr, Function fn) {
        super(FunctionSet.DICT_MAPPING, params);
        this.dictQueryExpr = dictQueryExpr;
        this.fn = fn;
        setType(fn.getReturnType());
    }

    protected DictQueryExpr(DictQueryExpr other) {
        super(other);
        this.dictQueryExpr = other.getDictQueryExpr();
    }


    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNode_type(TExprNodeType.DICT_QUERY_EXPR);
        msg.setDict_query_expr(dictQueryExpr);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDictQueryExpr(this, context);
    }

    @Override
    public boolean isAggregateFunction() {
        return false;
    }

    @Override
    public Expr clone() {
        return new DictQueryExpr(this);
    }

    public TDictQueryExpr getDictQueryExpr() {
        return dictQueryExpr;
    }

    public void setDictQueryExpr(TDictQueryExpr dictQueryExpr) {
        this.dictQueryExpr = dictQueryExpr;
    }
}
