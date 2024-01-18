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

import com.starrocks.common.exception.AnalysisException;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

// DictMappingExpr.
// The original expression will be rewritten as a dictionary mapping function in the global field optimization.
// child(0) was input lowcardinality dictionary column (input was ID type).
// child(1) was origin expr (input was string type).
// 
// in Global Dictionary Optimization. The process of constructing a dictionary mapping requires 
// a new dictionary to be constructed using the global dictionary columns as input columns. 
// So BE needs to know the original expressions.
public class DictMappingExpr extends Expr {

    public DictMappingExpr(Expr ref, Expr call) {
        super(ref);
        this.addChild(ref);
        this.addChild(call);
    }

    public DictMappingExpr(Expr ref, Expr call, Expr stringProvideExpr) {
        super(ref);
        this.addChild(ref);
        this.addChild(call);
        this.addChild(stringProvideExpr);
    }

    protected DictMappingExpr(DictMappingExpr other) {
        super(other);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {

    }

    @Override
    protected String toSqlImpl() {
        String fnName = this.type.matchesType(this.getChild(1).getType()) ? "DictDecode" : "DictDefine";

        if (children.size() == 2) {
            return fnName + "(" + this.getChild(0).toSqlImpl() + ", [" + this.getChild(1).toSqlImpl() + "])";
        }
        return fnName + "(" + this.getChild(0).toSqlImpl() + ", [" + this.getChild(1).toSqlImpl() + "], " +
                getChild(2).toSqlImpl() + ")";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNode_type(TExprNodeType.DICT_EXPR);
    }

    @Override
    public Expr clone() {
        return new DictMappingExpr(this);
    }
}
