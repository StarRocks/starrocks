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

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TDictionaryExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.util.List;

public class DictionaryExpr extends Expr {

    private TDictionaryExpr dictionaryExpr;
    private boolean skipStateCheck;
    private long dictionaryId;
    private long dictionaryTxnId;
    private int keySize;

    public DictionaryExpr(List<Expr> params) {
        this(params, NodePosition.ZERO);
    }

    public DictionaryExpr(List<Expr> params, NodePosition pos) {
        super(pos);
        this.children.addAll(params);
        this.skipStateCheck = false;
    }

    protected DictionaryExpr(DictionaryExpr other) {
        List<Expr> newChildren = Lists.newArrayList();
        for (Expr child : other.getChildren()) {
            newChildren.add(child.clone());
        }
        this.children.addAll(newChildren);
        this.skipStateCheck = other.getSkipStateCheck();
        this.dictionaryId = other.getDictionaryId();
        this.dictionaryTxnId = other.getDictionaryTxnId();
        this.keySize = other.getKeySize();
    }

    @Override
    protected String toSqlImpl() {
        String message = "DICTIONARY_GET(";
        for (int i = 0; i < this.children.size(); ++i) {
            Expr expr = this.children.get(i);
            message += expr.toSql();
            if (i != this.children.size() - 1) {
                message += ", ";
            }
        }
        message += ")";
        return message;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        TDictionaryExpr dictionaryExpr = new TDictionaryExpr();
        dictionaryExpr.setDict_id(dictionaryId);
        dictionaryExpr.setTxn_id(dictionaryTxnId);
        dictionaryExpr.setKey_size(keySize);
        setDictionaryExpr(dictionaryExpr);

        msg.setNode_type(TExprNodeType.DICTIONARY_EXPR);
        msg.setDictionary_expr(dictionaryExpr);
    }

    @Override
    public Expr clone() {
        return new DictionaryExpr(this);
    }

    public TDictionaryExpr getDictionaryExpr() {
        return dictionaryExpr;
    }

    public void setDictionaryExpr(TDictionaryExpr dictionaryExpr) {
        this.dictionaryExpr = dictionaryExpr;
    }

    public void setSkipStateCheck(boolean skipStateCheck) {
        this.skipStateCheck = skipStateCheck;
    }

    public boolean getSkipStateCheck() {
        return this.skipStateCheck;
    }

    public void setDictionaryId(long dictionaryId) {
        this.dictionaryId = dictionaryId;
    }

    public void setDictionaryTxnId(long dictionaryTxnId) {
        this.dictionaryTxnId = dictionaryTxnId;
    }

    public void setKeySize(int keySize) {
        this.keySize = keySize;
    }

    public long getDictionaryId() {
        return this.dictionaryId;
    }

    public long getDictionaryTxnId() {
        return this.dictionaryTxnId;
    }

    public int getKeySize() {
        return this.keySize;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDictionaryExpr(this, context);
    }
}
