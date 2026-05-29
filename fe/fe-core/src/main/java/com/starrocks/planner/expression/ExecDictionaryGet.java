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

import com.starrocks.thrift.TDictionaryGetExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.type.Type;

import java.util.List;

/**
 * Execution-plan dictionary_get expression.
 */
public class ExecDictionaryGet extends ExecExpr {
    private final long dictionaryId;
    private final long txnId;
    private final int keySize;
    private final boolean nullIfNotExist;

    public ExecDictionaryGet(Type type, long dictionaryId, long txnId, int keySize,
                             boolean nullIfNotExist, List<ExecExpr> children) {
        super(type, children);
        this.dictionaryId = dictionaryId;
        this.txnId = txnId;
        this.keySize = keySize;
        this.nullIfNotExist = nullIfNotExist;
    }

    private ExecDictionaryGet(ExecDictionaryGet other) {
        super(other.type, other.cloneChildren());
        this.dictionaryId = other.dictionaryId;
        this.txnId = other.txnId;
        this.keySize = other.keySize;
        this.nullIfNotExist = other.nullIfNotExist;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public long getDictionaryId() {
        return dictionaryId;
    }

    public long getTxnId() {
        return txnId;
    }

    public int getKeySize() {
        return keySize;
    }

    public boolean isNullIfNotExist() {
        return nullIfNotExist;
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public TExprNodeType getNodeType() {
        return TExprNodeType.DICTIONARY_GET_EXPR;
    }

    @Override
    public void toThrift(TExprNode node) {
        TDictionaryGetExpr expr = new TDictionaryGetExpr();
        expr.setDict_id(dictionaryId);
        expr.setTxn_id(txnId);
        expr.setKey_size(keySize);
        expr.setNull_if_not_exist(nullIfNotExist);
        node.setDictionary_get_expr(expr);
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecDictionaryGet(this, context);
    }

    @Override
    public ExecDictionaryGet clone() {
        return new ExecDictionaryGet(this);
    }
}
