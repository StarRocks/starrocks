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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/CastExpr.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.Type;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

public class CastExpr extends Expr {
    private static final Logger LOG = LogManager.getLogger(CastExpr.class);

    // Only set for explicit casts. Null for implicit casts.
    private final TypeDef targetTypeDef;

    // True if this is a "pre-analyzed" implicit cast.
    private boolean isImplicit;

    // True if this cast does not change the type.
    private boolean noOp = false;

    public CastExpr(Type targetType, Expr e) {
        this(targetType, e, NodePosition.ZERO);
    }

    public CastExpr(Type targetType, Expr e, NodePosition pos) {
        super(pos);
        Preconditions.checkArgument(targetType.isValid());
        Preconditions.checkNotNull(e);
        type = targetType;
        targetTypeDef = null;
        isImplicit = true;

        children.add(e);
        try {
            analyze();
        } catch (AnalysisException ex) {
            LOG.warn(ex);
            Preconditions.checkState(false,
                    "Implicit casts should never throw analysis exception.");
        }
        analysisDone();
    }

    /**
     * Copy c'tor used in clone().
     */
    public CastExpr(TypeDef targetTypeDef, Expr e) {
        this(targetTypeDef, e, NodePosition.ZERO);
    }

    public CastExpr(TypeDef targetTypeDef, Expr e, NodePosition pos) {
        super(pos);
        Preconditions.checkNotNull(targetTypeDef);
        Preconditions.checkNotNull(e);
        this.targetTypeDef = targetTypeDef;
        isImplicit = false;
        children.add(e);
    }

    protected CastExpr(CastExpr other) {
        super(other);
        targetTypeDef = other.targetTypeDef;
        isImplicit = other.isImplicit;
        noOp = other.noOp;
    }

    public TypeDef getTargetTypeDef() {
        return targetTypeDef;
    }

    private static String getFnName(Type targetType) {
        return "castTo" + targetType.getPrimitiveType().toString();
    }

    @Override
    public Expr clone() {
        return new CastExpr(this);
    }

    @Override
    public String toSqlImpl() {
        if (targetTypeDef == null) {
            return "CAST(" + getChild(0).toSql() + " AS " + type.toString() + ")";
        } else {
            return "CAST(" + getChild(0).toSql() + " AS " + targetTypeDef + ")";
        }
    }

    @Override
    protected String explainImpl() {
        if (noOp) {
            return getChild(0).explain();
        } else {
            return "cast(" + getChild(0).explain() + " as " + type.toString() + ")";
        }
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.CAST_EXPR;
        msg.setOpcode(opcode);
        msg.setOutput_column(outputColumn);
        if (getChild(0).getType().isComplexType()) {
            msg.setChild_type_desc(getChild(0).getType().toThrift());
        } else {
            msg.setChild_type(getChild(0).getType().getPrimitiveType().toThrift());
        }
    }

    public boolean isImplicit() {
        return isImplicit;
    }

    public void setImplicit(boolean implicit) {
        isImplicit = implicit;
    }

    public void analyze() throws AnalysisException {
        // cast was asked for in the query, check for validity of cast
        Type childType = getChild(0).getType();

        // this cast may result in loss of precision, but the user requested it
        if (childType.matchesType(type)) {
            noOp = true;
            return;
        }

        this.opcode = TExprOpcode.CAST;
        FunctionName fnName = new FunctionName(getFnName(type));
        Function searchDesc = new Function(fnName, collectChildReturnTypes(), Type.INVALID, false);
        if (isImplicit) {
            fn = GlobalStateMgr.getCurrentState().getFunction(
                    searchDesc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else {
            fn = GlobalStateMgr.getCurrentState().getFunction(
                    searchDesc, Function.CompareMode.IS_IDENTICAL);
        }
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        Preconditions.checkState(!isImplicit);
        targetTypeDef.analyze(analyzer);
        type = targetTypeDef.getType();
        analyze();
    }

    @Override
    public Expr reset() {
        Expr e = super.reset();
        if (noOp && !getChild(0).getType().matchesType(this.type)) {
            noOp = false;
        }
        return e;
    }

    /**
     * Returns child expr if this expr is an implicit cast, otherwise returns 'this'.
     */
    @Override
    public Expr ignoreImplicitCast() {
        if (isImplicit) {
            // we don't expect to see to consecutive implicit casts
            Preconditions.checkState(
                    !(getChild(0) instanceof CastExpr) || !((CastExpr) getChild(0)).isImplicit());
            return getChild(0);
        } else {
            return this;
        }
    }

    public boolean canHashPartition() {
        if (type.isFixedPointType() && getChild(0).getType().isFixedPointType()) {
            return true;
        }
        if (type.isDateType() && getChild(0).getType().isDateType()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean isNullable() {
        Expr fromExpr = getChild(0);
        if (fromExpr.getType().isFullyCompatible(getType())) {
            return fromExpr.isNullable();
        }
        return true;
    }

    /**
     * Below function is added by new analyzer
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return visitor.visitCastExpr(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        CastExpr castExpr = (CastExpr) o;

        if (this.opcode != castExpr.opcode) {
            return false;
        }

        if (targetTypeDef != null) {
            return targetTypeDef.getType().equals(castExpr.getTargetTypeDef().getType());
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), targetTypeDef == null ? null : targetTypeDef.getType(), opcode);
    }

    @Override
    public boolean isSelfMonotonic() {
        // It's very tempting to think cast is monotonic, but that's not true.
        // For example `cast(bigint to tinyint) < 10`
        // maybe min/max value will overflow tinyint, and we will get NULL value, so `NULL is true` is false.
        // but some values between min/max value like 5,6,7,8 can be evaluated to true.
        return false;
    }
}
