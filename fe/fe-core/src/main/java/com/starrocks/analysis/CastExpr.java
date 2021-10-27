// This file is made available under Elastic License 2.0.
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
import com.google.common.collect.Lists;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.ExprVisitor;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class CastExpr extends Expr {
    private static final Logger LOG = LogManager.getLogger(CastExpr.class);

    // Only set for explicit casts. Null for implicit casts.
    private final TypeDef targetTypeDef;

    // True if this is a "pre-analyzed" implicit cast.
    private boolean isImplicit;

    // True if this cast does not change the type.
    private boolean noOp = false;

    public CastExpr(Type targetType, Expr e) {
        super();
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

    public static void initBuiltins(FunctionSet functionSet) {
        for (Type fromType : Type.getSupportedTypes()) {
            if (fromType.isNull() || fromType.isPseudoType()) {
                continue;
            }
            for (Type toType : Type.getSupportedTypes()) {
                if (toType.isNull() || toType.isPseudoType()) {
                    continue;
                }
                // Disable casting from boolean to decimal or datetime or date
                if (fromType.isBoolean() &&
                        (toType.isDecimalOfAnyVersion() || toType.isDate() || toType.isDatetime())) {
                    continue;
                }
                // Disable no-op casts
                // for decimalv3, wildcard type(precision=-1, scale=-1) is used here.
                // so casting a decimalv3 type to another decimalv3 type that is as wide as the former
                // is accepted.
                if (fromType.equals(toType) && !fromType.isDecimalV3()) {
                    continue;
                }

                // Disable object type(hll, bitmap, percentile) casts
                if (fromType.isOnlyMetricType() || toType.isOnlyMetricType()) {
                    continue;
                }

                String beClass =
                        toType.isDecimalV2() || fromType.isDecimalV2() ? "DecimalV2Operators" : "CastFunctions";
                if (fromType.isTime()) {
                    beClass = "TimeOperators";
                }
                String typeName = Function.getUdfTypeName(toType.getPrimitiveType());
                if (toType.getPrimitiveType() == PrimitiveType.DATE) {
                    typeName = "date_val";
                }
                String beSymbol = "starrocks::" + beClass + "::cast_to_"
                        + typeName;
                functionSet.addBuiltin(ScalarFunction.createBuiltin(getFnName(toType),
                        Lists.newArrayList(fromType), false, toType, beSymbol, null, null, true));
            }
        }
    }

    @Override
    public Expr clone() {
        return new CastExpr(this);
    }

    @Override
    public String toSqlImpl() {
        if (isImplicit) {
            return getChild(0).toSql();
        }
        if (isAnalyzed) {
            return "CAST(" + getChild(0).toSql() + " AS " + type.toString() + ")";
        } else {
            return "CAST(" + getChild(0).toSql() + " AS " + targetTypeDef.toString() + ")";
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
    protected void treeToThriftHelper(TExpr container) {
        if (getChild(0).getType().matchesType(this.type)) {
            getChild(0).treeToThriftHelper(container);
            return;
        }
        super.treeToThriftHelper(container);
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.CAST_EXPR;
        msg.setOpcode(opcode);
        msg.setOutput_column(outputColumn);
        // vectorized engine all use cast-expression
        if (this.useVectorized || (type.isNativeType() && getChild(0).getType().isNativeType())) {
            if (getChild(0).getType().isComplexType()) {
                msg.setChild_type_desc(getChild(0).getType().toThrift());
            } else {
                msg.setChild_type(getChild(0).getType().getPrimitiveType().toThrift());
            }
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
            fn = Catalog.getCurrentCatalog().getFunction(
                    searchDesc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else {
            fn = Catalog.getCurrentCatalog().getFunction(
                    searchDesc, Function.CompareMode.IS_IDENTICAL);
        }
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        Preconditions.checkState(!isImplicit);
        // When cast target type is string and it's length is default -1, the result length
        // of cast is decided by child.
        if (targetTypeDef.getType().isScalarType()) {
            final ScalarType targetType = (ScalarType) targetTypeDef.getType();
            if (!(targetType.getPrimitiveType().isStringType()
                    && !targetType.isAssignedStrLenInColDefinition())) {
                targetTypeDef.analyze(analyzer);
            }
        } else {
            targetTypeDef.analyze(analyzer);
        }
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
    public Expr getResultValue() throws AnalysisException {
        recursiveResetChildrenResult();
        final Expr value = children.get(0);
        if (!(value instanceof LiteralExpr)) {
            return this;
        }
        Expr targetExpr;
        try {
            targetExpr = castTo((LiteralExpr) value);
        } catch (AnalysisException ae) {
            targetExpr = this;
        } catch (NumberFormatException nfe) {
            targetExpr = new NullLiteral();
        }
        return targetExpr;
    }

    private Expr castTo(LiteralExpr value) throws AnalysisException {
        if (value instanceof NullLiteral) {
            return value;
        } else if (type.isIntegerType()) {
            return new IntLiteral(value.getLongValue(), type);
        } else if (type.isLargeIntType()) {
            return new LargeIntLiteral(value.getStringValue());
        } else if (type.isDecimalOfAnyVersion()) {
            // Inside DecimalLiteral constructor, narrowest decimal type is used instead, so use
            // uncheckedCastTo to make decimal literals use specified type.
            DecimalLiteral decimalLiteral = new DecimalLiteral(value.getStringValue(), type);
            decimalLiteral.uncheckedCastTo(type);
            return decimalLiteral;
        } else if (type.isFloatingPointType()) {
            return new FloatLiteral(value.getDoubleValue(), type);
        } else if (type.isStringType()) {
            return new StringLiteral(value.getStringValue());
        } else if (type.isDateType()) {
            return new DateLiteral(value.getStringValue(), type);
        } else if (type.isBoolean()) {
            return new BoolLiteral(value.getStringValue());
        }
        return this;
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public boolean isVectorized() {
        for (Expr expr : children) {
            if (!expr.isVectorized()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Below function is added by new analyzer
     */
    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) throws SemanticException {
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
        return true;
    }
}
