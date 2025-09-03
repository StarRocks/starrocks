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

package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class ScalarOperator implements Cloneable {
    protected final OperatorType opType;
    protected Type type;
    // this operator will not eval in predicate estimate
    protected boolean notEvalEstimate = false;
    // Used to determine if it is derive from predicate range extractor
    protected boolean fromPredicateRangeDerive = false;
    // Check weather the scalar operator is redundant which will not affect the
    // final
    // if it's not considered. eg, `IsNullPredicateOperator` which is pushed down
    // from JoinNode.
    protected boolean isRedundant = false;

    // whether the ScalarOperator is pushdown from equivalence derivation
    protected boolean isPushdown = false;

    protected boolean isCorrelated = false;

    protected boolean isJoinDerived = false;

    private List<String> hints = Collections.emptyList();

    private boolean isIndexOnlyFilter = false;

    // 1. depth is scalar operator's nested depth, it starts from 0(eg:
    // ColumnRefOperator/ConstantOperator), incr +1 for each
    // child nested; if it contains multi children, the max depth of children will
    // be added to this operator's depth.
    // 2. depth is marked to avoid infinite loop in some cases.
    protected int depth = 0;

    protected Optional<Integer> numberFlatChildren = Optional.empty();

    public ScalarOperator(OperatorType opType, Type type) {
        this.opType = requireNonNull(opType, "opType is null");
        this.type = requireNonNull(type, "type is null");
    }

    @SuppressWarnings("unchecked")
    public <T extends ScalarOperator> T cast() {
        return (T) this;
    }

    public boolean isConstant() {
        for (ScalarOperator child : getChildren()) {
            if (!child.isConstant()) {
                return false;
            }
        }

        return true;
    }

    public boolean isTrue() {
        return this.equals(ConstantOperator.TRUE);
    }

    public boolean isFalse() {
        return this.equals(ConstantOperator.FALSE);
    }

    public abstract boolean isNullable();

    public OperatorType getOpType() {
        return opType;
    }

    public boolean isVariable() {
        for (ScalarOperator child : getChildren()) {
            if (child.isVariable()) {
                return true;
            }
        }

        return false;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public boolean isNotEvalEstimate() {
        return this.notEvalEstimate;
    }

    public void setNotEvalEstimate(boolean notEvalInPredicateEstimate) {
        this.notEvalEstimate = notEvalInPredicateEstimate;
    }

    public boolean isFromPredicateRangeDerive() {
        return fromPredicateRangeDerive;
    }

    public void setFromPredicateRangeDerive(boolean fromPredicateRangeDerive) {
        this.fromPredicateRangeDerive = fromPredicateRangeDerive;
    }

    public boolean isIndexOnlyFilter() {
        boolean result = isIndexOnlyFilter;
        for (ScalarOperator child : getChildren()) {
            result = result || child.isIndexOnlyFilter();
        }
        return result;
    }

    public void setIndexOnlyFilter(boolean indexOnlyFilter) {
        isIndexOnlyFilter = indexOnlyFilter;
    }

    public abstract List<ScalarOperator> getChildren();

    public abstract ScalarOperator getChild(int index);

    public abstract void setChild(int index, ScalarOperator child);

    @Override
    public abstract String toString();

    @Override
    public int hashCode() {
        return hashCodeSelf();
    }

    /**
     * Calculate hashCode based on the operator's own properties, excluding
     * children/arguments.
     * This method should be implemented by subclasses to provide a hashCode that
     * only considers
     * the operator's intrinsic properties, not its children.
     */
    public abstract int hashCodeSelf();

    /**
     * Check equality based on the operator's own properties, excluding
     * children/arguments.
     * This method should be implemented by subclasses to provide an equals that
     * only considers
     * the operator's intrinsic properties, not its children.
     */
    public abstract boolean equalsSelf(Object other);

    @Override
    public boolean equals(Object other) {
        return equalsSelf(other);
    }

    public int getDepth() {
        return depth;
    }

    public int getNumFlatChildren() {
        if (numberFlatChildren.isPresent()) {
            return numberFlatChildren.get();
        }
        int numFlatChildren = 1;
        for (ScalarOperator child : getChildren()) {
            numFlatChildren += child.getNumFlatChildren();
        }
        numberFlatChildren = Optional.of(numFlatChildren);
        return numberFlatChildren.get();
    }

    /**
     * Incr depth for this operator: this.depth = 1 + max(depth of children)
     */
    public void incrDepth(List<ScalarOperator> args) {
        // always add 1 for self
        this.depth += 1;
        if (args == null) {
            return;
        }
        this.depth += args.stream().map(ScalarOperator::getDepth).max(Integer::compareTo).orElse(0);
    }

    /**
     * Incr depth for this operator: this.depth = 1 + max(depth of children)
     */
    public void incrDepth(ScalarOperator... args) {
        // always add 1 for self
        this.depth += 1;

        if (args == null) {
            return;
        }
        int ans = 0;
        for (ScalarOperator arg : args) {
            ans = Math.max(ans, arg.getDepth());
        }
        this.depth += ans;
    }

    /**
     * equivalent means logical equals, but may physical different, such as with different id
     */
    public boolean equivalent(Object other) {
        return equals(other);
    }

    public abstract <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context);

    // Default Shallow Clone for ConstantOperator and ColumnRefOperator
    @Override
    public ScalarOperator clone() {
        ScalarOperator operator = null;
        try {
            operator = (ScalarOperator) super.clone();
            operator.hints = Lists.newArrayList(hints);
            operator.isRedundant = this.isRedundant;
            operator.isPushdown = this.isPushdown;
        } catch (CloneNotSupportedException ignored) {
        }
        return operator;
    }

    /**
     * Return the columns that this scalar operator used.
     * For a + b, the used columns are a and b.
     */
    public abstract ColumnRefSet getUsedColumns();

    public List<ColumnRefOperator> getColumnRefs() {
        List<ColumnRefOperator> columns = Lists.newArrayList();
        getColumnRefs(columns);
        return columns;
    }

    public void getColumnRefs(List<ColumnRefOperator> columns) {
        for (ScalarOperator child : getChildren()) {
            child.getColumnRefs(columns);
        }
    }

    public String debugString() {
        return toString();
    }

    public boolean isColumnRef() {
        return this instanceof ColumnRefOperator;
    }

    public boolean isCast() {
        return this instanceof CastOperator;
    }

    public boolean isConstantRef() {
        return this instanceof ConstantOperator;
    }

    public boolean isConstantNull() {
        return this instanceof ConstantOperator && ((ConstantOperator) this).isNull();
    }

    public boolean isConstantZero() {
        return this instanceof ConstantOperator && ((ConstantOperator) this).isZero();
    }

    public boolean isConstantFalse() {
        return this instanceof ConstantOperator && this.getType() == Type.BOOLEAN &&
                !((ConstantOperator) this).getBoolean();
    }

    public boolean isConstantTrue() {
        return this instanceof ConstantOperator && this.getType() == Type.BOOLEAN &&
                ((ConstantOperator) this).getBoolean();
    }

    public boolean isConstantNullOrFalse() {
        return isConstantNull() || isConstantFalse();
    }

    public void setHints(List<String> hints) {
        this.hints = hints;
    }

    public List<String> getHints() {
        return hints;
    }

    public boolean isRedundant() {
        return isRedundant;
    }

    public void setRedundant(boolean redundant) {
        isRedundant = redundant;
    }

    public boolean isPushdown() {
        return isPushdown;
    }

    public void setIsPushdown(boolean isPushdown) {
        this.isPushdown = isPushdown;
    }

    public boolean isCorrelated() {
        return isCorrelated;
    }

    public void setCorrelated(boolean correlated) {
        isCorrelated = correlated;
    }

    public boolean isJoinDerived() {
        return isJoinDerived;
    }

    public void setJoinDerived(boolean isJoinDerived) {
        this.isJoinDerived = isJoinDerived;
    }

    // whether ScalarOperator are equals without id
    public static boolean isEquivalent(ScalarOperator left, ScalarOperator right) {
        if (!left.getOpType().equals(right.getOpType())) {
            return false;
        }

        boolean ret = left.equivalent(right);
        if (!ret) {
            return false;
        }
        Preconditions.checkState(left.getChildren().size() == right.getChildren().size());
        for (int i = 0; i < left.getChildren().size(); i++) {
            if (!isEquivalent(left.getChild(i), right.getChild(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isColumnEqualBinaryPredicate(ScalarOperator predicate) {
        if (predicate instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) predicate;
            return binaryPredicate.getBinaryType().isEquivalence()
                    && binaryPredicate.getChild(0).isColumnRef() && binaryPredicate.getChild(1).isColumnRef();
        }
        return false;
    }

    public static boolean isColumnEqualConstant(ScalarOperator predicate) {
        if (predicate instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) predicate;
            return binaryPredicate.getBinaryType().isEquivalence()
                    && binaryPredicate.getChild(0).isColumnRef() && binaryPredicate.getChild(1).isConstantRef();
        }
        return false;
    }

    public static void updateLiteralPredicates(ScalarOperator predicate, List<Expr> exprs) {
        if (predicate instanceof CompoundPredicateOperator) {
            updateCompoundLiteralPredicate((CompoundPredicateOperator) predicate, exprs);
        } else {
            updateSingleLiteralPredicate(predicate, exprs.get(0));
        }
    }

    private static void updateCompoundLiteralPredicate(CompoundPredicateOperator compoundPredicate, List<Expr> exprs) {
        for (int i = 0; i < compoundPredicate.getChildren().size(); i++) {
            updateSingleLiteralPredicate(compoundPredicate.getChild(i), exprs.get(i));
        }
    }

    private static void updateSingleLiteralPredicate(ScalarOperator predicate, Expr expr) {
        Object realObjectValue = ((LiteralExpr) expr).getRealObjectValue();
        Optional<ConstantOperator> constantOperator =
                new ConstantOperator(realObjectValue, expr.getType()).castTo(predicate.getChild(1).getType());
        if (constantOperator.isPresent()) {
            predicate.setChild(1, constantOperator.get());
        }
    }
}
