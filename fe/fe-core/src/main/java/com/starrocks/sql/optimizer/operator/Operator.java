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

package com.starrocks.sql.optimizer.operator;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Objects;

public abstract class Operator {
    public static final long DEFAULT_LIMIT = -1;
    public static final long DEFAULT_OFFSET = 0;

    protected final OperatorType opType;
    protected long limit = DEFAULT_LIMIT;
    protected ScalarOperator predicate = null;

    private static long saltGenerator = 0;
    /**
     * Before entering the Cascades search framework,
     * we need to merge LogicalProject and child children into one node
     * to reduce the impact of LogicalProject on RULE matching
     * such as Join reorder
     */
    protected Projection projection;

    protected RowOutputInfo rowOutputInfo;

    // Add salt make the original equivalent operators nonequivalent to avoid Group
    // mutual reference in Memo.
    // Only LogicalScanOperator/PhysicalScanOperator yielded by CboTablePruneRule has salt.
    // if no salt, two different Groups will be merged into one, that leads to mutual reference
    // or self reference of groups
    protected long salt = 0;

    // Like LogicalJoinOperator#transformMask, add a mask to avoid one operator's dead-loop in one transform rule.
    // eg: MV's UNION-ALL RULE:
    //                 UNION                         UNION
    //               /        \                    /       \
    //  OP -->   EXTRA-OP    MV-SCAN  -->     UNION    MV-SCAN     ---> ....
    //                                       /      \
    //                                  EXTRA-OP    MV-SCAN
    protected int opRuleMask = 0;

    // an operator logically equivalent to 'this' operator
    // used by view based mv rewrite
    // eg: LogicalViewScanOperator is logically equivalent to the operator build from the view
    protected Operator equivalentOp;

    public Operator(OperatorType opType) {
        this.opType = opType;
    }

    public Operator(OperatorType opType, long limit, ScalarOperator predicate, Projection projection) {
        this.opType = opType;
        this.limit = limit;
        this.predicate = predicate;
        this.projection = projection;
    }

    @SuppressWarnings("unchecked")
    public <T extends Operator> T cast() {
        return (T) this;
    }

    public boolean isLogical() {
        return false;
    }

    public boolean isPhysical() {
        return false;
    }

    public OperatorType getOpType() {
        return opType;
    }

    public long getLimit() {
        return limit;
    }

    @Deprecated
    public void setLimit(long limit) {
        this.limit = limit;
    }

    public boolean hasLimit() {
        return limit != DEFAULT_LIMIT;
    }

    public ScalarOperator getPredicate() {
        return predicate;
    }

    @Deprecated
    public void setPredicate(ScalarOperator predicate) {
        this.predicate = predicate;
    }

    public Projection getProjection() {
        return projection;
    }

    public void setProjection(Projection projection) {
        this.projection = projection;
    }

    public void addSalt() {
        if ((this instanceof LogicalJoinOperator) || (this instanceof LogicalScanOperator)) {
            this.salt = ++saltGenerator;
        }
    }

    public void setSalt(long salt) {
        if ((this instanceof LogicalJoinOperator) ||
                (this instanceof LogicalScanOperator) ||
                (this instanceof PhysicalScanOperator) ||
                (this instanceof PhysicalJoinOperator)) {
            this.salt = salt;
        }
    }
    public boolean hasSalt() {
        return salt > 0;
    }

    public long getSalt() {
        return salt;
    }

    public int getOpRuleMask() {
        return opRuleMask;
    }

    public void setOpRuleMask(int b) {
        this.opRuleMask = b;
    }

    public Operator getEquivalentOp() {
        return equivalentOp;
    }

    public void setEquivalentOp(Operator equivalentOp) {
        this.equivalentOp = equivalentOp;
    }

    public RowOutputInfo getRowOutputInfo(List<OptExpression> inputs) {
        if (rowOutputInfo == null) {
            rowOutputInfo = deriveRowOutputInfo(inputs);
        }

        // transformation may update the projection, so update the rowOutputInfo at the same time
        if (projection != null) {
            rowOutputInfo = new RowOutputInfo(projection.getColumnRefMap(), projection.getCommonSubOperatorMap(),
                    rowOutputInfo.getOriginalColOutputInfo(), rowOutputInfo.getEndogenousCols());
        }
        return rowOutputInfo;
    }

    protected RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        throw new UnsupportedOperationException();
    }

    protected RowOutputInfo projectInputRow(RowOutputInfo inputRow) {
        List<ColumnOutputInfo> entryList = Lists.newArrayList();
        for (ColumnOutputInfo columnOutputInfo : inputRow.getColumnOutputInfo()) {
            entryList.add(new ColumnOutputInfo(columnOutputInfo.getColumnRef(), columnOutputInfo.getColumnRef()));
        }
        return new RowOutputInfo(entryList);
    }

    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitOperator(this, context);
    }

    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visit(optExpression, context);
    }

    @Override
    public String toString() {
        return opType.name();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Operator operator = (Operator) o;
        return limit == operator.limit && opType == operator.opType &&
                Objects.equals(predicate, operator.predicate) &&
                Objects.equals(projection, operator.projection) &&
                Objects.equals(salt, operator.salt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(opType.ordinal(), limit, predicate, projection, salt);
    }

    public abstract static class Builder<O extends Operator, B extends Builder> {
        protected O builder = newInstance();

        protected abstract O newInstance();

        public B withOperator(O operator) {
            builder.limit = operator.limit;
            builder.predicate = operator.predicate;
            builder.projection = operator.projection;
            builder.salt = operator.salt;
            builder.opRuleMask = operator.opRuleMask;
            builder.equivalentOp = operator.equivalentOp;
            return (B) this;
        }

        public O build() {
            O newOne = builder;
            builder = null;
            return newOne;
        }

        public OperatorType getOpType() {
            return builder.opType;
        }

        public long getLimit() {
            return builder.limit;
        }

        public B setLimit(long limit) {
            builder.limit = limit;
            return (B) this;
        }

        public ScalarOperator getPredicate() {
            return builder.predicate;
        }

        public B setPredicate(ScalarOperator predicate) {
            builder.predicate = predicate;
            return (B) this;
        }

        public Projection getProjection() {
            return builder.projection;
        }

        public B setProjection(Projection projection) {
            builder.projection = projection;
            return (B) this;
        }

        public B addSalt() {
            builder.salt = ++saltGenerator;
            return (B) this;
        }

        public B setOpBitSet(int opRuleMask) {
            builder.opRuleMask = opRuleMask;
            return (B) this;
        }
    }
}
