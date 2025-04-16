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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/BinaryPredicate.java

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
import com.starrocks.common.Pair;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Most predicates with two operands..
 */
public class BinaryPredicate extends Predicate implements Writable {

    public static final com.google.common.base.Predicate<BinaryPredicate> IS_RANGE_PREDICATE =
            arg -> arg.getOp() == BinaryType.LT
                    || arg.getOp() == BinaryType.LE
                    || arg.getOp() == BinaryType.GT
                    || arg.getOp() == BinaryType.GE;

    public static final com.google.common.base.Predicate<BinaryPredicate> IS_EQ_PREDICATE =
            arg -> arg.getOp() == BinaryType.EQ;

    public static final com.google.common.base.Predicate<BinaryPredicate> IS_EQ_NULL_PREDICATE =
            arg -> arg.getOp() == BinaryType.EQ_FOR_NULL;

    // true if this BinaryPredicate is inferred from slot equivalences, false otherwise.
    private boolean isInferred_ = false;

    private BinaryType op;
    // check if left is slot and right isnot slot.
    private Boolean slotIsleft = null;

    // for restoring
    public BinaryPredicate() {
        super();
    }

    public BinaryPredicate(BinaryType op, Expr e1, Expr e2) {
        this(op, e1, e2, NodePosition.ZERO);
    }

    public BinaryPredicate(BinaryType op, Expr e1, Expr e2, NodePosition pos) {
        super(pos);
        this.op = op;
        this.opcode = op.getOpcode();
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkNotNull(e2);
        children.add(e2);
    }

    protected BinaryPredicate(BinaryPredicate other) {
        super(other);
        op = other.op;
        slotIsleft = other.slotIsleft;
        isInferred_ = other.isInferred_;
    }

    @Override
    public Expr clone() {
        return new BinaryPredicate(this);
    }

    public BinaryType getOp() {
        return op;
    }

    @Override
    public Expr negate() {
        BinaryType newOp = null;
        switch (op) {
            case EQ:
                newOp = BinaryType.NE;
                break;
            case NE:
                newOp = BinaryType.EQ;
                break;
            case LT:
                newOp = BinaryType.GE;
                break;
            case LE:
                newOp = BinaryType.GT;
                break;
            case GE:
                newOp = BinaryType.LT;
                break;
            case GT:
                newOp = BinaryType.LE;
                break;
            default:
                throw new IllegalStateException("Not implemented");
        }
        return new BinaryPredicate(newOp, getChild(0), getChild(1));
    }

    @Override
    public boolean equalsWithoutChild(Object obj) {
        if (!super.equalsWithoutChild(obj)) {
            return false;
        }
        return ((BinaryPredicate) obj).opcode == this.opcode;
    }

    @Override
    public String toSqlImpl() {
        return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
    }

    @Override
    public String explainImpl() {
        return getChild(0).explain() + " " + op.toString() + " " + getChild(1).explain();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.BINARY_PRED;
        msg.setOpcode(opcode);
        msg.setVector_opcode(vectorOpcode);
        if (getChild(0).getType().isComplexType()) {
            msg.setChild_type_desc(getChild(0).getType().toThrift());
        } else {
            msg.setChild_type(getChild(0).getType().getPrimitiveType().toThrift());
        }
    }

    /*
     * the follow persistence code is only for TableFamilyDeleteInfo.
     * Maybe useless
     */
    @Override
    public void write(DataOutput out) throws IOException {
        boolean isWritable = true;
        Expr left = this.getChild(0);
        if (!(left instanceof SlotRef)) {
            isWritable = false;
        }

        Expr right = this.getChild(1);
        if (!(right instanceof StringLiteral)) {
            isWritable = false;
        }

        if (isWritable) {
            out.writeInt(1);
            // write op
            Text.writeString(out, op.name());
            // write left
            Text.writeString(out, ((SlotRef) left).getColumnName());
            // write right
            Text.writeString(out, ((StringLiteral) right).getStringValue());
        } else {
            out.writeInt(0);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int isWritable = in.readInt();
        if (isWritable == 0) {
            return;
        }

        // read op
        BinaryType op = BinaryType.valueOf(Text.readString(in));
        // read left
        SlotRef left = new SlotRef(null, Text.readString(in));
        // read right
        StringLiteral right = new StringLiteral(Text.readString(in));

        this.op = op;
        this.addChild(left);
        this.addChild(right);
    }

    public static BinaryPredicate read(DataInput in) throws IOException {
        BinaryPredicate binaryPredicate = new BinaryPredicate();
        binaryPredicate.readFields(in);
        return binaryPredicate;
    }

    public Pair<SlotRef, Expr> createSlotAndLiteralPair() {
        Expr leftExpr = getChild(0);
        Expr rightExpr = getChild(1);
        if (leftExpr instanceof SlotRef && (rightExpr instanceof Parameter) &&
                (((Parameter) rightExpr).getExpr() instanceof LiteralExpr)) {
            SlotRef slot = (SlotRef) leftExpr;
            return Pair.create(slot, ((Parameter) rightExpr).getExpr());
        } else if (rightExpr instanceof SlotRef && (leftExpr instanceof Parameter) &&
                (((Parameter) leftExpr).getExpr() instanceof LiteralExpr)) {
            SlotRef slot = (SlotRef) rightExpr;
            return Pair.create(slot, ((Parameter) leftExpr).getExpr());
        }
        return null;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(op);
    }

    public boolean isNullable() {
        return !BinaryType.EQ_FOR_NULL.equals(op) && hasNullableChild();
    }

    /**
     * Below function is added by new analyzer
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBinaryPredicate(this, context);
    }

    @Override
    public boolean isSelfMonotonic() {
        return op.isMonotonic();
    }
}
