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

package com.starrocks.sql.automv.boolalgebra;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public enum PowerBool {
    PB_FALSE(false, false, true),
    PB_NULL(false, true, false),
    PB_FALSE_OR_NULL(false, true, true),
    PB_TRUE(true, false, false),
    PB_NOT_NULL(true, false, true),
    PB_TRUE_OR_NULL(true, true, false),
    PB_UNKNOWN(true, true, true);
    private static final int BIT_WIDTH = 32 - Integer.numberOfLeadingZeros(values().length);
    private static final int LENGTH = values().length;
    private static final int RESULT_TABLE_LENGTH = LENGTH * LENGTH;
    private static final short[] RESULT_TABLE = new short[RESULT_TABLE_LENGTH];
    private static final int MASK = (1 << BIT_WIDTH) - 1;
    private static final int NOT_SHIFT = 0;
    private static final int AND_SHIFT = BIT_WIDTH;
    private static final int OR_SHIFT = BIT_WIDTH * 2;
    private static final int EQ_SHIFT = BIT_WIDTH * 3;
    private static final int NULL_SAFE_EQ_SHIFT = BIT_WIDTH * 4;

    static {
        // lhs.ordinal*, rhs.ordinal
        for (int i = 0; i < RESULT_TABLE_LENGTH; ++i) {
            int lhsOrdinal = i % LENGTH;
            int rhsOrdinal = i / LENGTH;
            PowerBool lhs = values()[lhsOrdinal];
            PowerBool rhs = values()[rhsOrdinal];
            PowerBool notResult = PowerBool.notImpl(lhs);
            PowerBool andResult = PowerBool.andImpl(lhs, rhs);
            PowerBool orResult = PowerBool.orImpl(lhs, rhs);
            PowerBool eqResult = PowerBool.eqImpl(lhs, rhs);
            PowerBool nullSafeEqResult = PowerBool.nullSafeEqImpl(lhs, rhs);
            RESULT_TABLE[i] = (short) ((notResult.ordinal() << NOT_SHIFT) |
                    (andResult.ordinal() << AND_SHIFT) |
                    (orResult.ordinal() << OR_SHIFT) |
                    (eqResult.ordinal() << EQ_SHIFT) |
                    (nullSafeEqResult.ordinal() << NULL_SAFE_EQ_SHIFT));
        }
    }

    private final List<TriBool> states;

    PowerBool(boolean trueBit, boolean nullBit, Boolean falseBit) {
        this.states = of(trueBit, nullBit, falseBit);
    }

    private static int ordinalOf(boolean trueBit, Boolean nullBit, Boolean falseBit) {
        return ((trueBit ? 4 : 0) | (nullBit ? 2 : 0) | (falseBit ? 1 : 0)) - 1;
    }

    public static PowerBool from(Set<TriBool> set) {
        Preconditions.checkArgument(!set.isEmpty());
        boolean trueBit = set.contains(TriBool.TB_TRUE);
        boolean nullBit = set.contains(TriBool.TB_NULL);
        boolean falseBit = set.contains(TriBool.TB_FALSE);
        return values()[ordinalOf(trueBit, nullBit, falseBit)];
    }

    private static List<TriBool> of(boolean trueBit, Boolean nullBit, Boolean falseBit) {
        ImmutableList.Builder<TriBool> stateBuilder = ImmutableList.builder();
        if (trueBit) {
            stateBuilder.add(TriBool.TB_TRUE);
        }
        if (nullBit) {
            stateBuilder.add(TriBool.TB_NULL);
        }
        if (falseBit) {
            stateBuilder.add(TriBool.TB_FALSE);
        }
        return stateBuilder.build();
    }

    private static PowerBool binOp(PowerBool lhs, PowerBool rhs, BiFunction<TriBool, TriBool, TriBool> op) {
        Set<TriBool> set = lhs.getStates().stream().flatMap(a -> rhs.getStates().stream().map(b -> op.apply(a, b)))
                .collect(Collectors.toSet());
        return from(set);
    }

    private static PowerBool andImpl(PowerBool lhs, PowerBool rhs) {
        return binOp(lhs, rhs, TriBool::and);
    }

    public static PowerBool not(PowerBool a) {
        return values()[((RESULT_TABLE[a.ordinal()] >> NOT_SHIFT) & MASK)];
    }

    private static PowerBool binOp(PowerBool lhs, PowerBool rhs, int shift) {
        short entry = RESULT_TABLE[lhs.ordinal() + (rhs.ordinal() * LENGTH)];
        return values()[(entry >> shift) & MASK];
    }

    private static PowerBool orImpl(PowerBool lhs, PowerBool rhs) {
        return binOp(lhs, rhs, TriBool::or);
    }

    private static PowerBool eqImpl(PowerBool lhs, PowerBool rhs) {
        return binOp(lhs, rhs, TriBool::eq);
    }

    private static PowerBool nullSafeEqImpl(PowerBool lhs, PowerBool rhs) {
        return binOp(lhs, rhs, TriBool::nullSafeEq);
    }

    private static PowerBool notImpl(PowerBool v) {
        Set<TriBool> set = v.getStates().stream().map(TriBool::not)
                .collect(Collectors.toSet());
        boolean trueBit = set.contains(TriBool.TB_TRUE);
        boolean nullBit = set.contains(TriBool.TB_NULL);
        boolean falseBit = set.contains(TriBool.TB_FALSE);
        return values()[ordinalOf(trueBit, nullBit, falseBit)];
    }

    public List<TriBool> getStates() {
        return states;
    }

    public PowerBool and(PowerBool that) {
        return binOp(this, that, AND_SHIFT);
    }

    public PowerBool or(PowerBool that) {
        return binOp(this, that, OR_SHIFT);
    }

    public PowerBool eq(PowerBool that) {
        return binOp(this, that, EQ_SHIFT);
    }

    public PowerBool nullSafeEq(PowerBool that) {
        return binOp(this, that, NULL_SAFE_EQ_SHIFT);
    }

    public PowerBool not() {
        return not(this);
    }

    public boolean maybeTrue() {
        return this.getStates().contains(TriBool.TB_TRUE);
    }

    public boolean maybeFalse() {
        return this.getStates().contains(TriBool.TB_FALSE);
    }

    public boolean maybeNull() {
        return this.getStates().contains(TriBool.TB_NULL);
    }
}