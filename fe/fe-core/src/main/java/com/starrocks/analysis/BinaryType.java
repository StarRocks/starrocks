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

import com.starrocks.thrift.TExprOpcode;

public enum BinaryType {
    EQ("=", "eq", TExprOpcode.EQ, false),
    NE("!=", "ne", TExprOpcode.NE, false),
    LE("<=", "le", TExprOpcode.LE, true),
    GE(">=", "ge", TExprOpcode.GE, true),
    LT("<", "lt", TExprOpcode.LT, true),
    GT(">", "gt", TExprOpcode.GT, true),
    EQ_FOR_NULL("<=>", "eq_for_null", TExprOpcode.EQ_FOR_NULL, false);

    private final String type;
    private final String name;
    private final TExprOpcode opcode;
    private final boolean monotonic;

    BinaryType(String description,
               String name,
               TExprOpcode opcode,
               boolean monotonic) {
        this.type = description;
        this.name = name;
        this.opcode = opcode;
        this.monotonic = monotonic;
    }

    @Override
    public String toString() {
        return type;
    }

    public String getName() {
        return name;
    }

    public TExprOpcode getOpcode() {
        return opcode;
    }

    public boolean isEqual() {
        return type.equals(EQ.type);
    }

    public boolean isNotEqual() {
        return type.equals(NE.type);
    }

    public boolean isEquivalence() {
        return this == EQ || this == EQ_FOR_NULL;
    }

    public boolean isUnequivalence() {
        return this == NE;
    }

    public boolean isNotRangeComparison() {
        return isEquivalence() || isUnequivalence();
    }

    public boolean isRange() {
        return type.equals(LT.type)
                || type.equals(LE.type)
                || type.equals(GT.type)
                || type.equals(GE.type);
    }

    public boolean isEqualOrRange() {
        return isEqual() || isRange();
    }

    public boolean isMonotonic() {
        return monotonic;
    }
}
