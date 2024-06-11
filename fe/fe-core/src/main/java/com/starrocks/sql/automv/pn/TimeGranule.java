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

package com.starrocks.sql.automv.pn;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TimeGranule {
    private final Op op;
    private final Var var;
    private final int num;
    private final Unit unit;
    private transient TimeGranule cachedWellFormedGranule = null;

    private TimeGranule(Op op, Var var, int num, Unit unit) {
        Preconditions.checkArgument(num > 0);
        this.op = Objects.requireNonNull(op);
        this.var = Objects.requireNonNull(var);
        this.num = num;
        this.unit = unit;
    }

    public static TimeGranule of(Op op) {
        if (op.getIds().size() != 1 || !op.getType().isDateType()) {
            return null;
        }

        if (op.isVar() && op.getType().isDateType()) {
            Unit unit = op.getType().isDate() ? Unit.DAY : Unit.MICROSECOND;
            return new TimeGranule(op, op.cast(), 1, unit);
        }

        if (op.isFun(FunctionSet.DATE_TRUNC)) {
            Op arg0 = op.arg(0);
            Op arg1 = op.arg(1);
            if (!arg0.isVal() || !arg0.getType().isStringType() || !arg1.isVar() || !arg1.getType().isDateType()) {
                return null;
            }
            String unit = arg0.mustCast(Val.class).getValue().getVarchar();
            Unit granuleUnit = null;
            try {
                granuleUnit = Unit.valueOf(unit.toUpperCase());
            } catch (IllegalArgumentException ignored) {
                return null;
            }
            return new TimeGranule(op, arg1.cast(), 1, granuleUnit);
        }

        if (op.isFun(FunctionSet.STR2DATE)) {
            Op arg0 = op.arg(0);
            Op arg1 = op.arg(1);
            if (!arg0.isVar() || !arg0.getType().isStringType() || !arg1.isVal() || !arg1.getType().isStringType()) {
                return null;
            }
            return new TimeGranule(op, arg0.cast(), 1, Unit.DAY);
        }
        return null;
    }

    public static Comparator<TimeGranule> getComparator() {
        return (lhs, rhs) -> {
            TimeGranule wellFormedLhs = lhs.toWellFormed();
            TimeGranule wellFormedRhs = rhs.toWellFormed();
            long lhsDuration = wellFormedLhs.unit.microSeconds() * wellFormedLhs.num;
            long rhsDuration = wellFormedRhs.unit.microSeconds() * wellFormedLhs.num;
            int r = Long.compare(lhsDuration, rhsDuration);
            if (r != 0) {
                return r;
            }
            Type lhsType = wellFormedLhs.var.getType();
            Type rhsType = wellFormedRhs.var.getType();
            int lhsTypeOrdinal = lhsType.isStringType() ? 0 : (lhsType.isDatetime() ? 1 : 2);
            int rhsTypeOrindal = rhsType.isStringType() ? 0 : (rhsType.isDatetime() ? 1 : 2);
            return Integer.compare(lhsTypeOrdinal, rhsTypeOrindal);
        };
    }

    public static void validate(String granule) {
        if (granule.equals("none")) {
            return;
        }
        List<String> acceptableGranules = Stream.of(
                TimeGranule.Unit.HOUR,
                TimeGranule.Unit.DAY,
                TimeGranule.Unit.MONTH,
                TimeGranule.Unit.QUARTER,
                TimeGranule.Unit.YEAR).map(
                Enum::name).collect(Collectors.toList());

        if (!acceptableGranules.contains(granule)) {
            String acceptableValues = String.join("/", acceptableGranules);
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s', acceptable values are %s", granule, acceptableValues));
        }
    }

    public Op getOp() {
        return op;
    }

    public Var getVar() {
        return var;
    }

    public int getNum() {
        return num;
    }

    public Unit getUnit() {
        return unit;
    }

    public boolean isFineGrained(Unit unitInclusive) {
        TimeGranule wellFormed = toWellFormed();
        return wellFormed.unit.ordinal() <= unitInclusive.ordinal();
    }

    private TimeGranule toWellFormedImpl() {
        if (var.getType().isDate() && unit.ordinal() <= Unit.DAY.ordinal()) {
            return Objects.requireNonNull(of(var));
        }
        if (var.getType().isDatetime() && unit.ordinal() == Unit.MICROSECOND.ordinal()) {
            return Objects.requireNonNull(of(var));
        }
        // duplicate itself to avoid self-referencing.
        return new TimeGranule(this.op, this.var, this.num, this.unit);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TimeGranule wellFormedThis = this.toWellFormed();
        TimeGranule wellFormedThat = ((TimeGranule) o).toWellFormed();
        return wellFormedThis.num == wellFormedThat.num &&
                wellFormedThis.unit == wellFormedThat.unit &&
                wellFormedThis.op.strict().equals(wellFormedThat.op.strict());
    }

    @Override
    public int hashCode() {
        TimeGranule wellFormed = this.toWellFormed();
        return Objects.hash(wellFormed.op, wellFormed.var, wellFormed.num, wellFormed.unit);
    }

    public TimeGranule toWellFormed() {
        if (cachedWellFormedGranule == null) {
            cachedWellFormedGranule = toWellFormedImpl();
        }
        return cachedWellFormedGranule;
    }

    public TimeGranule toCoarse(Unit coarseUnit) {
        if (var.getType().isDate()) {
            int maxOrdinal = Math.max(coarseUnit.ordinal(), Unit.DAY.ordinal());
            Unit effectiveUnit = Unit.values()[maxOrdinal];
            if (effectiveUnit.equals(Unit.DAY) && unit.ordinal() <= effectiveUnit.ordinal()) {
                return of(var);
            } else if (unit.ordinal() < effectiveUnit.ordinal()) {
                Op unitOp = Op.val(ConstantOperator.createVarchar(effectiveUnit.name().toLowerCase()));
                List<Op> args = ImmutableList.of(unitOp, var);
                Op op = Op.apply(var.getType(), FunctionSet.DATE_TRUNC, true, args);
                return of(op);
            } else {
                return this;
            }
        }

        if (var.getType().isDatetime()) {
            if (unit.ordinal() < coarseUnit.ordinal()) {
                Op unitOp = Op.val(ConstantOperator.createVarchar(coarseUnit.name().toLowerCase()));
                List<Op> args = ImmutableList.of(unitOp, var);
                Op op = Op.apply(var.getType(), FunctionSet.DATE_TRUNC, true, args);
                return of(op);
            } else {
                return this;
            }
        }
        if (var.getType().isStringType()) {
            Preconditions.checkArgument(op.isFun(FunctionSet.STR2DATE));
            return this;
        }
        return null;
    }

    public enum Unit {
        MICROSECOND,
        MILLISECOND,
        SECOND,
        MINUTE,
        HOUR,
        DAY,
        WEEK,
        MONTH,
        QUARTER,
        YEAR;
        private static final long[] MICRO_SECONDS_TABLE = new long[] {
                1L,
                1_000L,
                1_000_000L,
                60L * 1_000_000L,
                60L * 60L * 1_000_000L,
                24L * 60L * 60L * 1_000_000L,
                7L * 24L * 60L * 60L * 1_000_000L,
                30L * 24L * 60L * 60L * 1_000_000L,
                3 * 30L * 24L * 60L * 60L * 1_000_000L,
                365L * 24L * 60L * 60L * 1_000_000L,
        };

        public long microSeconds() {
            return MICRO_SECONDS_TABLE[this.ordinal()];
        }
    }
}
