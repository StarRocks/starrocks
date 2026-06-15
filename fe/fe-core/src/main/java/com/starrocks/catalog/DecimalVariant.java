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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.type.Type;

import java.math.BigDecimal;
import java.util.Objects;

/*
 * DecimalVariant is for the decimal types DECIMALV2 and DECIMAL32/64/128/256.
 * The column's ScalarType (carrying precision/scale) is held by the base type
 * field so an external split-boundary tuple serializes a scalar type the backend
 * validator can match exactly. getStringValue() renders canonical plain decimal
 * text (no scientific notation) that the backend's datum_from_string parses.
 */
public class DecimalVariant extends Variant {

    @SerializedName(value = "value")
    protected final BigDecimal value;

    static BigDecimal parseDecimal(String value) {
        try {
            return new BigDecimal(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid decimal value: " + value, e);
        }
    }

    public DecimalVariant(Type type, BigDecimal value) {
        super(type);
        // Carry a decimal ScalarType: getStringValue/validation/serialization all assume
        // one, and BoundaryPlanner casts the value type to ScalarType for the BE-mirrored
        // precision/scale check. Reject a non-decimal type at construction.
        Preconditions.checkArgument(type.isDecimalOfAnyVersion(),
                "DecimalVariant requires a decimal type, got %s", type);
        this.value = Objects.requireNonNull(value, "value");
    }

    public DecimalVariant(Type type, String value) {
        this(type, DecimalVariant.parseDecimal(value));
    }

    @Override
    public long getLongValue() {
        // Integer part only. Decimal sort keys are compared through compareToImpl
        // (DecimalVariant vs DecimalVariant), not the long-comparison path, but the
        // abstract Variant contract requires a long.
        return value.longValue();
    }

    @Override
    public String getStringValue() {
        // Canonical plain decimal so the backend's datum_from_string accepts it. The
        // backend rounds fractional digits beyond the column scale half-up; only an
        // over-precision integer part is rejected (then the split substrate falls back
        // to an identical tablet rather than publishing a wrong boundary).
        return value.toPlainString();
    }

    @Override
    protected int compareToImpl(Variant other) {
        Preconditions.checkArgument(other instanceof DecimalVariant, other);
        return this.value.compareTo(((DecimalVariant) other).value);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || !(object instanceof DecimalVariant)) {
            return false;
        }
        // Value-based equality, kept consistent with compareTo: BigDecimal.compareTo is
        // scale-independent (1.0 == 1.00), so equals must be too. BigDecimal.equals would be
        // scale-sensitive and break Tuple equals/hashCode-based dedup (e.g.
        // ColocateRangeMgr.hasBoundaryAt, any Set<Tuple>) when the same boundary value is
        // written at different scales.
        return this.value.compareTo(((DecimalVariant) object).value) == 0;
    }

    @Override
    public int hashCode() {
        // stripTrailingZeros canonicalizes scale so values equal under compareTo hash equally.
        return value.stripTrailingZeros().hashCode();
    }
}
