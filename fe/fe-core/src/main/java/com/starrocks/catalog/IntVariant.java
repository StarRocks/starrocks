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
import com.starrocks.thrift.TVariant;
import com.starrocks.type.Type;
import com.starrocks.type.TypeSerializer;

import java.util.Objects;

/*
 * IntVariant is for type TINYINT, SMALLINT, INT and BIGINT
 */
public class IntVariant extends Variant {

    @SerializedName(value = "value")
    protected final long value;

    static long parseLong(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid int value: " + value, e);
        }
    }

    public IntVariant(Type type, long value) {
        super(type);
        this.value = value;
        Preconditions.checkArgument(checkNumberRange(), "Number out of range, type: " + type + ", value: " + value);
    }

    public IntVariant(Type type, String value) {
        this(type, IntVariant.parseLong(value));
    }

    @Override
    public long getLongValue() {
        return value;
    }

    @Override
    public String getStringValue() {
        return String.valueOf(value);
    }

    @Override
    public TVariant toThrift() {
        TVariant variant = new TVariant();
        variant.setType(TypeSerializer.toThrift(type));
        variant.setValue(getStringValue());
        return variant;
    }

    @Override
    public int compareTo(Variant other) {
        if (other instanceof LargeIntVariant) {
            return -other.compareTo(this);
        }
        return Long.compare(this.getLongValue(), other.getLongValue());
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || !(object instanceof IntVariant)) {
            return false;
        }
        IntVariant other = (IntVariant) object;
        return this.value == other.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    private boolean checkNumberRange() {
        switch (type.getPrimitiveType()) {
            case TINYINT:
                return (this.value >= Byte.MIN_VALUE && this.value <= Byte.MAX_VALUE);
            case SMALLINT:
                return (this.value >= Short.MIN_VALUE && this.value <= Short.MAX_VALUE);
            case INT:
                return (this.value >= Integer.MIN_VALUE && this.value <= Integer.MAX_VALUE);
            case BIGINT:
                return true;
            default:
                throw new IllegalArgumentException("Invalid type: " + type);
        }
    }
}
