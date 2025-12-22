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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Int128;
import com.starrocks.thrift.TVariant;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeSerializer;

import java.math.BigInteger;

/*
 * LargeIntVariant is for type LARGEINT
 */
public class LargeIntVariant extends Variant {

    @SerializedName(value = "value")
    protected final Int128 value;

    public LargeIntVariant(Int128 value) {
        super(IntegerType.LARGEINT);
        this.value = value;
    }

    public LargeIntVariant(long value) {
        this(Int128.of(value));
    }

    public LargeIntVariant(String value) {
        this(Int128.of(value));
    }

    public LargeIntVariant(BigInteger value) {
        this(Int128.of(value));
    }

    @Override
    public long getLongValue() {
        return value.toLong();
    }

    @Override
    public String getStringValue() {
        return value.toString();
    }

    @Override
    public TVariant toThrift() {
        TVariant variant = new TVariant();
        variant.setType(TypeSerializer.toThrift(type));
        variant.setValue(getStringValue());
        return variant;
    }

    public BigInteger toBigInteger() {
        return value.toBigInteger();
    }

    @Override
    public int compareTo(Variant other) {
        if (other instanceof LargeIntVariant) {
            return this.value.compareTo(((LargeIntVariant) other).value);
        } else {
            return this.value.compareTo(Int128.of(other.getLongValue()));
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || !(object instanceof LargeIntVariant)) {
            return false;
        }
        LargeIntVariant other = (LargeIntVariant) object;
        return this.value.equals(other.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
