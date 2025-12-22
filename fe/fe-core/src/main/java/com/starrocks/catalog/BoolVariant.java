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
import com.starrocks.thrift.TVariant;
import com.starrocks.type.BooleanType;
import com.starrocks.type.TypeSerializer;

import java.util.Objects;

/*
 * BoolVariant is for type BOOLEAN
 */
public class BoolVariant extends Variant {

    @SerializedName(value = "value")
    protected final boolean value;

    public BoolVariant(boolean value) {
        super(BooleanType.BOOLEAN);
        this.value = value;
    }

    public BoolVariant(String value) {
        super(BooleanType.BOOLEAN);
        if (value.trim().equalsIgnoreCase("true") || value.trim().equals("1")) {
            this.value = true;
        } else if (value.trim().equalsIgnoreCase("false") || value.trim().equals("0")) {
            this.value = false;
        } else {
            throw new RuntimeException("Invalid boolean value: " + value);
        }
    }

    @Override
    public long getLongValue() {
        return value ? 1 : 0;
    }

    @Override
    public String getStringValue() {
        return value ? "TRUE" : "FALSE";
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
        if (object == null || !(object instanceof BoolVariant)) {
            return false;
        }
        BoolVariant other = (BoolVariant) object;
        return this.value == other.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
