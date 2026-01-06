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

import com.starrocks.thrift.TInfinityType;
import com.starrocks.thrift.TVariant;
import com.starrocks.type.Type;

import java.util.Objects;

public class MaxVariant extends Variant {
    public MaxVariant(Type type) {
        super(type);
    }

    @Override
    public String getStringValue() {
        return "MAX";
    }

    @Override
    public long getLongValue() {
        return Long.MAX_VALUE;
    }

    @Override
    public TVariant toThrift() {
        TVariant variant = new TVariant();
        variant.setType(com.starrocks.type.TypeSerializer.toThrift(type));
        variant.setInfinity_type(TInfinityType.MAXIMUM);
        return variant;
    }

    @Override
    protected int compareToImpl(Variant other) {
        throw new IllegalStateException("Should not reach here");
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof MaxVariant)) {
            return false;
        }
        MaxVariant other = (MaxVariant) object;
        return Objects.equals(this.type, other.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(MaxVariant.class, type);
    }
}

