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
import com.starrocks.proto.TuplePB;
import com.starrocks.thrift.TTuple;

import java.util.List;
import java.util.stream.Collectors;

public class Tuple implements Comparable<Tuple> {

    @SerializedName(value = "values")
    private final List<Variant> values;

    public Tuple(List<Variant> values) {
        this.values = values;
    }

    public List<Variant> getValues() {
        return this.values;
    }

    @Override
    public int compareTo(Tuple other) {
        return Variant.compatibleCompare(this.values, other.values);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || !(object instanceof Tuple)) {
            return false;
        }
        Tuple other = (Tuple) object;
        return this.values.equals(other.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    public static Tuple fromThrift(TTuple tTuple) {
        return new Tuple(tTuple.values.stream().map(v -> Variant.fromThrift(v)).collect(Collectors.toList()));
    }

    public static Tuple fromProto(TuplePB tuplePB) {
        return new Tuple(tuplePB.values.stream().map(v -> Variant.fromProto(v)).collect(Collectors.toList()));
    }

    public TTuple toThrift() {
        TTuple tuple = new TTuple();
        tuple.setValues(values.stream().map(Variant::toThrift).collect(Collectors.toList()));
        return tuple;
    }
}
