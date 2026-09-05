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

package com.starrocks.common.tvr;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

import java.util.Objects;
import java.util.Optional;

/**
 * TvrVersionRange represents a range of versions in a Time Varying Relation (TVR).
 */
public abstract class TvrVersionRange {
    @SerializedName("from")
    public final TvrVersion from;

    @SerializedName("to")
    public final TvrVersion to;

    protected TvrVersionRange(TvrVersion from, TvrVersion to) {
        Preconditions.checkArgument(from != null && to != null,
                "TvrVersionRange from and to cannot be null");
        this.from = from;
        this.to = to;
    }

    public boolean timeRangeEquals(TvrVersionRange other) {
        return Objects.equals(from, other.from) &&
                Objects.equals(to, other.to);
    }

    public TvrVersion from() {
        return from;
    }

    public TvrVersion to() {
        return to;
    }

    public Optional<Long> start() {
        if (from.isMinOrMax()) {
            return Optional.empty();
        } else {
            return Optional.of(from.getVersion());
        }
    }

    public Optional<Long> end() {
        if (to.isMinOrMax()) {
            return Optional.empty();
        } else {
            return Optional.of(to.getVersion());
        }
    }

    public boolean isEmpty() {
        return (from.isMin() && to.isMax()) || (from.equals(to));
    }

    public abstract TvrVersionRange copy(TvrVersion from, TvrVersion to);

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TvrVersionRange)) {
            return false;
        }
        TvrVersionRange other = (TvrVersionRange) obj;
        return timeRangeEquals(other);
    }

    @Override
    public String toString() {
        return "Delta@(" + from + ", " + to + ")";
    }
}
