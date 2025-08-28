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

import com.google.gson.annotations.SerializedName;

/**
 * TvrVersion represents a specific version in a Time Varying Relation (TVR).
 */
public class TvrVersion implements Comparable<TvrVersion> {

    @SerializedName("version")
    private final long version;

    public static final long MIN_TIME = Long.MIN_VALUE;
    public static final long MAX_TIME = Long.MAX_VALUE;
    public static final TvrVersion MIN = new TvrVersion(Long.MIN_VALUE);
    public static final TvrVersion MAX = new TvrVersion(Long.MAX_VALUE);

    protected TvrVersion(long version) {
        this.version = version;
    }

    public static TvrVersion of(long version) {
        if (version == MIN_TIME) {
            return MIN;
        } else if (version == MAX_TIME) {
            return MAX;
        }

        return new TvrVersion(version);
    }

    public boolean isMax() {
        return this.equals(MAX);
    }

    public boolean isMin() {
        return this.equals(MIN);
    }

    public boolean isMinOrMax() {
        return isMin() || isMax();
    }

    public long getVersion() {
        return version;
    }

    @Override
    public String toString() {
        if (version == MIN_TIME) {
            return "MIN";
        } else if (version == MAX_TIME) {
            return "MAX";
        } else {
            return String.valueOf(version);
        }
    }

    @Override
    public int compareTo(TvrVersion o) {
        if (this == MAX && o != MAX) {
            return 1;
        } else if (this != MAX && o == MAX) {
            return -1;
        } else if (this == MIN && o != MIN) {
            return -1;
        } else if (this != MIN && o == MIN) {
            return 1;
        }
        return Long.compare(this.version, o.version);
    }

    public boolean isAfter(TvrVersion other) {
        return this.compareTo(other) > 0;
    }

    public boolean isBefore(TvrVersion other) {
        return this.compareTo(other) < 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TvrVersion)) {
            return false;
        }
        TvrVersion that = (TvrVersion) o;
        return version == that.version;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(version);
    }
}