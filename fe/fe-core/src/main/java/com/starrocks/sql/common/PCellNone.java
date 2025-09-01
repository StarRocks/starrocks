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

package com.starrocks.sql.common;

/**
 * PCellNone is a special PCell that represents the absence of a partition cell which can be used for non-partitioned tables.
 */
public final class PCellNone extends PCell {
    public boolean isIntersected(PCell o) {
        return false;
    }

    @Override
    public int compareTo(PCell o) {
        if (o instanceof PCellNone) {
            return 0; // Both are PCellNone, considered equal.
        }
        return -1; // PCellNone is less than any other PCell type.
    }

    @Override
    public String toString() {
        return "PCellNone";
    }

    @Override
    public int hashCode() {
        return 0; // PCellNone has no meaningful state, so it can return a constant hash code.
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PCellNone; // Only equal to another PCellNone instance.
    }
}
