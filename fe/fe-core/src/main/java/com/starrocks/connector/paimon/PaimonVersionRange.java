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

package com.starrocks.connector.paimon;

import com.starrocks.common.tvr.TvrVersion;
import com.starrocks.common.tvr.TvrVersionRange;

import java.util.Objects;

/** A scan's branch is part of its version identity, including across statistics and scan planning. */
final class PaimonVersionRange extends TvrVersionRange {
    private final TvrVersionRange range;
    private final String branch;

    PaimonVersionRange(TvrVersionRange range, String branch) {
        super(range.from(), range.to());
        this.range = range;
        this.branch = branch;
    }

    static String branchOf(TvrVersionRange range) {
        return range instanceof PaimonVersionRange ? ((PaimonVersionRange) range).branch : "main";
    }

    @Override
    public boolean isEmpty() {
        return range.isEmpty();
    }

    @Override
    public PaimonVersionRange copy(TvrVersion from, TvrVersion to) {
        return new PaimonVersionRange(range.copy(from, to), branch);
    }

    @Override
    public boolean timeRangeEquals(TvrVersionRange other) {
        return other instanceof PaimonVersionRange && branch.equals(((PaimonVersionRange) other).branch)
                && range.timeRangeEquals(((PaimonVersionRange) other).range);
    }

    @Override
    public String toString() {
        return range + " branch=" + branch;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof PaimonVersionRange
                && branch.equals(((PaimonVersionRange) other).branch)
                && range.equals(((PaimonVersionRange) other).range);
    }

    @Override
    public int hashCode() {
        return Objects.hash(range, branch);
    }
}
