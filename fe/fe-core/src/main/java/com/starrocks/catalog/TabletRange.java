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
import com.starrocks.common.Range;
import com.starrocks.thrift.TTabletRange;

public class TabletRange {
    @SerializedName(value = "range")
    private final Range<Tuple> range;

    public TabletRange(Range<Tuple> range) {
        this.range = range;
    }

    public Range<Tuple> getRange() {
        return this.range;
    }

    public TTabletRange toThrift() {
        TTabletRange tRange = new TTabletRange();
        tRange.setLower_bound_included(range.isLowerBoundIncluded());
        tRange.setUpper_bound_included(range.isUpperBoundIncluded());

        if (!range.isMinimum()) {
            tRange.setLower_bound(range.getLowerBound().toThrift());
        }
        if (!range.isMaximum()) {
            tRange.setUpper_bound(range.getUpperBound().toThrift());
        }
        return tRange;
    }
}
