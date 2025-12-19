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
import com.starrocks.proto.TabletRangePB;
import com.starrocks.thrift.TTabletRange;

public class TabletRange {

    @SerializedName(value = "range")
    private final Range<Tuple> range;

    public TabletRange() {
        this.range = Range.all();
    }

    public TabletRange(Range<Tuple> range) {
        this.range = range;
    }

    public Range<Tuple> getRange() {
        return this.range;
    }

    public static TabletRange fromThrift(TTabletRange tTabletRange) {
        return new TabletRange(
                Range.of(Tuple.fromThrift(tTabletRange.lower_bound), Tuple.fromThrift(tTabletRange.upper_bound),
                        tTabletRange.lower_bound_included, tTabletRange.upper_bound_included));
    }

    public static TabletRange fromProto(TabletRangePB tabletRangePB) {
        Tuple lowerBound = tabletRangePB.lowerBound != null ? Tuple.fromProto(tabletRangePB.lowerBound) : null;
        Tuple upperBound = tabletRangePB.upperBound != null ? Tuple.fromProto(tabletRangePB.upperBound) : null;
        boolean lowerIncluded = tabletRangePB.lowerBoundIncluded != null ? tabletRangePB.lowerBoundIncluded : false;
        boolean upperIncluded = tabletRangePB.upperBoundIncluded != null ? tabletRangePB.upperBoundIncluded : false;
        return new TabletRange(Range.of(lowerBound, upperBound, lowerIncluded, upperIncluded));
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
