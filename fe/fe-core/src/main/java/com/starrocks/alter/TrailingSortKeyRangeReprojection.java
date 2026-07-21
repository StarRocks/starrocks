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

package com.starrocks.alter;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Range;

import java.util.ArrayList;
import java.util.List;

/**
 * Reprojects a range-distribution tablet's boundary tuple when a trailing sort-key
 * column is appended to the table's sort key.
 */
public class TrailingSortKeyRangeReprojection {

    /**
     * Appends one {@link Variant#nullVariant(com.starrocks.type.Type)} sentinel per column in
     * {@code newTrailingSortKeyColumns} (in order) to each bounded side of {@code old}, preserving the
     * existing prefix, inclusivity, and any unbounded side.
     *
     * @param old the tablet's current boundary range, keyed on the sort key before the new columns
     * @param newTrailingSortKeyColumns the trailing sort-key columns being appended, in sort-key order
     * @return the reprojected range, keyed on the sort key including the new columns
     */
    public static Range<Tuple> appendTrailing(Range<Tuple> old, List<Column> newTrailingSortKeyColumns) {
        if (old.isAll()) {
            return Range.all();
        }
        Tuple lowerBound = old.isMinimum() ? null
                : appendNulls(old.getLowerBound(), newTrailingSortKeyColumns);
        Tuple upperBound = old.isMaximum() ? null
                : appendNulls(old.getUpperBound(), newTrailingSortKeyColumns);
        return Range.of(lowerBound, upperBound,
                old.isLowerBoundIncluded(),
                old.isUpperBoundIncluded());
    }

    private static Tuple appendNulls(Tuple tuple, List<Column> newTrailingSortKeyColumns) {
        List<Variant> values = new ArrayList<>(tuple.getValues());
        for (Column column : newTrailingSortKeyColumns) {
            values.add(Variant.nullVariant(column.getType()));
        }
        return new Tuple(values);
    }
}
