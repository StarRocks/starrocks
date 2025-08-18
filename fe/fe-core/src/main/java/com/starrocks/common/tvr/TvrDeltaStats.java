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

public class TvrDeltaStats {
    public static final TvrDeltaStats EMPTY = new TvrDeltaStats(0);

    private final long addedRows;

    public TvrDeltaStats(long addedRows) {
        this.addedRows = addedRows;
    }

    public static TvrDeltaStats of(Long addedRows) {
        if (addedRows == null || addedRows == 0) {
            return EMPTY;
        } else {
            return new TvrDeltaStats(addedRows);
        }
    }

    public long getChangedRows() {
        return addedRows;
    }

    @Override
    public String toString() {
        return "Stats{" +
                "addedRows=" + addedRows +
                '}';
    }
}
