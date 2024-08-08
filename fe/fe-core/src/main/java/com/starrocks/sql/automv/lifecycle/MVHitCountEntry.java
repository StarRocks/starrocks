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

package com.starrocks.sql.automv.lifecycle;

import com.google.common.collect.ImmutableList;
import com.starrocks.sql.automv.qe.ColumnPlus;
import com.starrocks.sql.automv.util.ColumnDescription;

import java.util.List;
import java.util.stream.Stream;

import static com.starrocks.sql.automv.qe.ColumnPlus.BIGINT;
import static com.starrocks.sql.automv.qe.ColumnPlus.VARCHAR;

public class MVHitCountEntry {
    public static final List<ColumnPlus> COLUMNS = collectColumns();
    @ColumnDescription(type = VARCHAR)
    private String mv;
    @ColumnDescription(type = BIGINT)
    private long count;

    private static List<ColumnPlus> collectColumns() {
        return Stream.of(MVHitCountEntry.class.getDeclaredFields())
                .filter(ColumnPlus::isAcceptable)
                .map(ColumnPlus::fieldToColumn)
                .collect(ImmutableList.toImmutableList());
    }

    public static List<ColumnPlus> getColumns() {
        return COLUMNS;
    }

    public String getMv() {
        return mv;
    }

    public void setMv(String mv) {
        this.mv = mv;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
