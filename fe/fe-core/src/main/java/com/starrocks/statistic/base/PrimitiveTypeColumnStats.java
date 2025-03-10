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

package com.starrocks.statistic.base;

import com.starrocks.catalog.Type;
import com.starrocks.statistic.sample.SampleInfo;

public class PrimitiveTypeColumnStats extends BaseColumnStats {
    public PrimitiveTypeColumnStats(String columnName, Type columnType) {
        super(columnName, columnType);
    }

    @Override
    public String getFullDataSize() {
        if (columnType.getPrimitiveType().isCharFamily()) {
            return "IFNULL(SUM(CHAR_LENGTH(" + getQuotedColumnName() + ")), 0)";
        }
        long typeSize = columnType.getTypeSize();
        return "COUNT(" + getQuotedColumnName() + ") * " + typeSize;
    }

    @Override
    public String getSampleDateSize(SampleInfo info) {
        if (columnType.getPrimitiveType().isCharFamily()) {
            return "IFNULL(SUM(CHAR_LENGTH(" + getQuotedColumnName() + ")) * "
                    + info.getTotalRowCount() + "/ COUNT(*), 0)";
        }
        long typeSize = columnType.getTypeSize();
        return typeSize + " * " + info.getTotalRowCount();
    }

    @Override
    public String getSampleNullCount(SampleInfo info) {
        return "(" + getFullNullCount() + ") * " + info.getTotalRowCount() + " / COUNT(*)";
    }

    @Override
    public String getMax() {
        String fn = "MAX";
        if (columnType.getPrimitiveType().isCharFamily()) {
            fn = fn + "(LEFT(" + getQuotedColumnName() + ", 200))";
        } else {
            fn = fn + "(" + getQuotedColumnName() + ")";
        }
        fn = "IFNULL(" + fn + ", '')";
        return fn;
    }

    @Override
    public String getMin() {
        String fn = "MIN";
        if (columnType.getPrimitiveType().isCharFamily()) {
            fn = fn + "(LEFT(" + getQuotedColumnName() + ", 200))";
        } else {
            fn = fn + "(" + getQuotedColumnName() + ")";
        }
        fn = "IFNULL(" + fn + ", '')";
        return fn;
    }

    @Override
    public String getNDV() {
        return "hex(hll_serialize(IFNULL(hll_raw(" + getQuotedColumnName() + "), hll_empty())))";
    }
}
