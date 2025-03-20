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

package com.starrocks.statistic.sample;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Type;

public class PrimitiveTypeColumnStats extends ColumnStats {

    public PrimitiveTypeColumnStats(String columnName, Type columnType) {
        super(columnName, columnType);
    }

    @Override
    public String getQuotedColumnName() {
        return "`" + columnName + "`";
    }

    @Override
    public String getRowCount() {
        return "IFNULL(SUM(t1.count), 0)";
    }

    @Override
    public String getDateSize() {
        String typeSize;
        if (columnType.getPrimitiveType().isCharFamily()) {
            typeSize = "IFNULL(SUM(CHAR_LENGTH(column_key)) / COUNT(1), 0)";
        } else {
            typeSize = columnType.getTypeSize() + "";
        }
        return "IFNULL(SUM(t1.count), 0) * " + typeSize;
    }

    @Override
    public String getNullCount() {
        return "IFNULL(SUM(IF(t1.column_key IS NULL, t1.count, 0)), 0)";
    }

    @Override
    public String getMax() {
        String fn = "MAX";
        if (columnType.getPrimitiveType().isCharFamily()) {
            fn = fn + "(LEFT(column_key, 200))";
        } else {
            fn = fn + "(column_key)";
        }
        fn = "IFNULL(" + fn + ", '')";
        return fn;
    }

    @Override
    public String getMin() {
        String fn = "MIN";
        if (columnType.getPrimitiveType().isCharFamily()) {
            fn = fn + "(LEFT(column_key, 200))";
        } else {
            fn = fn + "(column_key)";
        }
        fn = "IFNULL(" + fn + ", '')";
        return fn;
    }

    @Override
    public String getDistinctCount(double rowSampleRatio) {
        Preconditions.checkArgument(rowSampleRatio <= 1.0, "invalid sample ratio: " + rowSampleRatio);
        return NDVEstimator.build().generateQuery(rowSampleRatio);
    }

}
