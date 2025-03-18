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

import java.text.MessageFormat;

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

    // From PostgreSQL: n*d / (n - f1 + f1*n/N)
    // (https://github.com/postgres/postgres/blob/master/src/backend/commands/analyze.c)
    // and paper: ESTIMATING THE NUMBER OF CLASSES IN A FINITE POPULATION
    // (http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.93.8637&rep=rep1&type=pdf)
    // sample_row * count_distinct / ( sample_row - once_count + once_count * sample_row / total_row)
    @Override
    public String getDistinctCount(double rowSampleRatio) {
        Preconditions.checkArgument(rowSampleRatio <= 1.0, "invalid sample ratio: " + rowSampleRatio);
        String sampleRows = "SUM(t1.count)";
        String onceCount = "SUM(IF(t1.count = 1, 1, 0))";
        String countDistinct = "COUNT(1)";
        String fn = MessageFormat.format("{0} * {1} / ({0} - {2} + {2} * {3})", sampleRows,
                countDistinct, onceCount, String.valueOf(rowSampleRatio));
        return "IFNULL(" + fn + ", COUNT(1))";
    }
}
