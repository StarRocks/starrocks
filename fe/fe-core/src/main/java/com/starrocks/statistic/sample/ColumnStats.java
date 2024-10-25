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

import com.starrocks.catalog.Type;
import org.apache.commons.lang.StringEscapeUtils;

public abstract class ColumnStats {

    protected final String columnName;

    protected final Type columnType;

    protected ColumnStats(String columnName, Type columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
    }

    public String getColumnName() {
        return StringEscapeUtils.escapeSql(columnName);
    }

    public abstract String getQuotedColumnName();

    public abstract String getRowCount();

    public abstract String getDateSize();

    public abstract String getNullCount();

    public abstract String getMax();

    public abstract String getMin();

    public abstract String getDistinctCount(double rowSampleRatio);

}
