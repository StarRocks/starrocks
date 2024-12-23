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
import org.apache.commons.lang.StringEscapeUtils;

import static com.starrocks.sql.optimizer.statistics.ColumnStatistic.DEFAULT_COLLECTION_SIZE;

/*
 * For describe how to collect statistics on different column type
 */
public abstract class ColumnStats {

    protected final String columnName;

    protected final Type columnType;

    protected ColumnStats(String columnName, Type columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
    }

    public boolean supportMeta() {
        return columnType.canStatistic() && !columnType.getPrimitiveType().isCharFamily();
    }

    public boolean supportData() {
        return columnType.canStatistic();
    }

    public long getTypeSize() {
        return columnType.getTypeSize();
    }

    public String getColumnNameStr() {
        return StringEscapeUtils.escapeSql(columnName);
    }

    public String getQuotedColumnName() {
        return "`" + columnName + "`";
    }

    public abstract String getMax();

    public abstract String getMin();

    public String getCollectionSize() {
        return String.valueOf(DEFAULT_COLLECTION_SIZE);
    }

    public abstract String getFullDateSize();

    public String getFullNullCount() {
        return "COUNT(*) - COUNT(" + getQuotedColumnName() + ")";
    }

    public abstract String getNDV();

    public abstract String getSampleDateSize(SampleInfo info);

    public abstract String getSampleNullCount(SampleInfo info);

}
