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

import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.Type;

public class CollectionTypeColumnStats extends ColumnStats {
    private final long tableRowCount;

    public CollectionTypeColumnStats(String columnName, Type columnType, long tableRowCount) {
        super(columnName, columnType);
        this.tableRowCount = tableRowCount <= 0 ? 1 : tableRowCount;
    }

    @Override
    public String getQuotedColumnName() {
        return "`" + columnName + "`";
    }

    @Override
    public String getRowCount() {
        return tableRowCount + "";
    }

    @Override
    public String getDateSize() {
        long elementTypeSize = columnType.isArrayType() ? ((ArrayType) columnType).getItemType().getTypeSize() :
                ((MapType) columnType).getKeyType().getTypeSize() + ((MapType) columnType).getValueType().getTypeSize();
        return tableRowCount + " * " + elementTypeSize + " * " + getCollectionSize();
    }

    @Override
    public String getNullCount() {
        return "0";
    }

    @Override
    public String getMax() {
        return "''";
    }

    @Override
    public String getMin() {
        return "''";
    }

    @Override
    public String getDistinctCount(double rowSampleRatio) {
        return "0";
    }

    @Override
    public String getCollectionSize() {
        String collectionSizeFunction = columnType.isArrayType() ? "ARRAY_LENGTH" : "MAP_SIZE";
        return "AVG(" + collectionSizeFunction + "(" + getQuotedColumnName() + ")) ";
    }

}
