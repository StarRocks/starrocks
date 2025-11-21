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

import com.starrocks.common.Config;
import com.starrocks.statistic.sample.SampleInfo;
import com.starrocks.type.ArrayType;
import com.starrocks.type.MapType;
import com.starrocks.type.Type;

public class CollectionTypeColumnStats extends BaseColumnStats {
    private final boolean isManualJob;

    public CollectionTypeColumnStats(String columnName, Type columnType, boolean isManualJob) {
        super(columnName, columnType);
        this.isManualJob = isManualJob;
    }

    @Override
    public String getFullDataSize() {
        long elementTypeSize = columnType.isArrayType() ? ((ArrayType) columnType).getItemType().getTypeSize() :
                ((MapType) columnType).getKeyType().getTypeSize() + ((MapType) columnType).getValueType().getTypeSize();
        return "COUNT(*) * " + elementTypeSize + " * GREATEST(0, " + getCollectionSize().trim() + ") ";
    }

    @Override
    public String getSampleDateSize(SampleInfo info) {
        return columnType.getTypeSize() + " * " + info.getTotalRowCount();
    }

    @Override
    public String getSampleNullCount(SampleInfo info) {
        return "0";
    }

    @Override
    public String getFullNullCount() {
        return "0";
    }

    @Override
    public String getCollectionSize() {
        String collectionSizeFunction = columnType.isArrayType() ? "ARRAY_LENGTH" : "MAP_SIZE";
        return "IFNULL(AVG(" + collectionSizeFunction + "(" + getQuotedColumnName() + ")), -1) ";
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
    public String getNDV() {
        if (columnType.isArrayType() && enableCollectNDV()) {
            return "hex(hll_serialize(IFNULL(hll_raw(crc32_hash(" + getQuotedColumnName() + ")), hll_empty())))";
        } else {
            return "'00'";
        }
    }

    private boolean enableCollectNDV() {
        return isManualJob ? Config.enable_manual_collect_array_ndv : Config.enable_auto_collect_array_ndv;
    }
}
