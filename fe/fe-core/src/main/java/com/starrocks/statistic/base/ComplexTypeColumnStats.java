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

public class ComplexTypeColumnStats extends BaseColumnStats {

    public ComplexTypeColumnStats(String columnName, Type columnType) {
        super(columnName, columnType);
    }

    @Override
    public String getFullDataSize() {
        return "COUNT(*) * " + columnType.getTypeSize();
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
    public String getMax() {
        return "''";
    }

    @Override
    public String getMin() {
        return "''";
    }

    @Override
    public String getFullNullCount() {
        return "0";
    }

    @Override
    public String getNDV() {
        return "00";
    }
}
