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

import java.util.List;
import java.util.stream.Collectors;

public class SubFieldColumnStats extends PrimitiveTypeColumnStats {

    private List<String> names;
    public ColumnStats complexStats;
    private boolean isComplexType = false;

    public SubFieldColumnStats(List<String> names, Type columnType) {
        super(String.join(".", names), columnType);
        this.names = names;
        this.isComplexType = !columnType.canStatistic() || columnType.isCollectionType();
        if (!columnType.canStatistic()) {
            complexStats = new ComplexTypeColumnStats("name", columnType);
        } else if (columnType.isCollectionType()) {
            complexStats = new CollectionTypeColumnStats("name", columnType);
        }
    }

    @Override
    public boolean supportMeta() {
        return false;
    }

    @Override
    public String getColumnNameStr() {
        return StringEscapeUtils.escapeSql(String.join(".", names));
    }

    @Override
    public String getQuotedColumnName() {
        return names.stream().map(e -> "`" + e + "`").collect(Collectors.joining("."));
    }

    @Override
    public String getMax() {
        return isComplexType ? complexStats.getMax() : super.getMax();
    }

    @Override
    public String getMin() {
        return isComplexType ? complexStats.getMin() : super.getMin();
    }

    @Override
    public String getFullDataSize() {
        return isComplexType ? complexStats.getFullDataSize() : super.getFullDataSize();
    }

    @Override
    public String getFullNullCount() {
        return isComplexType ? complexStats.getFullNullCount() : super.getFullNullCount();
    }

    @Override
    public String getNDV() {
        return isComplexType ? complexStats.getNDV() : super.getNDV();
    }

    @Override
    public String getSampleDateSize(SampleInfo info) {
        return isComplexType ? complexStats.getSampleDateSize(info) : super.getSampleDateSize(info);
    }

    @Override
    public String getSampleNullCount(SampleInfo info) {
        return isComplexType ? complexStats.getSampleNullCount(info) : super.getSampleNullCount(info);
    }
}
