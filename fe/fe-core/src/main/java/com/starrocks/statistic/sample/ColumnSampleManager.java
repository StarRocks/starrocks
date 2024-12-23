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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;

import java.util.List;

public class ColumnSampleManager {

    private final List<ColumnStats> primitiveTypeStats = Lists.newArrayList();

    private final List<ColumnStats> complexTypeStats = Lists.newArrayList();

    private ColumnSampleManager() {

    }

    public static ColumnSampleManager init(List<String> columnNames, List<Type> columnTypes, Table table,
                                           SampleInfo sampleInfo) {
        ColumnSampleManager columnSampleManager = new ColumnSampleManager();
        columnSampleManager.classifyColumnStats(columnNames, columnTypes, table, sampleInfo);
        return columnSampleManager;
    }

    private void classifyColumnStats(List<String> columnNames, List<Type> columnTypes, Table table,
                                     SampleInfo sampleInfo) {
        boolean onlyOneDistributionCol = table.getDistributionColumnNames().size() == 1;
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Type columnType = columnTypes.get(i);

            if (table.getColumn(columnName) != null) {
                if (columnType.canStatistic() && !columnType.isCollectionType()) {
                    if (onlyOneDistributionCol && table.getDistributionColumnNames().contains(columnName)) {
                        primitiveTypeStats.add(new DistributionColumnStats(columnName, columnType, sampleInfo));
                        onlyOneDistributionCol = false;
                    } else {
                        primitiveTypeStats.add(new PrimitiveTypeColumnStats(columnName, columnType));
                    }
                } else {
                    complexTypeStats.add(new ComplexTypeColumnStats(columnName, columnType));
                }
            } else {
                int start = 0;
                int end;
                List<String> names = Lists.newArrayList();
                while ((end = columnName.indexOf(".", start)) > 0) {
                    start = end + 1;
                    String name = columnName.substring(0, end);
                    Column c = table.getColumn(name);
                    if (c != null && c.getType().isStructType()) {
                        names.add(name);
                        columnName = columnName.substring(end + 1);
                        Type type = c.getType();
                        if (!columnName.contains(".")) {
                            names.add(columnName);
                        } else {
                            int subStart = 0;
                            int pos = 0;
                            int subEnd;
                            while ((subEnd = columnName.indexOf(".", pos)) > 0 && type.isStructType()) {
                                String subName = columnName.substring(subStart, subEnd);
                                if (((StructType) type).containsField(subName)) {
                                    names.add(subName);
                                    type = ((StructType) type).getField(subName).getType();
                                    subStart = subEnd + 1;
                                }
                                pos = subEnd + 1;
                            }
                            names.add(columnName.substring(subStart));
                        }
                        break;
                    }
                }
                if (!names.isEmpty()) {
                    if (columnType.canStatistic() && !columnType.isCollectionType()) {
                        primitiveTypeStats.add(new SubFieldColumnStats(names, columnType));
                    } else {
                        complexTypeStats.add(new SubFieldColumnStats(names, columnType));
                    }
                }
            }
        }
    }


    public List<ColumnStats> getComplexTypeStats() {
        return complexTypeStats;
    }

    public List<List<ColumnStats>> splitPrimitiveTypeStats() {
        int dop = 2;
        if (ConnectContext.get() != null) {
            dop = Math.max(dop, ConnectContext.get().getSessionVariable().getStatisticCollectParallelism());
        }
        return Lists.partition(primitiveTypeStats, dop);
    }
}
