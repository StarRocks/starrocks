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

package com.starrocks.connector.iceberg.cost;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.ConnectorTableColumnStat;
import com.starrocks.connector.iceberg.CachingIcebergCatalog;
import com.starrocks.connector.iceberg.IcebergCatalog;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class IcebergAnalyzedStatisticProvider implements IcebergStatisticProvider {
    private IcebergCatalog icebergcatalog;

    public IcebergAnalyzedStatisticProvider(IcebergCatalog icebergCatalog) {
        this.icebergcatalog = icebergCatalog;
    }

    public Statistics getTableStatistics(IcebergTable icebergTable, ScalarOperator predicate,
                                         Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        Optional<Statistics> srStatistics = getSRTableStatistics(icebergTable, colRefToColumnMetaMap);
        if (srStatistics.isPresent()) {
            return srStatistics.get();
        } else {
            IcebergStatisticProvider provider = new IcebergFileBasedStatisticProvider();
            return provider.getTableStatistics(icebergTable, predicate, colRefToColumnMetaMap);
        }
    }

    private Optional<Statistics> getSRTableStatistics(IcebergTable icebergTable,
                                            Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        Map<String, ColumnRefOperator> nameToColRef = colRefToColumnMetaMap.entrySet().stream().
                collect(Collectors.toMap(entry -> entry.getValue().getName(), Map.Entry::getKey));
        List<String> columnNames = Lists.newArrayList(nameToColRef.keySet());
        List<Optional<ConnectorTableColumnStat>> columnStatistics = ((CachingIcebergCatalog) icebergcatalog).
                getColumnStatistics(icebergTable.getUUID(), columnNames);
        if (columnStatistics.stream().anyMatch(Optional::isPresent)) {
            Statistics.Builder statisticsBuilder = Statistics.builder();
            for (int i = 0; i < columnNames.size(); i++) {
                if (columnStatistics.get(i).isPresent()) {
                    statisticsBuilder.addColumnStatistic(nameToColRef.get(columnNames.get(i)),
                            columnStatistics.get(i).get().getColumnStatistic());
                    statisticsBuilder.setOutputRowCount(columnStatistics.get(i).get().getRowCount());
                } else {
                    statisticsBuilder.addColumnStatistic(nameToColRef.get(columnNames.get(i)), ColumnStatistic.unknown());
                }

            }
            return Optional.of(statisticsBuilder.build());
        } else {
            return Optional.empty();
        }
    }



}
