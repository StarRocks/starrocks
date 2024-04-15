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
package com.starrocks.connector.delta.cost;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.Config;
import com.starrocks.connector.delta.ScalarOperatorToDeltaExpression;
import com.starrocks.connector.delta.StarRocksDeltaLakeException;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import io.delta.standalone.DeltaScan;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.expressions.Expression;
import io.delta.standalone.internal.util.DeltaJsonUtil;
import io.delta.standalone.types.StructType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DeltaLakeStatisticProvider {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeStatisticProvider.class);

    public DeltaLakeStatisticProvider() {
    }


    public Statistics getTableStatistics(OptimizerContext session,
                                         DeltaLakeTable table,
                                         List<ColumnRefOperator> columns,
                                         List<PartitionKey> partitionKeys) {
        Statistics.Builder statisticsBuilder = Statistics.builder();

        return statisticsBuilder.build();

    }


    public Statistics getTableStatistics(
            DeltaLakeTable table,
            List<ColumnRefOperator> columns,
            ScalarOperator predicates) {
        Statistics.Builder statisticsBuilder = Statistics.builder();

        Snapshot snapshot = table.getDeltaLog().snapshot();
        if (snapshot == null) {
            return statisticsBuilder.build();
        }

        DeltaLakeFileStats deltaLakeFileStats = new DeltaLakeFileStats();
        Metadata metadata = snapshot.getMetadata();
        StructType tableSchema = metadata.getSchema();

        ScalarOperatorToDeltaExpression convertor = new ScalarOperatorToDeltaExpression();
        Expression expression = convertor.convert(predicates, new ScalarOperatorToDeltaExpression.DeltaContext(tableSchema));
        DeltaScan scan = snapshot.scan(expression);
        CloseableIterator<AddFile> files = scan.getFiles();
        AddFile addFile;
        List<String> partitionColumns = metadata.getPartitionColumns();
        Set<String> partitionValues = new HashSet<>();
        Set<String> firstPartitionValues = new HashSet<>();
        while (files.hasNext()) {
            try {
                addFile = files.next();

                Map<String, String> addFilePartitionValues = addFile.getPartitionValues();
                if (addFilePartitionValues.size() > 0) {
                    partitionValues.add(addFilePartitionValues.toString());
                    firstPartitionValues.add(addFilePartitionValues.get(partitionColumns.get(0)));
                }
                long size = addFile.getSize();
                String stats = addFile.getStats();
                long numRecords = DeltaJsonUtil.mapper.readTree(stats).get("numRecords").asLong();
                deltaLakeFileStats.incrementFileCount(1);
                deltaLakeFileStats.incrementSize(size);
                deltaLakeFileStats.incrementRecordCount(numRecords);
            } catch (JsonProcessingException e) {
                LOG.error("error {}", e.getMessage());
            }

        }

        long maxSize = Config.delta_single_query_size_limit_gb * 1024 * 1024 * 1024L;

        //The delta lake table byte limit is read in a single query
        LOG.info("single query read byte size limit:{}," +
                        "deltaLakeFileStats: filecount:{},size:{},recordCount:{}",
                maxSize,
                deltaLakeFileStats.getFileCount(),
                deltaLakeFileStats.getSize(),
                deltaLakeFileStats.getRecordCount());
        if (maxSize > 0) {
            if (deltaLakeFileStats.getSize() > maxSize) {
                String message;
                if (partitionColumns.size() > 0) {
                    long avgPartitionSize = deltaLakeFileStats.getSize() / partitionValues.size();
                    // throw new StarRocksPlannerException("");
                    long recommendMaxPartitionNum = maxSize / avgPartitionSize;
                    long recommendMaxFirstPartitionNum = maxSize /
                            (deltaLakeFileStats.getSize() / firstPartitionValues.size());
                    message = String.format("delta_max_limit_size:%d,query_partition_num_total:%d,query_file_size_total:%d," +
                                    "query_record_counts:%d,partition_field:%s," +
                                    "recommend_first_partition_max_num:%d,recommend_partition_max_num:%d",
                            maxSize,
                            partitionValues.size(), deltaLakeFileStats.getSize(),
                            deltaLakeFileStats.getRecordCount(),
                            metadata.getPartitionColumns(),
                            recommendMaxFirstPartitionNum,
                            recommendMaxPartitionNum);
                } else {
                    message = String.format("delta_max_limit_size:%d,query_file_size_total:%d," +
                                    "query_record_counts:%d,Query is not recommended for the non-partition table",
                            maxSize,
                            deltaLakeFileStats.getSize(),
                            deltaLakeFileStats.getRecordCount()
                    );
                }
                throw new StarRocksDeltaLakeException(message);
            }
        }
        statisticsBuilder.addColumnStatistics(buildColumnStatistics(snapshot.getMetadata(), columns));
        statisticsBuilder.setOutputRowCount(deltaLakeFileStats.getRecordCount());

        return statisticsBuilder.build();

    }

    private Map<ColumnRefOperator, ColumnStatistic> buildColumnStatistics(Metadata metadata, List<ColumnRefOperator> columns) {
        Map<ColumnRefOperator, ColumnStatistic> statisticMap = new HashMap<>();

        Set<String> fieldNames = Arrays.stream(metadata
                        .getSchema().getFieldNames())
                .map(String::toLowerCase)
                .collect(Collectors.toSet());

        columns.stream().filter(column -> fieldNames.contains(column.getName().toLowerCase()))
                .forEach(columnRefOperator -> {
                    statisticMap.put(columnRefOperator, ColumnStatistic.unknown());
                });

        return statisticMap;
    }


}

