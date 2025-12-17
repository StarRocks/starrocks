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

package com.starrocks.sql.automv.qe;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.automv.util.ColumnDescription;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.starrocks.sql.automv.qe.ColumnPlus.BIGINT;
import static com.starrocks.sql.automv.qe.ColumnPlus.DATETIME;
import static com.starrocks.sql.automv.qe.ColumnPlus.JSON;
import static com.starrocks.sql.automv.qe.ColumnPlus.VARCHAR;

public class ShowRecommendationResult {
    public static final List<ColumnPlus> COLUMNS = collectColumns();
    @ColumnDescription(type = BIGINT, autoIncrement = true, isBucketColumn = true)
    private long id;
    @ColumnDescription(type = DATETIME, isPartitionColumn = true)
    private Timestamp ts;
    @ColumnDescription(type = VARCHAR, len = 255)
    private String taskName;
    @ColumnDescription(type = JSON)
    private List<String> result;

    private ShowRecommendationResult(Timestamp ts, String taskName, List<String> result) {
        this.ts = ts;
        this.taskName = taskName;
        this.result = result;
    }

    public ShowRecommendationResult() {
    }

    public static ShowRecommendationResult of(String taskName, List<String> result) {
        return new ShowRecommendationResult(new Timestamp(System.currentTimeMillis()), taskName, result);
    }

    private static List<ColumnPlus> collectColumns() {
        return Stream.of(ShowRecommendationResult.class.getDeclaredFields())
                .filter(ColumnPlus::isAcceptable)
                .map(ColumnPlus::fieldToColumn)
                .collect(ImmutableList.toImmutableList());
    }

    public static List<ColumnPlus> getColumns() {
        return COLUMNS;
    }

    public static TablePlus getTable(String fqTableName, int numBucket, int replicationNum) {
        List<Column> columns =
                getColumns().stream().map(ColumnPlus::getColumn).collect(ImmutableList.toImmutableList());

        List<Column> partitionKey = getColumns().stream().filter(ColumnPlus::isPartitionColumn)
                .map(ColumnPlus::getColumn)
                .collect(Collectors.toList());

        PartitionInfo partitionInfo;
        if (partitionKey.size() == 1 && partitionKey.get(0).getType().isDatetime()) {
            partitionInfo = new RangePartitionInfo(partitionKey);
        } else {
            partitionInfo = new SinglePartitionInfo();
        }

        DistributionInfo distributionInfo;
        List<Column> bucketKey = getColumns()
                .stream()
                .filter(ColumnPlus::isBucketColumn)
                .map(ColumnPlus::getColumn)
                .collect(ImmutableList.toImmutableList());

        if (bucketKey.isEmpty()) {
            distributionInfo = new RandomDistributionInfo(numBucket);
        } else {
            distributionInfo = new HashDistributionInfo(numBucket, bucketKey);
        }

        OlapTable table = new OlapTable(0xdeadbeef, fqTableName, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE, "true");
        properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX, "true");
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "" + replicationNum);

        if (partitionInfo instanceof RangePartitionInfo) {
            properties.put(DynamicPartitionProperty.TIME_UNIT, "DAY");
            properties.put(DynamicPartitionProperty.START, "-30");
            properties.put(DynamicPartitionProperty.END, "3");
            properties.put(DynamicPartitionProperty.ENABLE, "true");
            properties.put(DynamicPartitionProperty.BUCKETS, "" + numBucket);
            properties.put(DynamicPartitionProperty.PREFIX, "p");
        }

        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildReplicatedStorage();
        tableProperty.buildEnablePersistentIndex();
        tableProperty.buildReplicationNum();
        tableProperty.buildDynamicProperty();
        table.setTableProperty(tableProperty);
        return TablePlus.of(table, ShowRecommendationResult.class, getColumns());
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public List<String> getResult() {
        return result;
    }

    public void setResult(List<String> result) {
        this.result = result;
    }

    public void setResult(String result) {
        this.result = new Gson().<List<String>>fromJson(result, List.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShowRecommendationResult result1 = (ShowRecommendationResult) o;
        return id == result1.id && Objects.equals(ts, result1.ts) &&
                Objects.equals(taskName, result1.taskName) && Objects.equals(result, result1.result);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, ts, taskName, result);
    }
}
