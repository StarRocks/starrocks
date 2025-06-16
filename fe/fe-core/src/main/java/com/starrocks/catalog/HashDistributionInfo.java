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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/HashDistributionInfo.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Hash Distribution Info.
 */
public class HashDistributionInfo extends DistributionInfo implements GsonPostProcessable {

    @SerializedName("colIds")
    private List<ColumnId> distributionColumnIds;

    @SerializedName(value = "distributionColumns")
    @Deprecated // Use distributionColumnIds to get columns, this is reserved for rollback compatibility only.
    private List<Column> deprecatedColumns;

    @SerializedName(value = "bucketNum")
    private int bucketNum;

    private static final Logger LOG = LogManager.getLogger(OlapScanNode.class);

    public HashDistributionInfo() {
        super();
        this.deprecatedColumns = new ArrayList<>();
        this.distributionColumnIds = new ArrayList<>();
    }

    public HashDistributionInfo(int bucketNum, List<Column> distributionColumns) {
        super(DistributionInfoType.HASH);
        this.deprecatedColumns = requireNonNull(distributionColumns, "distributionColumns is null");
        this.distributionColumnIds = distributionColumns.stream()
                .map(Column::getColumnId)
                .collect(Collectors.toList());
        this.bucketNum = bucketNum;
    }

    @Override
    public boolean supportColocate() {
        return true;
    }

    @Override
    public List<ColumnId> getDistributionColumns() {
        return distributionColumnIds;
    }

    @Override
    public int getBucketNum() {
        return bucketNum;
    }

    @Override
    public String getDistributionKey(Map<ColumnId, Column> schema) {
        List<String> colNames = Lists.newArrayList();
        for (Column column : MetaUtils.getColumnsByColumnIds(schema, distributionColumnIds)) {
            colNames.add("`" + column.getName() + "`");
        }
        String colList = Joiner.on(", ").join(colNames);
        return colList;
    }

    public void setDistributionColumns(List<Column> columns) {
        this.deprecatedColumns = columns;
        this.distributionColumnIds = columns.stream()
                .map(column -> ColumnId.create(column.getName()))
                .collect(Collectors.toList());
    }

    @Override
    public void setBucketNum(int bucketNum) {
        this.bucketNum = bucketNum;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(type, bucketNum, distributionColumnIds);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof HashDistributionInfo)) {
            return false;
        }

        HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) other;

        return type == hashDistributionInfo.type
                && bucketNum == hashDistributionInfo.bucketNum
                && distributionColumnIds.equals(hashDistributionInfo.distributionColumnIds);
    }

    @Override
    public DistributionDesc toDistributionDesc(Map<ColumnId, Column> schema) {
        List<String> distriColNames = Lists.newArrayList();
        for (Column col : MetaUtils.getColumnsByColumnIds(schema, distributionColumnIds)) {
            distriColNames.add(col.getName());
        }
        DistributionDesc distributionDesc = new HashDistributionDesc(bucketNum, distriColNames);
        return distributionDesc;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (distributionColumnIds == null || distributionColumnIds.size() <= 0) {
            distributionColumnIds = deprecatedColumns.stream().map(Column::getColumnId).collect(Collectors.toList());
        }
    }

    @Override
    public HashDistributionInfo copy() {
        return new HashDistributionInfo(bucketNum, deprecatedColumns);
    }

    @Override
    public String toSql(Map<ColumnId, Column> schema) {
        StringBuilder builder = new StringBuilder();
        builder.append("DISTRIBUTED BY HASH(");

        List<String> colNames = Lists.newArrayList();
        for (Column column : MetaUtils.getColumnsByColumnIds(schema, distributionColumnIds)) {
            colNames.add("`" + column.getName() + "`");
        }
        String colList = Joiner.on(", ").join(colNames);
        builder.append(colList);
        builder.append(")");
        if (bucketNum > 0) {
            builder.append(" BUCKETS ").append(bucketNum).append(" ");
        }
        return builder.toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("type: ").append(type).append("; ");

        builder.append("distribution columns: [");
        for (ColumnId name : distributionColumnIds) {
            builder.append(name.getId()).append(",");
        }
        builder.append("]; ");

        if (bucketNum > 0) {
            builder.append("bucket num: ").append(bucketNum).append("; ");
        }

        return builder.toString();
    }
}
