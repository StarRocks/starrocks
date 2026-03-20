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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/ColocateGroupSchema.java

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

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.io.Writable;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * This class saves the schema of a colocation group
 */
public class ColocateGroupSchema implements Writable {
    @SerializedName("gi")
    private GroupId groupId;
    @SerializedName("ct")
    private List<Type> distributionColTypes = Lists.newArrayList();
    @SerializedName("bn")
    private int bucketsNum;
    @SerializedName("rn")
    private short replicationNum;
    @SerializedName("dt")
    private DistributionInfoType distributionType = DistributionInfoType.HASH;

    private ColocateGroupSchema() {

    }

    public ColocateGroupSchema(GroupId groupId, List<Column> distributionCols, int bucketsNum,
                                short replicationNum, DistributionInfoType distributionType) {
        this.groupId = groupId;
        this.distributionColTypes = distributionCols.stream().map(c -> c.getType()).collect(Collectors.toList());
        this.bucketsNum = bucketsNum;
        this.replicationNum = replicationNum;
        this.distributionType = distributionType;
    }

    public GroupId getGroupId() {
        return groupId;
    }

    public int getBucketsNum() {
        return bucketsNum;
    }

    public short getReplicationNum() {
        return replicationNum;
    }

    public List<Type> getDistributionColTypes() {
        return distributionColTypes;
    }

    public DistributionInfoType getDistributionType() {
        return distributionType;
    }

    public boolean isRangeColocate() {
        return distributionType == DistributionInfoType.RANGE;
    }

    /**
     * Returns the number of colocate columns (sort key prefix length) for range colocate groups,
     * or the number of hash distribution columns for hash colocate groups.
     */
    public int getColocateColumnCount() {
        return distributionColTypes.size();
    }

    public void checkColocateSchema(OlapTable tbl) throws DdlException {
        checkDistribution(tbl, tbl.getDefaultDistributionInfo());
        checkReplicationNum(tbl.getPartitionInfo());
    }

    public void checkDistribution(OlapTable table, DistributionInfo distributionInfo)
            throws DdlException {
        // check distribution type consistency
        if (distributionInfo.getType() != distributionType) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_HAS_SAME_DISTRIBUTION_TYPE,
                    groupId.toString(), distributionType.name(), distributionInfo.getType().name());
        }

        if (distributionInfo instanceof HashDistributionInfo) {
            HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
            // buckets num
            if (info.getBucketNum() != bucketsNum) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_HAS_SAME_BUCKET_NUM, bucketsNum,
                        groupId.toString(), info.toString());
            }
            // distribution col size
            if (info.getDistributionColumns().size() != distributionColTypes.size()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_HAS_SAME_DISTRIBUTION_COLUMN_SIZE,
                        distributionColTypes.size(), groupId.toString(), info.toString());
            }
            // distribution col type
            Map<ColumnId, Column> idToColumn = table.getIdToColumn();
            List<Column> distributionColumns = MetaUtils.getColumnsByColumnIds(
                    idToColumn, distributionInfo.getDistributionColumns());
            for (int i = 0; i < distributionColTypes.size(); i++) {
                Type targetColType = distributionColTypes.get(i);
                if (!targetColType.equals(distributionColumns.get(i).getType())) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_HAS_SAME_DISTRIBUTION_COLUMN_TYPE,
                            groupId.toString(), distributionColumns.get(i).getName(), targetColType, info.toString());
                }
            }
        } else if (distributionInfo instanceof RangeDistributionInfo) {
            // For range distribution colocate, the colocate columns are the sort key prefix.
            List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(table);
            // colocate column count
            if (sortKeyColumns.size() < distributionColTypes.size()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_HAS_SAME_DISTRIBUTION_COLUMN_SIZE,
                        distributionColTypes.size(), groupId.toString(),
                        "sort key columns: " + sortKeyColumns.size());
            }
            // colocate column type
            for (int i = 0; i < distributionColTypes.size(); i++) {
                Type expectedType = distributionColTypes.get(i);
                if (!expectedType.equals(sortKeyColumns.get(i).getType())) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_HAS_SAME_DISTRIBUTION_COLUMN_TYPE,
                            groupId.toString(), sortKeyColumns.get(i).getName(),
                            expectedType, "sort key prefix");
                }
            }
        }
    }

    public void checkReplicationNum(PartitionInfo partitionInfo) throws DdlException {
        for (Short repNum : partitionInfo.idToReplicationNum.values()) {
            if (repNum != replicationNum) {
                ErrorReport
                        .reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_HAS_SAME_REPLICATION_NUM,
                                replicationNum, groupId.toString(), partitionInfo.toString());
            }
        }
    }

    public void checkReplicationNum(short repNum) throws DdlException {
        if (repNum != replicationNum) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_HAS_SAME_REPLICATION_NUM,
                    replicationNum, groupId.toString(), String.valueOf(repNum));
        }
    }



}
