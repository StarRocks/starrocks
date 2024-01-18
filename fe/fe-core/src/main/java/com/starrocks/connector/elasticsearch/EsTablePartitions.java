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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/external/elasticsearch/EsTablePartitions.java

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

package com.starrocks.connector.elasticsearch;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.exception.DdlException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * save the dynamic info parsed from es cluster state such as shard routing, partition info
 */
public class EsTablePartitions {

    private static final Logger LOG = LogManager.getLogger(EsTablePartitions.class);

    private PartitionInfo partitionInfo;
    private Map<Long, String> partitionIdToIndices;
    private Map<String, EsShardPartitions> partitionedIndexStates;
    private Map<String, EsShardPartitions> unPartitionedIndexStates;

    public EsTablePartitions() {
        partitionInfo = null;
        partitionIdToIndices = Maps.newHashMap();
        partitionedIndexStates = Maps.newHashMap();
        unPartitionedIndexStates = Maps.newHashMap();
    }

    public static EsTablePartitions fromShardPartitions(EsTable esTable, EsShardPartitions shardPartitions)
            throws StarRocksConnectorException, DdlException {
        EsTablePartitions esTablePartitions = new EsTablePartitions();
        RangePartitionInfo partitionInfo = null;
        if (esTable.getPartitionInfo() != null) {
            if (esTable.getPartitionInfo() instanceof RangePartitionInfo) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) esTable.getPartitionInfo();
                partitionInfo = new RangePartitionInfo(rangePartitionInfo.getPartitionColumns());
                esTablePartitions.setPartitionInfo(partitionInfo);
                if (LOG.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder();
                    int idx = 0;
                    for (Column column : rangePartitionInfo.getPartitionColumns()) {
                        if (idx != 0) {
                            sb.append(", ");
                        }
                        sb.append("`").append(column.getName()).append("`");
                        idx++;
                    }
                    sb.append(")");
                    LOG.debug("begin to parse es table [{}] state from search shards,"
                            + " with partition info [{}]", esTable.getName(), sb.toString());
                }
            } else if (esTable.getPartitionInfo() instanceof SinglePartitionInfo) {
                LOG.debug("begin to parse es table [{}] state from search shards, "
                        + "with no partition info", esTable.getName());
            } else {
                throw new StarRocksConnectorException("es table only support range partition, "
                        + "but current partition type is "
                        + esTable.getPartitionInfo().getType());
            }
        }
        esTablePartitions.addIndexState(esTable.getIndexName(), shardPartitions);
        LOG.debug("add index {} to es table {}", shardPartitions, esTable.getName());
        if (partitionInfo != null) {
            // sort the index state according to partition key and then add to range map
            List<EsShardPartitions> esShardPartitionsList = new ArrayList<>(
                    esTablePartitions.getPartitionedIndexStates().values());
            esShardPartitionsList.sort(Comparator.comparing(EsShardPartitions::getPartitionKey));
            long partitionId = 0;
            for (EsShardPartitions esShardPartitions : esShardPartitionsList) {
                Range<PartitionKey> range = partitionInfo.handleNewSinglePartitionDesc(
                        esShardPartitions.getPartitionDesc(), partitionId, false);
                esTablePartitions.addPartition(esShardPartitions.getIndexName(), partitionId);
                esShardPartitions.setPartitionId(partitionId);
                ++partitionId;
                LOG.debug("add parition to es table [{}] with range [{}]", esTable.getName(),
                        range);
            }
        }
        return esTablePartitions;
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public void setPartitionInfo(PartitionInfo partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

    public Map<Long, String> getPartitionIdToIndices() {
        return partitionIdToIndices;
    }

    public void addPartition(String indexName, long partitionId) {
        partitionIdToIndices.put(partitionId, indexName);
    }

    public void addIndexState(String indexName, EsShardPartitions indexState) {
        if (indexState.getPartitionDesc() != null) {
            partitionedIndexStates.put(indexName, indexState);
        } else {
            unPartitionedIndexStates.put(indexName, indexState);
        }
    }

    public Map<String, EsShardPartitions> getPartitionedIndexStates() {
        return partitionedIndexStates;
    }

    public Map<String, EsShardPartitions> getUnPartitionedIndexStates() {
        return unPartitionedIndexStates;
    }

    public EsShardPartitions getEsShardPartitions(long partitionId) {
        if (partitionIdToIndices.containsKey(partitionId)) {
            return partitionedIndexStates.get(partitionIdToIndices.get(partitionId));
        }
        return null;
    }

    public EsShardPartitions getEsShardPartitions(String indexName) {
        if (partitionedIndexStates.containsKey(indexName)) {
            return partitionedIndexStates.get(indexName);
        }
        return unPartitionedIndexStates.get(indexName);
    }
}
