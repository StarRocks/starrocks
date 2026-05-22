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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/external/elasticsearch/EsShardPartitions.java

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

package com.starrocks.connector.opensearch;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class OpenSearchShardPartitions {

    private static final Logger LOG = LogManager.getLogger(OpenSearchShardPartitions.class);

    private final String indexName;
    // shardid -> host1, host2, host3
    private Map<Integer, List<OpenSearchShardRouting>> shardRoutings;
    private SingleRangePartitionDesc partitionDesc;
    private PartitionKey partitionKey;
    private long partitionId = -1;

    public OpenSearchShardPartitions(String indexName) {
        this.indexName = indexName;
        this.shardRoutings = Maps.newHashMap();
        this.partitionDesc = null;
        this.partitionKey = null;
    }

    /**
     * Parse shardRoutings from the json
     *
     * @param indexName    indexName(alias or really name)
     * @param searchShards the return value of _search_shards
     * @return shardRoutings is used for searching
     */
    public static OpenSearchShardPartitions findShardPartitions(String indexName, String searchShards)
            throws StarRocksConnectorException {

        OpenSearchShardPartitions partitions = new OpenSearchShardPartitions(indexName);
        JSONObject jsonObject = new JSONObject(searchShards);
        JSONArray shards = jsonObject.getJSONArray("shards");
        int length = shards.length();
        for (int i = 0; i < length; i++) {
            List<OpenSearchShardRouting> singleShardRouting = Lists.newArrayList();
            JSONArray shardsArray = shards.getJSONArray(i);
            int arrayLength = shardsArray.length();
            for (int j = 0; j < arrayLength; j++) {
                JSONObject indexShard = shardsArray.getJSONObject(j);
                String shardState = indexShard.getString("state");
                if ("STARTED".equalsIgnoreCase(shardState) || "RELOCATING".equalsIgnoreCase(shardState)) {
                    try {
                        singleShardRouting.add(
                                OpenSearchShardRouting.newSearchShard(
                                        indexShard.getString("index"),
                                        indexShard.getInt("shard"),
                                        indexShard.getBoolean("primary"),
                                        indexShard.getString("node"),
                                        jsonObject.getJSONObject("nodes")));
                    } catch (Exception e) {
                        LOG.error("fetch index [{}] shard partitions failure", indexName, e);
                        throw new StarRocksConnectorException(
                                "fetch [" + indexName + "] shard partitions failure [" + e.getMessage() + "]");
                    }
                }
            }
            if (singleShardRouting.isEmpty()) {
                LOG.warn("could not find a healthy allocation for [{}][{}]", indexName, i);
            }
            partitions.addShardRouting(i, singleShardRouting);
        }
        return partitions;
    }

    public void addHttpAddress(Map<String, OpenSearchNodeInfo> nodesInfo) {
        for (Map.Entry<Integer, List<OpenSearchShardRouting>> entry : shardRoutings.entrySet()) {
            List<OpenSearchShardRouting> shardRoutings = entry.getValue();
            for (OpenSearchShardRouting shardRouting : shardRoutings) {
                String nodeId = shardRouting.getNodeId();
                if (nodesInfo.containsKey(nodeId)) {
                    shardRouting.setHttpAddress(nodesInfo.get(nodeId).getPublishAddress());
                } else {
                    shardRouting.setHttpAddress(randomAddress(nodesInfo));
                }
            }
        }
    }

    public TNetworkAddress randomAddress(Map<String, OpenSearchNodeInfo> nodesInfo) {
        // return a random value between 0 and 32767 : [0, 32767)
        int seed = ThreadLocalRandom.current().nextInt(Short.MAX_VALUE) % nodesInfo.size();
        OpenSearchNodeInfo[] nodeInfos = nodesInfo.values().toArray(new OpenSearchNodeInfo[0]);
        return nodeInfos[seed].getPublishAddress();
    }

    public void addShardRouting(int shardId, List<OpenSearchShardRouting> singleShardRouting) {
        shardRoutings.put(shardId, singleShardRouting);
    }

    public String getIndexName() {
        return indexName;
    }

    public Map<Integer, List<OpenSearchShardRouting>> getShardRoutings() {
        return shardRoutings;
    }

    public SingleRangePartitionDesc getPartitionDesc() {
        return partitionDesc;
    }

    public void setPartitionDesc(SingleRangePartitionDesc partitionDesc) {
        this.partitionDesc = partitionDesc;
    }

    public PartitionKey getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(PartitionKey partitionKey) {
        this.partitionKey = partitionKey;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(long partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public String toString() {
        return "OpenSearchIndexState [indexName=" + indexName + ", partitionDesc=" + partitionDesc + ", partitionKey="
                + partitionKey + "]";
    }
}
