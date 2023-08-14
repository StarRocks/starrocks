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

import com.starrocks.catalog.EsTable;
import com.starrocks.connector.exception.StarRocksConnectorException;

import java.util.HashMap;
import java.util.Map;

/**
 * Fetch resolved indices's search shards from remote ES Cluster
 */
public class PartitionPhase implements SearchPhase {

    private EsRestClient client;
    private EsShardPartitions shardPartitions;
    private Map<String, EsNodeInfo> nodesInfo;

    public PartitionPhase(EsRestClient client) {
        this.client = client;
    }

    @Override
    public void execute(SearchContext context) throws StarRocksConnectorException {
        shardPartitions = client.searchShards(context.sourceIndex());
        nodesInfo = client.getHttpNodes();
        if (!context.wanOnly()) {
            nodesInfo = client.getHttpNodes();
        } else {
            nodesInfo = new HashMap<>();
            String[] seeds = context.esTable().getSeeds();
            for (int i = 0; i < seeds.length; i++) {
                nodesInfo.put(String.valueOf(i), new EsNodeInfo(String.valueOf(i), seeds[i]));
            }
        }
    }

    @Override
    public void postProcess(SearchContext context) throws StarRocksConnectorException {
        context.partitions(shardPartitions);
        if (EsTable.KEY_TRANSPORT_HTTP.equals(context.esTable().getTransport())) {
            context.partitions().addHttpAddress(nodesInfo);
        }
    }
}
