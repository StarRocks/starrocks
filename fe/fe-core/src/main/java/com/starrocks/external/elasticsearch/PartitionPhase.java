// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/external/elasticsearch/PartitionPhase.java

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

package com.starrocks.external.elasticsearch;

import com.starrocks.catalog.EsTable;

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
    public void execute(SearchContext context) throws StarRocksESException {
        shardPartitions = client.searchShards(context.sourceIndex());
        nodesInfo = client.getHttpNodes();
    }

    @Override
    public void postProcess(SearchContext context) throws StarRocksESException {
        context.partitions(shardPartitions);
        if (EsTable.TRANSPORT_HTTP.equals(context.esTable().getTransport())) {
            context.partitions().addHttpAddress(nodesInfo);
        }
    }
}
