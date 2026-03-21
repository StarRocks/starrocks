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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/external/elasticsearch/EsRepository.java

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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.elasticsearch.EsMajorVersion;
import com.starrocks.connector.elasticsearch.EsMetaStateTracker;
import com.starrocks.connector.elasticsearch.EsNodeInfo;
import com.starrocks.connector.elasticsearch.EsShardPartitions;
import com.starrocks.connector.elasticsearch.EsShardRouting;
import com.starrocks.connector.elasticsearch.EsTablePartitions;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class OpenSearchMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(OpenSearchMetadata.class);

    private final OpenSearchRestClient restClient;
    private final Map<String, String> properties;
    private final String catalogName;
    public static final String DEFAULT_DB = "default_db";
    public static final long DEFAULT_DB_ID = 1L;

    public OpenSearchMetadata(OpenSearchRestClient restClient, Map<String, String> properties, String catalogName) {
        this.restClient = restClient;
        this.properties = properties;
        this.catalogName = catalogName;
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.OPENSEARCH;
    }

    @Override
    public Database getDb(ConnectContext context, String name) {
        if (DEFAULT_DB.equals(name)) {
            return new Database(DEFAULT_DB_ID, DEFAULT_DB);
        }
        return null;
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        return Arrays.asList(DEFAULT_DB);
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        return restClient.listTables();
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        return toOpenSearchTable(restClient, properties, tblName, dbName, catalogName);
    }

    public static EsTable toOpenSearchTable(OpenSearchRestClient restClient,
                                            Map<String, String> properties,
                                            String tableName, String dbName, String catalogName) {
        try {
            List<Column> columns = OpenSearchUtil.convertColumnSchema(restClient, tableName);
            properties.put(EsTable.KEY_INDEX, tableName);
            // OpenSearch 2.x has no mapping type, set to null
            properties.put(EsTable.KEY_TYPE, "");
            EsTable esTable = new EsTable(CONNECTOR_ID_GENERATOR.getNextId().asInt(),
                    catalogName, dbName, tableName, columns, properties, new SinglePartitionInfo());
            esTable.setComment("created by external opensearch catalog");

            // Phase 1: Use reflection to set EsTable fields for query support
            try {
                // Get shard partitions from OpenSearch
                OpenSearchShardPartitions shardPartitions = restClient.searchShards(tableName);
                Map<String, OpenSearchNodeInfo> nodesInfo = restClient.getHttpNodes();
                shardPartitions.addHttpAddress(nodesInfo);

                // Convert to EsShardPartitions
                EsShardPartitions esShardPartitions = convertToEsShardPartitions(shardPartitions);
                
                // Convert nodes to EsNodeInfo format and add HTTP addresses
                Map<String, EsNodeInfo> esNodesInfo = convertToEsNodeInfo(nodesInfo);
                esShardPartitions.addHttpAddress(esNodesInfo);

                // Create EsTablePartitions
                EsTablePartitions esTablePartitions = EsTablePartitions.fromShardPartitions(esTable, esShardPartitions);
                
                // Use reflection to set esTablePartitions field
                Field esTablePartitionsField = EsTable.class.getDeclaredField("esTablePartitions");
                esTablePartitionsField.setAccessible(true);
                esTablePartitionsField.set(esTable, esTablePartitions);

                // Set esMetaStateTracker field
                Field esMetaStateTrackerField = EsTable.class.getDeclaredField("esMetaStateTracker");
                esMetaStateTrackerField.setAccessible(true);
                esMetaStateTrackerField.set(esTable, new EsMetaStateTracker(null, esTable));

                // Set majorVersion field to V_7_X (OpenSearch 2.x is compatible with ES 7.x API)
                Field majorVersionField = EsTable.class.getDeclaredField("majorVersion");
                majorVersionField.setAccessible(true);
                majorVersionField.set(esTable, EsMajorVersion.V_7_X);

                // Set lastMetaDataSyncException to null (success)
                Field lastExceptionField = EsTable.class.getDeclaredField("lastMetaDataSyncException");
                lastExceptionField.setAccessible(true);
                lastExceptionField.set(esTable, null);

                LOG.info("Successfully set up OpenSearch table partitions for {} with {} shards", 
                        tableName, esShardPartitions.getShardRoutings().size());
            } catch (Exception e) {
                LOG.warn("Failed to set up shard partitions for {}, query may not work: {}", 
                        tableName, e.getMessage(), e);
                // Continue - table metadata will still be available
            }

            return esTable;
        } catch (NoSuchElementException e) {
            LOG.error(String.format("Unknown index {%s}", tableName), e);
            return null;
        } catch (Exception e) {
            LOG.error("transform to OpenSearch table Error", e);
            return null;
        }
    }
    
    /**
     * Converts OpenSearchShardPartitions to EsShardPartitions
     */
    private static EsShardPartitions convertToEsShardPartitions(OpenSearchShardPartitions osPartitions) {
        EsShardPartitions esPartitions = new EsShardPartitions(osPartitions.getIndexName());
        
        for (Map.Entry<Integer, List<OpenSearchShardRouting>> entry : osPartitions.getShardRoutings().entrySet()) {
            int shardId = entry.getKey();
            List<OpenSearchShardRouting> osRoutings = entry.getValue();
            List<EsShardRouting> esRoutings = Lists.newArrayList();
            
            for (OpenSearchShardRouting osRouting : osRoutings) {
                // Create EsShardRouting using constructor
                EsShardRouting esRouting = new EsShardRouting(
                        osRouting.getIndexName(),
                        osRouting.getShardId(),
                        osRouting.isPrimary(),
                        osRouting.getAddress(),  // This might be null for HTTP mode
                        osRouting.getNodeId()
                );
                
                // Copy HTTP address if available
                if (osRouting.getHttpAddress() != null) {
                    esRouting.setHttpAddress(osRouting.getHttpAddress());
                }
                
                esRoutings.add(esRouting);
            }
            
            esPartitions.addShardRouting(shardId, esRoutings);
        }
        
        return esPartitions;
    }
    
    /**
     * Converts OpenSearchNodeInfo map to EsNodeInfo map
     */
    private static java.util.Map<String, EsNodeInfo> convertToEsNodeInfo(
            java.util.Map<String, OpenSearchNodeInfo> osNodes) {
        java.util.Map<String, EsNodeInfo> esNodes = new java.util.HashMap<>();
        
        for (java.util.Map.Entry<String, OpenSearchNodeInfo> entry : osNodes.entrySet()) {
            OpenSearchNodeInfo osNode = entry.getValue();
            // Create EsNodeInfo with minimal info - only need publish address for HTTP mode
            EsNodeInfo esNode = createMinimalEsNodeInfo(osNode.getId(), osNode.getPublishAddress());
            if (esNode != null) {
                esNodes.put(entry.getKey(), esNode);
            }
        }
        
        return esNodes;
    }
    
    /**
     * Creates a minimal EsNodeInfo with just the publish address.
     * This is sufficient for HTTP mode.
     */
    private static EsNodeInfo createMinimalEsNodeInfo(String id, TNetworkAddress publishAddress) {
        try {
            // Create a fake node map with minimal required info
            java.util.Map<String, Object> nodeMap = new java.util.HashMap<>();
            nodeMap.put("name", id);
            nodeMap.put("host", publishAddress.getHostname());
            nodeMap.put("ip", publishAddress.getHostname());
            
            // Version - OpenSearch 2.x is compatible with ES 7.x
            nodeMap.put("version", "7.10.2");
            
            // Roles - assume it's a data node
            nodeMap.put("roles", java.util.Arrays.asList("data", "ingest"));
            
            // HTTP info
            java.util.Map<String, Object> httpMap = new java.util.HashMap<>();
            httpMap.put("publish_address", publishAddress.getHostname() + ":" + publishAddress.getPort());
            nodeMap.put("http", httpMap);
            
            // Attributes
            java.util.Map<String, Object> attrMap = new java.util.HashMap<>();
            nodeMap.put("attributes", attrMap);
            
            return new EsNodeInfo(id, nodeMap);
        } catch (Exception e) {
            LOG.warn("Failed to create EsNodeInfo for node {}: {}", id, e.getMessage());
            return null;
        }
    }
}
