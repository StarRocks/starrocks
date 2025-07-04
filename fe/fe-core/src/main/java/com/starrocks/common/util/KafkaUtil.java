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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/util/KafkaUtil.java

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

package com.starrocks.common.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.LoadException;
import com.starrocks.common.StarRocksException;
import com.starrocks.proto.PKafkaLoadInfo;
import com.starrocks.proto.PKafkaMetaProxyRequest;
import com.starrocks.proto.PKafkaOffsetBatchProxyRequest;
import com.starrocks.proto.PKafkaOffsetProxyRequest;
import com.starrocks.proto.PKafkaOffsetProxyResult;
import com.starrocks.proto.PProxyRequest;
import com.starrocks.proto.PProxyResult;
import com.starrocks.proto.PStringPair;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.ComputeResourceProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class KafkaUtil {
    private static final Logger LOG = LogManager.getLogger(KafkaUtil.class);

    private static final ProxyAPI PROXY_API = new ProxyAPI();

    public static List<Integer> getAllKafkaPartitions(String brokerList, String topic,
                                                      ImmutableMap<String, String> properties,
                                                      ComputeResource computeResource) throws StarRocksException {
        return PROXY_API.getAllKafkaPartitions(brokerList, topic, properties, computeResource);
    }

    // latest offset is (the latest existing message offset + 1)
    public static Map<Integer, Long> getLatestOffsets(String brokerList, String topic,
                                                      ImmutableMap<String, String> properties,
                                                      List<Integer> partitions,
                                                      ComputeResource computeResource) throws StarRocksException {
        return PROXY_API.getLatestOffsets(brokerList, topic, properties, partitions, computeResource);
    }

    public static Map<Integer, Long> getBeginningOffsets(String brokerList, String topic,
                                                         ImmutableMap<String, String> properties,
                                                         List<Integer> partitions,
                                                         ComputeResource computeResource) throws StarRocksException {
        return PROXY_API.getBeginningOffsets(brokerList, topic, properties, partitions, computeResource);
    }

    public static List<PKafkaOffsetProxyResult> getBatchOffsets(List<PKafkaOffsetProxyRequest> requests)
            throws StarRocksException {
        return PROXY_API.getBatchOffsets(requests);
    }

    public static PKafkaLoadInfo genPKafkaLoadInfo(String brokerList, String topic,
                                                   ImmutableMap<String, String> properties,
                                                   ComputeResource computeResource) {
        PKafkaLoadInfo kafkaLoadInfo = new PKafkaLoadInfo();
        kafkaLoadInfo.brokers = brokerList;
        kafkaLoadInfo.topic = topic;
        if (RunMode.isSharedDataMode()) {
            kafkaLoadInfo.warehouseId = computeResource.getWarehouseId();
            kafkaLoadInfo.workgroupId = computeResource.getWorkerGroupId();
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            PStringPair pair = new PStringPair();
            pair.key = entry.getKey();
            pair.val = entry.getValue();
            if (kafkaLoadInfo.properties == null) {
                kafkaLoadInfo.properties = Lists.newArrayList();
            }
            kafkaLoadInfo.properties.add(pair);
        }
        return kafkaLoadInfo;
    }

    static class ProxyAPI {
        public List<Integer> getAllKafkaPartitions(String brokerList, String topic,
                                                   ImmutableMap<String, String> convertedCustomProperties,
                                                   ComputeResource computeResource)
                throws StarRocksException {
            // create request
            PKafkaMetaProxyRequest metaRequest = new PKafkaMetaProxyRequest();
            metaRequest.kafkaInfo = genPKafkaLoadInfo(brokerList, topic, convertedCustomProperties, computeResource);
            PProxyRequest request = new PProxyRequest();
            request.kafkaMetaRequest = metaRequest;

            PProxyResult result = sendProxyRequest(request);
            return result.kafkaMetaResult.partitionIds;
        }

        public Map<Integer, Long> getLatestOffsets(String brokerList, String topic,
                                                   ImmutableMap<String, String> properties,
                                                   List<Integer> partitions,
                                                   ComputeResource computeResource) throws StarRocksException {
            return getOffsets(brokerList, topic, properties, partitions, true, computeResource);
        }

        public Map<Integer, Long> getBeginningOffsets(String brokerList, String topic,
                                                      ImmutableMap<String, String> properties,
                                                      List<Integer> partitions,
                                                      ComputeResource computeResource) throws StarRocksException {
            return getOffsets(brokerList, topic, properties, partitions, false, computeResource);
        }

        public Map<Integer, Long> getOffsets(String brokerList, String topic,
                                             ImmutableMap<String, String> properties,
                                             List<Integer> partitions, boolean isLatest,
                                             ComputeResource computeResource) throws StarRocksException {
            // create request
            PKafkaOffsetProxyRequest offsetRequest = new PKafkaOffsetProxyRequest();
            offsetRequest.kafkaInfo = genPKafkaLoadInfo(brokerList, topic, properties, computeResource);
            offsetRequest.partitionIds = partitions;
            PProxyRequest request = new PProxyRequest();
            request.kafkaOffsetRequest = offsetRequest;

            // send request
            PProxyResult result = sendProxyRequest(request);

            // assembly result
            Map<Integer, Long> partitionOffsets = Maps.newHashMapWithExpectedSize(partitions.size());
            List<Long> offsets;
            if (isLatest) {
                offsets = result.kafkaOffsetResult.latestOffsets;
            } else {
                offsets = result.kafkaOffsetResult.beginningOffsets;
            }
            for (int i = 0; i < result.kafkaOffsetResult.partitionIds.size(); i++) {
                partitionOffsets.put(result.kafkaOffsetResult.partitionIds.get(i), offsets.get(i));
            }
            return partitionOffsets;
        }

        public List<PKafkaOffsetProxyResult> getBatchOffsets(List<PKafkaOffsetProxyRequest> requests)
                throws StarRocksException {
            // create request
            PProxyRequest pProxyRequest = new PProxyRequest();
            PKafkaOffsetBatchProxyRequest pKafkaOffsetBatchProxyRequest = new PKafkaOffsetBatchProxyRequest();
            pKafkaOffsetBatchProxyRequest.requests = requests;
            pProxyRequest.kafkaOffsetBatchRequest = pKafkaOffsetBatchProxyRequest;

            // send request
            PProxyResult result = sendProxyRequest(pProxyRequest);

            return result.kafkaOffsetBatchResult.results;
        }

        private PProxyResult sendProxyRequest(PProxyRequest request) throws StarRocksException {
            // TODO: need to refactor after be split into cn + dn
            List<Long> nodeIds = new ArrayList<>();
            if (RunMode.isSharedDataMode()) {
                final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
                final ComputeResourceProvider computeResourceProvider =
                        warehouseManager.getComputeResourceProvider();
                ComputeResource computeResource = WarehouseManager.DEFAULT_RESOURCE;
                if (request.kafkaMetaRequest != null) {
                    computeResource = computeResourceProvider.ofComputeResource(
                            request.kafkaMetaRequest.kafkaInfo.warehouseId,
                            request.kafkaMetaRequest.kafkaInfo.workgroupId);
                } else if (request.kafkaOffsetRequest != null) {
                    computeResource = computeResourceProvider.ofComputeResource(
                            request.kafkaOffsetRequest.kafkaInfo.warehouseId,
                            request.kafkaOffsetRequest.kafkaInfo.workgroupId);
                } else if (request.kafkaOffsetBatchRequest != null && request.kafkaOffsetBatchRequest.requests != null) {
                    // contain kafkaOffsetBatchRequest
                    PKafkaOffsetProxyRequest req = request.kafkaOffsetBatchRequest.requests.get(0);
                    computeResource = computeResourceProvider.ofComputeResource(
                            req.kafkaInfo.warehouseId, req.kafkaInfo.workgroupId);
                }
                List<Long> computeNodeIds = null;
                try {
                    computeNodeIds = warehouseManager.getAllComputeNodeIds(computeResource);
                } catch (ErrorReportException e) {
                    throw new LoadException(
                            String.format("Failed to send get kafka partition info request. err: %s", e.getMessage()));
                }
                for (long nodeId : computeNodeIds) {
                    ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr()
                            .getClusterInfo().getBackendOrComputeNode(nodeId);
                    if (node != null && node.isAlive()) {
                        nodeIds.add(nodeId);
                    }
                }
                if (nodeIds.isEmpty()) {
                    throw new LoadException(
                            "Failed to send get kafka partition info request. err: No alive backends or compute nodes");
                }
            } else {
                nodeIds = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(true);
                if (nodeIds.isEmpty()) {
                    throw new LoadException("Failed to send get kafka partition info request. err: No alive backends");
                }
            }

            // get info
            long kafkaTimeoutSecond = Config.routine_load_kafka_timeout_second;
            long taskTimeoutSecond = Config.routine_load_task_timeout_second;
            int retryTimes = 0;
            String msg = null;
            long nodeId = -1L;

            while (true) {
                // Remove the last failed node and retry
                nodeIds.remove(nodeId);
                if (nodeIds.isEmpty()) {
                    throw new LoadException(msg);
                }

                Collections.shuffle(nodeIds);
                nodeId = nodeIds.get(0);
                ComputeNode be = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId);
                TNetworkAddress address = new TNetworkAddress(be.getHost(), be.getBrpcPort());

                try {
                    request.timeout = kafkaTimeoutSecond;
                    Future<PProxyResult> future = BackendServiceClient.getInstance().getInfo(address, request);
                    PProxyResult result = future.get(kafkaTimeoutSecond, TimeUnit.SECONDS);
                    TStatusCode code = TStatusCode.findByValue(result.status.statusCode);
                    if (code != TStatusCode.OK) {
                        msg = String.format("Failed to process get kafka partition info in BE %s, err: %s",
                                address, StringUtils.join(result.status.errorMsgs, ","));
                        throw new LoadException(msg);
                    }
                    return result;
                } catch (InterruptedException e) {
                    // Not retry
                    msg = String.format("Got interrupted exception when sending get kafka partition info request to BE %s",
                            address);
                    LOG.warn(msg);
                    Thread.currentThread().interrupt();
                    throw new LoadException(msg);
                } catch (Exception e) {
                    // Retry three times
                    if (e instanceof LoadException) {
                        msg = e.getMessage();
                    } else if (e.getMessage().contains("Unable to validate object")) {
                        msg = String.format("Failed to send get kafka partition info request to BE %s. err: BE is not alive",
                                address);
                    } else {
                        msg = String.format("Failed to send get kafka partition info request to BE %s, err: %s",
                                address, e.getMessage());
                    }

                    LOG.warn("{}, retry: {}", msg, retryTimes);
                    if (++retryTimes > 3 || (retryTimes + 1) * kafkaTimeoutSecond > taskTimeoutSecond) {
                        throw new LoadException(msg);
                    }
                }
            }
        }
    }
}
