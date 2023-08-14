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
import com.starrocks.common.LoadException;
import com.starrocks.common.UserException;
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
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.warehouse.Warehouse;
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
                                                      ImmutableMap<String, String> properties) throws UserException {
        return PROXY_API.getAllKafkaPartitions(brokerList, topic, properties);
    }

    // latest offset is (the latest existing message offset + 1)
    public static Map<Integer, Long> getLatestOffsets(String brokerList, String topic,
                                                      ImmutableMap<String, String> properties,
                                                      List<Integer> partitions) throws UserException {
        return PROXY_API.getLatestOffsets(brokerList, topic, properties, partitions);
    }

    public static Map<Integer, Long> getBeginningOffsets(String brokerList, String topic,
                                                         ImmutableMap<String, String> properties,
                                                         List<Integer> partitions) throws UserException {
        return PROXY_API.getBeginningOffsets(brokerList, topic, properties, partitions);
    }

    public static List<PKafkaOffsetProxyResult> getBatchOffsets(List<PKafkaOffsetProxyRequest> requests)
            throws UserException {
        return PROXY_API.getBatchOffsets(requests);
    }

    public static PKafkaLoadInfo genPKafkaLoadInfo(String brokerList, String topic,
                                                   ImmutableMap<String, String> properties) {
        PKafkaLoadInfo kafkaLoadInfo = new PKafkaLoadInfo();
        kafkaLoadInfo.brokers = brokerList;
        kafkaLoadInfo.topic = topic;
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
                                                   ImmutableMap<String, String> convertedCustomProperties)
                throws UserException {
            // create request
            PKafkaMetaProxyRequest metaRequest = new PKafkaMetaProxyRequest();
            metaRequest.kafkaInfo = genPKafkaLoadInfo(brokerList, topic, convertedCustomProperties);
            PProxyRequest request = new PProxyRequest();
            request.kafkaMetaRequest = metaRequest;

            PProxyResult result = sendProxyRequest(request);
            return result.kafkaMetaResult.partitionIds;
        }

        public Map<Integer, Long> getLatestOffsets(String brokerList, String topic,
                                                   ImmutableMap<String, String> properties,
                                                   List<Integer> partitions) throws UserException {
            return getOffsets(brokerList, topic, properties, partitions, true);
        }

        public Map<Integer, Long> getBeginningOffsets(String brokerList, String topic,
                                                      ImmutableMap<String, String> properties,
                                                      List<Integer> partitions) throws UserException {
            return getOffsets(brokerList, topic, properties, partitions, false);
        }

        public Map<Integer, Long> getOffsets(String brokerList, String topic,
                                             ImmutableMap<String, String> properties,
                                             List<Integer> partitions, boolean isLatest) throws UserException {
            // create request
            PKafkaOffsetProxyRequest offsetRequest = new PKafkaOffsetProxyRequest();
            offsetRequest.kafkaInfo = genPKafkaLoadInfo(brokerList, topic, properties);
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
                throws UserException {
            // create request
            PProxyRequest pProxyRequest = new PProxyRequest();
            PKafkaOffsetBatchProxyRequest pKafkaOffsetBatchProxyRequest = new PKafkaOffsetBatchProxyRequest();
            pKafkaOffsetBatchProxyRequest.requests = requests;
            pProxyRequest.kafkaOffsetBatchRequest = pKafkaOffsetBatchProxyRequest;

            // send request
            PProxyResult result = sendProxyRequest(pProxyRequest);

            return result.kafkaOffsetBatchResult.results;
        }

        private PProxyResult sendProxyRequest(PProxyRequest request) throws UserException {
            TNetworkAddress address = new TNetworkAddress();
            try {
                // TODO: need to refactor after be split into cn + dn
                List<Long> nodeIds = new ArrayList<>();
                if ((RunMode.getCurrentRunMode() == RunMode.SHARED_DATA)) {
                    Warehouse warehouse = GlobalStateMgr.getCurrentWarehouseMgr().getDefaultWarehouse();
                    for (long nodeId : warehouse.getAnyAvailableCluster().getComputeNodeIds()) {
                        ComputeNode node = GlobalStateMgr.getCurrentSystemInfo().getBackendOrComputeNode(nodeId);
                        if (node != null && node.isAlive()) {
                            nodeIds.add(nodeId);
                        }
                    }
                    if (nodeIds.isEmpty()) {
                        throw new LoadException("Failed to send proxy request. No alive backends or computeNodes");
                    }
                } else {
                    nodeIds = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true);
                    if (nodeIds.isEmpty()) {
                        throw new LoadException("Failed to send proxy request. No alive backends");
                    }
                }

                Collections.shuffle(nodeIds);

                ComputeNode be = GlobalStateMgr.getCurrentSystemInfo().getBackendOrComputeNode(nodeIds.get(0));
                address = new TNetworkAddress(be.getHost(), be.getBrpcPort());

                // get info
                int retryTimes = 0;
                while (true) {
                    request.timeout = Config.routine_load_kafka_timeout_second;
                    Future<PProxyResult> future = BackendServiceClient.getInstance().getInfo(address, request);
                    PProxyResult result;
                    try {
                        result = future.get(Config.routine_load_kafka_timeout_second, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        LOG.warn("failed to send proxy request to " + address + " err " + e.getMessage());
                        // Jprotobuf-rpc-socket throws an ExecutionException when an exception occurs.
                        // We use the error message to identify the type of exception.
                        if (e.getMessage().contains("Ocurrs time out")) {
                            // When getting kafka info timed out, we tried again three times.
                            if (++retryTimes > 3 || (retryTimes + 1) * Config.routine_load_kafka_timeout_second >
                                                                            Config.routine_load_task_timeout_second) {
                                throw e;
                            }
                            continue;
                        } else {
                            throw e;
                        }
                    }
                    TStatusCode code = TStatusCode.findByValue(result.status.statusCode);
                    if (code != TStatusCode.OK) {
                        LOG.warn("failed to send proxy request to " + address + " err " + result.status.errorMsgs);
                        throw new UserException(
                                "failed to send proxy request to " + address + " err " + result.status.errorMsgs);
                    } else {
                        return result;
                    }
                }
            } catch (InterruptedException ie) {
                LOG.warn("got interrupted exception when sending proxy request to " + address);
                Thread.currentThread().interrupt();
                throw new LoadException("got interrupted exception when sending proxy request to " + address);
            } catch (Exception e) {
                LOG.warn("failed to send proxy request to " + address + " err " + e.getMessage());
                throw new LoadException("failed to send proxy request to " + address + " err " + e.getMessage());
            }
        }
    }
}

