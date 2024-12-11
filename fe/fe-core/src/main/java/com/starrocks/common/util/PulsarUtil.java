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

<<<<<<< HEAD

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
package com.starrocks.common.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.LoadException;
<<<<<<< HEAD
import com.starrocks.common.UserException;
=======
import com.starrocks.common.StarRocksException;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.proto.PPulsarBacklogBatchProxyRequest;
import com.starrocks.proto.PPulsarBacklogProxyRequest;
import com.starrocks.proto.PPulsarBacklogProxyResult;
import com.starrocks.proto.PPulsarLoadInfo;
import com.starrocks.proto.PPulsarMetaProxyRequest;
import com.starrocks.proto.PPulsarProxyRequest;
import com.starrocks.proto.PPulsarProxyResult;
import com.starrocks.proto.PStringPair;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
<<<<<<< HEAD
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.warehouse.Warehouse;
=======
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class PulsarUtil {
    private static final Logger LOG = LogManager.getLogger(PulsarUtil.class);

    private static final PulsarUtil.ProxyAPI PROXY_API = new PulsarUtil.ProxyAPI();

    public static List<String> getAllPulsarPartitions(String serviceUrl, String topic, String subscription,
<<<<<<< HEAD
                                                       ImmutableMap<String, String> properties) throws UserException {
        return PROXY_API.getAllPulsarPartitions(serviceUrl, topic, subscription, properties);
    }

    public static Map<String, Long> getBacklogNums(String serviceUrl, String topic, String subscription,
                                                    ImmutableMap<String, String> properties,
                                                    List<String> partitions) throws UserException {
        return PROXY_API.getBacklogNums(serviceUrl, topic, subscription, properties, partitions);
    }

    public static List<PPulsarBacklogProxyResult> getBatchBacklogNums(List<PPulsarBacklogProxyRequest> requests)
            throws UserException {
=======
                                                      ImmutableMap<String, String> properties,
                                                      long warehouseId) throws StarRocksException {
        return PROXY_API.getAllPulsarPartitions(serviceUrl, topic, subscription, properties, warehouseId);
    }

    public static Map<String, Long> getBacklogNums(String serviceUrl, String topic, String subscription,
                                                   ImmutableMap<String, String> properties,
                                                   List<String> partitions,
                                                   long warehouseId) throws StarRocksException {
        return PROXY_API.getBacklogNums(serviceUrl, topic, subscription, properties, partitions, warehouseId);
    }

    public static List<PPulsarBacklogProxyResult> getBatchBacklogNums(List<PPulsarBacklogProxyRequest> requests)
            throws StarRocksException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        return PROXY_API.getBatchBacklogNums(requests);
    }

    public static PPulsarLoadInfo genPPulsarLoadInfo(String serviceUrl, String topic, String subscription,
<<<<<<< HEAD
                                                   ImmutableMap<String, String> properties) {
=======
                                                     ImmutableMap<String, String> properties,
                                                     long warehouseId) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        PPulsarLoadInfo pulsarLoadInfo = new PPulsarLoadInfo();
        pulsarLoadInfo.serviceUrl = serviceUrl;
        pulsarLoadInfo.topic = topic;
        pulsarLoadInfo.subscription = subscription;
<<<<<<< HEAD
=======
        pulsarLoadInfo.warehouseId = warehouseId;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            PStringPair pair = new PStringPair();
            pair.key = entry.getKey();
            pair.val = entry.getValue();
            if (pulsarLoadInfo.properties == null) {
                pulsarLoadInfo.properties = Lists.newArrayList();
            }
            pulsarLoadInfo.properties.add(pair);
        }
        return pulsarLoadInfo;
    }

    static class ProxyAPI {
        public List<String> getAllPulsarPartitions(String serviceUrl, String topic, String subscription,
<<<<<<< HEAD
                                                   ImmutableMap<String, String> convertedCustomProperties)
                throws UserException {
            // create request
            PPulsarMetaProxyRequest metaRequest = new PPulsarMetaProxyRequest();
            metaRequest.pulsarInfo = genPPulsarLoadInfo(serviceUrl, topic, subscription, convertedCustomProperties);
=======
                                                   ImmutableMap<String, String> convertedCustomProperties,
                                                   long warehouseId)
                throws StarRocksException {
            // create request
            PPulsarMetaProxyRequest metaRequest = new PPulsarMetaProxyRequest();
            metaRequest.pulsarInfo = genPPulsarLoadInfo(serviceUrl, topic, subscription, convertedCustomProperties, warehouseId);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            PPulsarProxyRequest request = new PPulsarProxyRequest();
            request.pulsarMetaRequest = metaRequest;

            PPulsarProxyResult result = sendProxyRequest(request);
            return result.pulsarMetaResult.partitions;
        }

        public Map<String, Long> getBacklogNums(String serviceUrl, String topic, String subscription,
<<<<<<< HEAD
                                                ImmutableMap<String, String> properties, List<String> partitions)
                throws UserException {
            // create request
            PPulsarBacklogProxyRequest backlogRequest = new PPulsarBacklogProxyRequest();
            backlogRequest.pulsarInfo = genPPulsarLoadInfo(serviceUrl, topic, subscription, properties);
=======
                                                ImmutableMap<String, String> properties, List<String> partitions,
                                                long warehouseId)
                throws StarRocksException {
            // create request
            PPulsarBacklogProxyRequest backlogRequest = new PPulsarBacklogProxyRequest();
            backlogRequest.pulsarInfo = genPPulsarLoadInfo(serviceUrl, topic, subscription, properties, warehouseId);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            backlogRequest.partitions = partitions;
            PPulsarProxyRequest request = new PPulsarProxyRequest();
            request.pulsarBacklogRequest = backlogRequest;

            // send request
            PPulsarProxyResult result = sendProxyRequest(request);

            // assembly result
            Map<String, Long> partitionBacklogs = Maps.newHashMapWithExpectedSize(partitions.size());
            List<Long> backlogs = result.pulsarBacklogResult.backlogNums;
            for (int i = 0; i < result.pulsarBacklogResult.partitions.size(); i++) {
                partitionBacklogs.put(result.pulsarBacklogResult.partitions.get(i), backlogs.get(i));
            }
            return partitionBacklogs;
        }

        public List<PPulsarBacklogProxyResult> getBatchBacklogNums(List<PPulsarBacklogProxyRequest> requests)
<<<<<<< HEAD
                throws UserException {
=======
                throws StarRocksException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            // create request
            PPulsarProxyRequest pProxyRequest = new PPulsarProxyRequest();
            PPulsarBacklogBatchProxyRequest pPulsarBacklogBatchProxyRequest = new PPulsarBacklogBatchProxyRequest();
            pPulsarBacklogBatchProxyRequest.requests = requests;
            pProxyRequest.pulsarBacklogBatchRequest = pPulsarBacklogBatchProxyRequest;

            // send request
            PPulsarProxyResult result = sendProxyRequest(pProxyRequest);

            return result.pulsarBacklogBatchResult.results;
        }

<<<<<<< HEAD
        private PPulsarProxyResult sendProxyRequest(PPulsarProxyRequest request) throws UserException {
=======
        private PPulsarProxyResult sendProxyRequest(PPulsarProxyRequest request) throws StarRocksException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            TNetworkAddress address = new TNetworkAddress();
            try {
                // TODO: need to refactor after be split into cn + dn
                List<Long> nodeIds = new ArrayList<>();
<<<<<<< HEAD
                if ((RunMode.getCurrentRunMode() == RunMode.SHARED_DATA)) {
                    Warehouse warehouse = GlobalStateMgr.getCurrentWarehouseMgr().getDefaultWarehouse();
                    for (long nodeId : warehouse.getAnyAvailableCluster().getComputeNodeIds()) {
                        ComputeNode node = GlobalStateMgr.getCurrentSystemInfo().getBackendOrComputeNode(nodeId);
                        if (node != null && node.isAlive()) {
                            nodeIds.add(nodeId);
                        }
                    }
=======
                if ((RunMode.isSharedDataMode())) {
                    long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
                    if (request.pulsarMetaRequest != null) {
                        warehouseId = request.pulsarMetaRequest.pulsarInfo.warehouseId;
                    } else if (request.pulsarBacklogRequest != null) {
                        warehouseId = request.pulsarBacklogRequest.pulsarInfo.warehouseId;
                    } else if (request.pulsarBacklogBatchRequest != null) {
                        // contain kafkaOffsetBatchRequest
                        PPulsarBacklogProxyRequest req = request.pulsarBacklogBatchRequest.requests.get(0);
                        warehouseId = req.pulsarInfo.warehouseId;
                    }

                    nodeIds = GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllComputeNodeIds(warehouseId);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                    if (nodeIds.isEmpty()) {
                        throw new LoadException("Failed to send proxy request. No alive backends or computeNodes");
                    }
                } else {
<<<<<<< HEAD
                    nodeIds = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true);
=======
                    nodeIds = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(true);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                    if (nodeIds.isEmpty()) {
                        throw new LoadException("Failed to send proxy request. No alive backends");
                    }
                }

                Collections.shuffle(nodeIds);

<<<<<<< HEAD
                ComputeNode be = GlobalStateMgr.getCurrentSystemInfo().getBackendOrComputeNode(nodeIds.get(0));
=======
                ComputeNode be =
                        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeIds.get(0));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                address = new TNetworkAddress(be.getHost(), be.getBrpcPort());

                // get info
                request.timeout = Config.routine_load_pulsar_timeout_second;
                Future<PPulsarProxyResult> future = BackendServiceClient.getInstance().getPulsarInfo(address, request);
                PPulsarProxyResult result = future.get(Config.routine_load_pulsar_timeout_second, TimeUnit.SECONDS);
                TStatusCode code = TStatusCode.findByValue(result.status.statusCode);
                if (code != TStatusCode.OK) {
                    LOG.warn("failed to send proxy request to " + address + " err " + result.status.errorMsgs);
<<<<<<< HEAD
                    throw new UserException(
=======
                    throw new StarRocksException(
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                            "failed to send proxy request to " + address + " err " + result.status.errorMsgs);
                } else {
                    return result;
                }
            } catch (InterruptedException ie) {
<<<<<<< HEAD
                LOG.warn("got interrupted exception when sending proxy request to " + address);
                Thread.currentThread().interrupt();
                throw new LoadException("got interrupted exception when sending proxy request to " + address);
            } catch (Exception e) {
                LOG.warn("failed to send proxy request to " + address + " err " + e.getMessage());
=======
                LOG.warn("got interrupted exception when sending proxy request to " + address, ie);
                Thread.currentThread().interrupt();
                throw new LoadException("got interrupted exception when sending proxy request to " + address);
            } catch (Exception e) {
                LOG.warn("failed to send proxy request to " + address, e);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                throw new LoadException("failed to send proxy request to " + address + " err " + e.getMessage());
            }
        }
    }
}

