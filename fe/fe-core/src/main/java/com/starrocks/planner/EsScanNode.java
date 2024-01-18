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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/EsScanNode.java

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

package com.starrocks.planner;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.EsTable;
import com.starrocks.common.exception.UserException;
import com.starrocks.connector.elasticsearch.EsShardPartitions;
import com.starrocks.connector.elasticsearch.EsShardRouting;
import com.starrocks.connector.elasticsearch.QueryBuilders;
import com.starrocks.connector.elasticsearch.QueryConverter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TEsScanNode;
import com.starrocks.thrift.TEsScanRange;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class EsScanNode extends ScanNode {

    private static final Logger LOG = LogManager.getLogger(EsScanNode.class);

    private final Random random = new Random(System.currentTimeMillis());
    private Multimap<String, ComputeNode> nodeMap;
    private List<ComputeNode> nodeList;
    private List<TScanRangeLocations> shardScanRanges = Lists.newArrayList();
    private EsTable table;

    public EsScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        table = (EsTable) (desc.getTable());
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);

        assignNodes();
    }

    @Override
    public int getNumInstances() {
        return shardScanRanges.size();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return shardScanRanges;
    }

    public void setShardScanRanges(List<TScanRangeLocations> shardScanRanges) {
        this.shardScanRanges = shardScanRanges;
    }

    @Override
    public void finalizeStats(Analyzer analyzer) throws UserException {
    }

    /**
     * return whether can use the doc_values scan
     * 0 and 1 are returned to facilitate StarRocks BE processing
     *
     * @param desc            the fields needs to read from ES
     * @param docValueContext the mapping for docvalues fields from origin field to doc_value fields
     * @return
     */
    private int useDocValueScan(TupleDescriptor desc, Map<String, String> docValueContext) {
        ArrayList<SlotDescriptor> slotDescriptors = desc.getSlots();
        List<String> selectedFields = new ArrayList<>(slotDescriptors.size());
        for (SlotDescriptor slotDescriptor : slotDescriptors) {
            selectedFields.add(slotDescriptor.getColumn().getName());
        }
        if (selectedFields.size() > table.maxDocValueFields()) {
            return 0;
        }
        Set<String> docValueFields = docValueContext.keySet();
        boolean useDocValue = true;
        for (String selectedField : selectedFields) {
            if (!docValueFields.contains(selectedField)) {
                useDocValue = false;
                break;
            }
        }
        return useDocValue ? 1 : 0;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        if (EsTable.KEY_TRANSPORT_HTTP.equals(table.getTransport())) {
            msg.node_type = TPlanNodeType.ES_HTTP_SCAN_NODE;
        } else {
            msg.node_type = TPlanNodeType.ES_SCAN_NODE;
        }
        Map<String, String> properties = Maps.newHashMap();
        properties.put(EsTable.KEY_USER, table.getUserName());
        properties.put(EsTable.KEY_PASSWORD, table.getPasswd());
        properties.put(EsTable.KEY_ES_NET_SSL, String.valueOf(table.sslEnabled()));
        String time_zone = table.getTimeZone();
        if (time_zone != null) {
            // If user has set timezone, we need to send it to BE
            properties.put(EsTable.KEY_TIME_ZONE, time_zone);
        }
        TEsScanNode esScanNode = new TEsScanNode(desc.getId().asInt());
        esScanNode.setProperties(properties);
        if (table.isDocValueScanEnable()) {
            esScanNode.setDocvalue_context(table.docValueContext());
            properties.put(EsTable.KEY_DOC_VALUES_MODE, String.valueOf(useDocValueScan(desc, table.docValueContext())));
        }
        if (table.isKeywordSniffEnable() && table.fieldsContext().size() > 0) {
            esScanNode.setFields_context(table.fieldsContext());
        }
        msg.es_scan_node = esScanNode;
    }

    public void assignNodes() throws UserException {
        nodeMap = HashMultimap.create();
        nodeList = Lists.newArrayList();
        for (ComputeNode node : GlobalStateMgr.getCurrentSystemInfo().
                backendAndComputeNodeStream().collect(Collectors.toList())) {
            if (node.isAlive()) {
                nodeMap.put(node.getHost(), node);
                nodeList.add(node);
            }
        }
        if (nodeMap.isEmpty()) {
            throw new UserException("No Alive backends or compute nodes");
        }
    }

    public List<TScanRangeLocations> computeShardLocations(List<EsShardPartitions> selectedIndex) {
        int size = nodeList.size();
        int nodeIndex = random.nextInt(size);
        List<TScanRangeLocations> result = Lists.newArrayList();
        for (EsShardPartitions indexState : selectedIndex) {
            for (List<EsShardRouting> shardRouting : indexState.getShardRoutings().values()) {
                // get compute nodes
                Set<ComputeNode> colocatedNodes = Sets.newHashSet();
                int numNode = Math.min(3, size);
                List<TNetworkAddress> shardAllocations = new ArrayList<>();
                for (EsShardRouting item : shardRouting) {
                    shardAllocations.add(EsTable.KEY_TRANSPORT_HTTP.equals(table.getTransport()) ? item.getHttpAddress() :
                            item.getAddress());
                }

                Collections.shuffle(shardAllocations, random);
                for (TNetworkAddress address : shardAllocations) {
                    if (address == null) {
                        continue;
                    }
                    colocatedNodes.addAll(nodeMap.get(address.getHostname()));
                }
                boolean usingRandomNode = colocatedNodes.size() == 0;
                List<ComputeNode> candidateNodeList = Lists.newArrayList();
                if (usingRandomNode) {
                    for (int i = 0; i < numNode; ++i) {
                        candidateNodeList.add(nodeList.get(nodeIndex++ % size));
                    }
                } else {
                    candidateNodeList.addAll(colocatedNodes);
                    if (!candidateNodeList.isEmpty()) {
                        Collections.shuffle(candidateNodeList);
                    }
                }

                // Locations
                TScanRangeLocations locations = new TScanRangeLocations();
                for (int i = 0; i < numNode && i < candidateNodeList.size(); ++i) {
                    TScanRangeLocation location = new TScanRangeLocation();
                    ComputeNode be = candidateNodeList.get(i);
                    location.setBackend_id(be.getId());
                    location.setServer(new TNetworkAddress(be.getHost(), be.getBePort()));
                    locations.addToLocations(location);
                }

                // Generate on es scan range
                TEsScanRange esScanRange = new TEsScanRange();
                esScanRange.setEs_hosts(shardAllocations);
                esScanRange.setIndex(shardRouting.get(0).getIndexName());
                if (table.getMappingType() != null) {
                    esScanRange.setType(table.getMappingType());
                }
                esScanRange.setShard_id(shardRouting.get(0).getShardId());
                // Scan range
                TScanRange scanRange = new TScanRange();
                scanRange.setEs_scan_range(esScanRange);
                locations.setScan_range(scanRange);
                // result
                result.add(locations);
            }

        }
        if (LOG.isDebugEnabled()) {
            StringBuilder scratchBuilder = new StringBuilder();
            for (TScanRangeLocations scanRangeLocations : result) {
                scratchBuilder.append(scanRangeLocations.toString());
                scratchBuilder.append(" ");
            }
            LOG.debug("ES table {}  scan ranges {}", table.getName(), scratchBuilder.toString());
        }
        return result;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(table.getName()).append("\n");

        if (null != sortColumn) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }

        if (conjuncts.isEmpty()) {
            output.append(prefix).append("PREDICATES: ").append(
                    getExplainString(conjuncts)).append("\n");
            output.append(prefix).append("ES_QUERY_DSL: ").append("{\"match_all\": {}}").append("\n");
        } else {
            QueryConverter queryConverter = new QueryConverter();
            QueryBuilders.QueryBuilder queryBuilder = queryConverter.convert(getConjuncts());
            output.append(prefix).append("PREDICATES: ").append(
                    getExplainString(conjuncts)).append("\n");
            // reserved for later using: LOCAL_PREDICATES is processed by StarRocks EsScanNode
            output.append(prefix).append("LOCAL_PREDICATES: ")
                    .append(getExplainString(queryConverter.localConjuncts()))
                    .append("\n");
            output.append(prefix).append("REMOTE_PREDICATES: ")
                    .append(getExplainString(queryConverter.remoteConjuncts()))
                    .append("\n");
            output.append(prefix)
                    .append("ES_QUERY_DSL: ")
                    .append(queryBuilder.toString())
                    .append("\n");
        }
        String indexName = table.getIndexName();
        String typeName = table.getMappingType();
        if (typeName == null) {
            output.append(prefix)
                    .append(String.format("ES index: %s", indexName))
                    .append("\n");
        } else {
            output.append(prefix)
                    .append(String.format("ES index/type: %s/%s", indexName, typeName))
                    .append("\n");
        }
        return output.toString();
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }
}
