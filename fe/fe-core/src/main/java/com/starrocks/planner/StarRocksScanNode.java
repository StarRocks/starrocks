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

package com.starrocks.planner;

import com.google.common.base.MoreObjects;
import com.starrocks.catalog.StarRocksExternalTable;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TStarRocksRemoteScanOutput;
import com.starrocks.thrift.TStarRocksRemoteScanRequiredOutput;
import com.starrocks.thrift.TStarRocksScanNode;
import com.starrocks.thrift.TStarRocksScanRange;
import com.starrocks.thrift.TStarRocksScanTransport;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StarRocksScanNode extends ScanNode {
    private final StarRocksExternalTable table;
    private final ComputeResource computeResource;
    private final List<TScanRangeLocations> scanRangeLocations = new ArrayList<>();
    private TStarRocksScanTransport transport = TStarRocksScanTransport.STARROCKS_ARROW_FLIGHT;
    private List<TNetworkAddress> remoteBes = new ArrayList<>();
    private List<String> outputColumnNames = new ArrayList<>();
    private List<TStarRocksRemoteScanOutput> remoteOutputs = new ArrayList<>();
    // Remote execution parameters captured at plan time for EXPLAIN visibility.
    private String pushdownPredicateSql = "";
    private List<String> nonPushdownReasons = new ArrayList<>();
    private List<TStarRocksRemoteScanRequiredOutput> requiredOutputs = new ArrayList<>();
    private long remoteSoftLimit = -1;
    private long remoteSchemaVersion = -1;
    private Map<String, String> forwardedSessionVars = new LinkedHashMap<>();

    public StarRocksScanNode(PlanNodeId id, TupleDescriptor desc, ComputeResource computeResource) {
        super(id, desc, "SCAN STARROCKS");
        this.table = (StarRocksExternalTable) desc.getTable();
        this.computeResource = computeResource;
    }

    public void setupScanRangeLocations(List<TStarRocksScanRange> remoteScanRanges,
                                        TStarRocksScanTransport transport) {
        this.remoteBes = remoteScanRanges == null ? new ArrayList<>() : remoteScanRanges.stream()
                .filter(TStarRocksScanRange::isSetRemote_be)
                .map(TStarRocksScanRange::getRemote_be)
                .collect(Collectors.toList());
        this.transport = transport == null ? TStarRocksScanTransport.STARROCKS_ARROW_FLIGHT : transport;
        scanRangeLocations.clear();
        if (remoteScanRanges == null || remoteScanRanges.isEmpty()) {
            return;
        }

        List<ComputeNode> localNodes = LoadScanNode.getAvailableComputeNodes(computeResource);
        if (localNodes.isEmpty()) {
            throw new IllegalStateException("No available local BE/CN nodes for StarRocks remote scan");
        }

        for (int i = 0; i < remoteScanRanges.size(); i++) {
            TStarRocksScanRange inputRange = remoteScanRanges.get(i);
            if (!inputRange.isSetScan_token() || inputRange.getScan_token().isEmpty() || !inputRange.isSetRemote_be()) {
                continue;
            }
            ComputeNode localNode = localNodes.get(i % localNodes.size());
            TScanRangeLocation location = new TScanRangeLocation();
            location.setBackend_id(localNode.getId());
            location.setServer(new TNetworkAddress(localNode.getHost(), localNode.getBePort()));

            TStarRocksScanRange starRocksScanRange = new TStarRocksScanRange(inputRange);
            starRocksScanRange.setTransport(inputRange.isSetTransport() ? inputRange.getTransport() : this.transport);
            starRocksScanRange.setPacket_seq(inputRange.isSetPacket_seq() ? inputRange.getPacket_seq() : 0L);

            TScanRange scanRange = new TScanRange();
            scanRange.setStarrocks_scan_range(starRocksScanRange);

            TScanRangeLocations locations = new TScanRangeLocations();
            locations.setScan_range(scanRange);
            locations.addToLocations(location);
            scanRangeLocations.add(locations);
        }
        if (scanRangeLocations.isEmpty()) {
            throw new IllegalStateException("No valid StarRocks remote scan ranges returned by remote FE");
        }
    }

    public void setOutputColumnNames(List<String> outputColumnNames) {
        this.outputColumnNames = outputColumnNames == null ? new ArrayList<>() : new ArrayList<>(outputColumnNames);
    }

    public void setRemoteOutputs(List<TStarRocksRemoteScanOutput> remoteOutputs) {
        this.remoteOutputs = remoteOutputs == null ? new ArrayList<>() : new ArrayList<>(remoteOutputs);
    }

    public void setRemoteScanExplainInfo(String pushdownPredicateSql,
                                         List<String> nonPushdownReasons,
                                         List<TStarRocksRemoteScanRequiredOutput> requiredOutputs,
                                         long softLimit,
                                         long schemaVersion,
                                         Map<String, String> forwardedSessionVars) {
        this.pushdownPredicateSql = pushdownPredicateSql == null ? "" : pushdownPredicateSql;
        this.nonPushdownReasons = nonPushdownReasons == null ? new ArrayList<>() : new ArrayList<>(nonPushdownReasons);
        this.requiredOutputs = requiredOutputs == null ? new ArrayList<>() : new ArrayList<>(requiredOutputs);
        this.remoteSoftLimit = softLimit;
        this.remoteSchemaVersion = schemaVersion;
        this.forwardedSessionVars =
                forwardedSessionVars == null ? new LinkedHashMap<>() : new LinkedHashMap<>(forwardedSessionVars);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocations;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.STARROCKS_SCAN_NODE;
        TStarRocksScanNode starRocksScanNode = new TStarRocksScanNode();
        starRocksScanNode.setTuple_id(desc.getId().asInt());
        starRocksScanNode.setTransport(transport);
        starRocksScanNode.setRemote_bes(remoteBes);
        List<String> thriftOutputColumnNames = outputColumnNames.isEmpty() ? desc.getSlots().stream()
                .filter(SlotDescriptor::isMaterialized)
                .map(slot -> slot.getColumn().getName())
                .collect(Collectors.toList()) : outputColumnNames;
        starRocksScanNode.setOutput_column_names(thriftOutputColumnNames);
        if (!remoteOutputs.isEmpty()) {
            starRocksScanNode.setRemote_outputs(remoteOutputs);
        }
        msg.starrocks_scan_node = starRocksScanNode;

        setConnectorCatalogType(msg);
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        return helper.addValue(super.debugString()).addValue("table=" + table.getName()).toString();
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(table.getName()).append("\n");
        output.append(prefix).append("DATABASE: ").append(table.getCatalogDBName()).append("\n");
        output.append(prefix).append("TRANSPORT: ").append(transport).append("\n");
        output.append(prefix).append("REMOTE BES: ").append(remoteBes).append("\n");
        if (!pushdownPredicateSql.isEmpty()) {
            output.append(prefix).append("PUSHDOWN PREDICATE: ").append(pushdownPredicateSql).append("\n");
        }
        if (detailLevel == TExplainLevel.VERBOSE) {
            if (!nonPushdownReasons.isEmpty()) {
                output.append(prefix).append("NON-PUSHDOWN PREDICATE: ")
                        .append(String.join("; ", nonPushdownReasons)).append("\n");
            }
            if (remoteSoftLimit >= 0) {
                output.append(prefix).append("SOFT LIMIT: ").append(remoteSoftLimit).append("\n");
            }
            if (remoteSchemaVersion >= 0) {
                output.append(prefix).append("SCHEMA VERSION: ").append(remoteSchemaVersion).append("\n");
            }
            if (!requiredOutputs.isEmpty()) {
                output.append(prefix).append("REQUIRED OUTPUTS: ")
                        .append(requiredOutputs.stream()
                                .map(StarRocksScanNode::explainRequiredOutput)
                                .collect(Collectors.joining("; ")))
                        .append("\n");
            }
            if (!forwardedSessionVars.isEmpty()) {
                output.append(prefix).append("FORWARDED SESSION VARS: ")
                        .append(forwardedSessionVars.entrySet().stream()
                                .sorted(Map.Entry.comparingByKey())
                                .map(entry -> entry.getKey() + "=" + entry.getValue())
                                .collect(Collectors.joining(", ")))
                        .append("\n");
            }
            output.append(explainColumnAccessPath(prefix));
        }
        return output.toString();
    }

    private static String explainRequiredOutput(TStarRocksRemoteScanRequiredOutput output) {
        StringBuilder sb = new StringBuilder();
        sb.append("slot=").append(output.getLocal_slot_id());
        if (output.isSetRoot_column()) {
            sb.append(" col=").append(output.getRoot_column());
        }
        if (output.isSetWire_shape()) {
            sb.append(" shape=").append(output.getWire_shape());
        }
        return sb.toString();
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return false;
    }
}
