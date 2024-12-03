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

package com.starrocks.qe;

import com.google.common.base.Objects;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.SetMultimap;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.metric.MetricRepo;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.ProjectNode;
import com.starrocks.proto.PExecShortCircuitResult;
import com.starrocks.qe.scheduler.NonRecoverableException;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.ConfigurableSerDesFactory;
import com.starrocks.rpc.PBackendService;
import com.starrocks.rpc.PExecShortCircuitRequest;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TExecShortCircuitParams;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TKeyLiteralExpr;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TRuntimeProfileTree;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TStatusCode;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.starrocks.qe.scheduler.DefaultWorkerProvider.isWorkerAvailable;

public class ShortCircuitHybridExecutor extends ShortCircuitExecutor {

    private static final Logger LOG = LogManager.getLogger(ShortCircuitHybridExecutor.class);

    public ShortCircuitHybridExecutor(ConnectContext context, PlanFragment planFragment,
                                      List<TScanRangeLocations> scanRangeLocations, TDescriptorTable tDescriptorTable,
                                      boolean isBinaryRow, boolean enableProfile, String protocol,
                                      WorkerProvider workerProvider) {
        super(context, planFragment, scanRangeLocations, tDescriptorTable, isBinaryRow, enableProfile,
                protocol, workerProvider);
    }

    @Override
    public void exec() throws StarRocksException {
        if (result != null) {
            return;
        }
        Stopwatch watch = Stopwatch.createUnstarted();
        SetMultimap<TNetworkAddress, TExecShortCircuitParams> be2ShortCircuitRequests = createRequests();
        Queue<RowBatch> rowBatchQueue = new LinkedList<>();
        AtomicReference<RuntimeProfile> runtimeProfile = new AtomicReference<>();
        AtomicLong affectedRows = new AtomicLong();

        // all data will be pruned by fe
        if (be2ShortCircuitRequests.keys().size() == 0) {
            rowBatchQueue.offer(new RowBatch());
            result = new ShortCircuitResult(rowBatchQueue, affectedRows.get(), runtimeProfile.get());
            return;
        }

        AtomicInteger i = new AtomicInteger();
        MetricRepo.COUNTER_SHORTCIRCUIT_QUERY.increase(1L);
        MetricRepo.COUNTER_SHORTCIRCUIT_RPC.increase((long) be2ShortCircuitRequests.size());
        be2ShortCircuitRequests.forEach((beAddress, tRequest) -> {
            PBackendService service = BrpcProxy.getBackendService(beAddress);
            try {
                PExecShortCircuitRequest pRequest = new PExecShortCircuitRequest();
                pRequest.setAttachmentProtocol(protocol);
                pRequest.setRequest(tRequest, protocol);
                watch.start();
                Future<PExecShortCircuitResult> future = service.execShortCircuit(pRequest);
                if (null == future) {
                    return;
                }
                PExecShortCircuitResult shortCircuitResult = future.get(context.getExecTimeout(), TimeUnit.SECONDS);
                watch.stop();
                long t = watch.elapsed().toMillis();
                MetricRepo.HISTO_SHORTCIRCUIT_RPC_LATENCY.update(t);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("exec short circuit time: " + t + "ms.");
                }

                TStatusCode code = TStatusCode.findByValue(shortCircuitResult.status.statusCode);
                if (shortCircuitResult.status.errorMsgs != null && !shortCircuitResult.status.errorMsgs.isEmpty()) {
                    throw new RuntimeException(shortCircuitResult.status.errorMsgs.get(0) + "code: " + code +
                            " backend: " + beAddress.getHostname() + ", port:" + beAddress.getPort());
                }

                if (null != shortCircuitResult.affectedRows) {
                    affectedRows.getAndAdd(shortCircuitResult.affectedRows);
                }

                byte[] serialResult = pRequest.getSerializedResult();
                RowBatch rowBatch = new RowBatch();
                rowBatch.setEos(i.incrementAndGet() == be2ShortCircuitRequests.keys().size());
                if (serialResult != null && serialResult.length > 0) {
                    TDeserializer deserializer = ConfigurableSerDesFactory.getTDeserializer();
                    TResultBatch resultBatch = new TResultBatch();
                    deserializer.deserialize(resultBatch, serialResult);
                    rowBatch.setBatch(resultBatch);
                }
                rowBatchQueue.offer(rowBatch);

                if (shortCircuitResult.profile != null) {
                    TDeserializer deserializer = ConfigurableSerDesFactory.getTDeserializer();
                    TRuntimeProfileTree runtimeProfileTree = new TRuntimeProfileTree();
                    deserializer.deserialize(runtimeProfileTree, shortCircuitResult.profile);
                    RuntimeProfile beProfile = new RuntimeProfile(beAddress.toString());
                    beProfile.update(runtimeProfileTree);
                    if (enableProfile) {
                        perBeExecutionProfile.put(beAddress.toString(), beProfile);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable e) {
                throw new RuntimeException("Execute short circuit failed, reason: "
                        + e.getMessage() + " backend: " + beAddress.getHostname() + ", port:" + beAddress.getPort(), e);
            }

        });

        result = new ShortCircuitResult(rowBatchQueue, affectedRows.get(), runtimeProfile.get());
    }

    public class TabletWithVersion {
        final long tabletId;
        final String version;

        public TabletWithVersion(long tabletId, String version) {
            this.tabletId = tabletId;
            this.version = version;
        }

        public long getTabletId() {
            return tabletId;
        }

        public String getVersion() {
            return version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TabletWithVersion that = (TabletWithVersion) o;
            return tabletId == that.tabletId && Objects.equal(version, that.version);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(tabletId, version);
        }
    }

    /**
     * compute all tablets per be
     *
     * @return
     */
    private SetMultimap<TNetworkAddress, TabletWithVersion> assignTablet2Backends() throws NonRecoverableException {
        SetMultimap<TNetworkAddress, TabletWithVersion> backend2Tablets = HashMultimap.create();
        ImmutableMap<Long, Backend> idToBackends =
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend();
        Map<Long, Backend> aliveIdToBackends = idToBackends.entrySet().stream()
                .filter(be -> isWorkerAvailable(be.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        for (TScanRangeLocations range : scanRangeLocations) {
            TInternalScanRange internalScanRange = range.getScan_range().getInternal_scan_range();
            Set<Long> scanBackendIds =
                    range.getLocations().stream().map(TScanRangeLocation::getBackend_id).collect(Collectors.toSet());
            TabletWithVersion tabletWithVersion = new TabletWithVersion(internalScanRange.getTablet_id(),
                    internalScanRange.getVersion());

            Optional<Backend> be = pick(scanBackendIds, aliveIdToBackends);
            if (be.isEmpty()) {
                workerProvider.reportWorkerNotFoundException();
            }
            be.ifPresent(backend -> backend2Tablets.put(be.get().getBrpcAddress(), tabletWithVersion));
        }
        return backend2Tablets;
    }

    private SetMultimap<TNetworkAddress, TExecShortCircuitParams> createRequests() throws StarRocksException {
        SetMultimap<TNetworkAddress, TExecShortCircuitParams> toSendRequests = HashMultimap.create();
        Optional<PlanNode> planNode = getOlapScanNode();
        if (planNode.isEmpty()) {
            return toSendRequests;
        }
        OlapScanNode olapScanNode = ((OlapScanNode) planNode.get());
        // set literal exprs
        List<List<LiteralExpr>> keyTuples = olapScanNode.getRowStoreKeyLiterals();
        List<TKeyLiteralExpr> keyLiteralExprs = keyTuples.stream().map(keyTuple -> {
            TKeyLiteralExpr keyLiteralExpr = new TKeyLiteralExpr();
            keyLiteralExpr.setLiteral_exprs(keyTuple.stream()
                    .map(Expr::treeToThrift)
                    .collect(Collectors.toList()));
            return keyLiteralExpr;
        }).collect(Collectors.toList());

        // fill tablet id and version , then bind be network
        SetMultimap<TNetworkAddress, TabletWithVersion> be2Tablets = assignTablet2Backends();

        olapScanNode.clearScanNodeForThriftBuild();
        be2Tablets.forEach((be, tableVersion) -> {
            TExecShortCircuitParams commonRequest = new TExecShortCircuitParams();
            commonRequest.setDesc_tbl(tDescriptorTable);
            commonRequest.setOutput_exprs(planFragment.getOutputExprs().stream()
                    .map(Expr::treeToThrift).collect(Collectors.toList()));
            commonRequest.setIs_binary_row(isBinaryRow);
            commonRequest.setEnable_profile(enableProfile);
            if (planFragment.getSink() != null) {
                commonRequest.setData_sink(planFragment.sinkToThrift());
            }
            commonRequest.setKey_literal_exprs(keyLiteralExprs);

            List<Long> tabletIds = be2Tablets.get(be).stream().map(TabletWithVersion::getTabletId)
                    .collect(Collectors.toList());
            commonRequest.setTablet_ids(tabletIds);
            List<String> versions = be2Tablets.get(be).stream().map(TabletWithVersion::getVersion)
                    .collect(Collectors.toList());
            commonRequest.setVersions(versions);
            commonRequest.setPlan(planFragment.getPlanRoot().treeToThrift());
            toSendRequests.put(be, commonRequest);
        });

        return toSendRequests;
    }

    private Optional<PlanNode> getOlapScanNode() {
        return Optional.ofNullable(planFragment.getPlanRoot()).map(rootNode -> {
            if (rootNode instanceof OlapScanNode) {
                return rootNode;
            } else if (rootNode instanceof ProjectNode && rootNode.getChildren().size() == 1 &&
                    rootNode.getChild(0) instanceof OlapScanNode) {
                return rootNode.getChild(0);
            } else {
                return null;
            }
        });
    }

}