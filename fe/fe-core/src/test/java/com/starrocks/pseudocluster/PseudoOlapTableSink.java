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

package com.starrocks.pseudocluster;

import com.starrocks.proto.PTabletWithPartition;
import com.starrocks.proto.PTabletWriterAddBatchResult;
import com.starrocks.proto.PTabletWriterAddChunkRequest;
import com.starrocks.proto.PTabletWriterCancelRequest;
import com.starrocks.proto.PTabletWriterOpenRequest;
import com.starrocks.proto.PTabletWriterOpenResult;
import com.starrocks.proto.PUniqueId;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TOlapTableSink;
import com.starrocks.thrift.TTabletCommitInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PseudoOlapTableSink {
    private static final Logger LOG = LogManager.getLogger(PseudoOlapTableSink.class);

    static class PartitionTablet {
        long partitionId;
        long tabletId;

        PartitionTablet(long partitionId, long tabletId) {
            this.partitionId = partitionId;
            this.tabletId = tabletId;
        }
    }

    static class Partition {
        long id;
        Map<Long, List<Long>> indexToTabletIds;
    }

    class NodeChannel {
        long indexId;
        long nodeId;
        List<PartitionTablet> tablets = new ArrayList<>();
        String errMsg = null;

        NodeChannel(long indexId, long nodeId) {
            this.indexId = indexId;
            this.nodeId = nodeId;
        }

        public String getErrMsg() {
            return errMsg;
        }

        boolean open() {
            PTabletWriterOpenRequest request = new PTabletWriterOpenRequest();
            request.id = loadId;
            request.indexId = indexId;
            request.txnId = txnId;
            request.isLakeTablet = false;
            request.tablets = tablets.stream().map(pt -> {
                PTabletWithPartition tablet = new PTabletWithPartition();
                tablet.partitionId = pt.partitionId;
                tablet.tabletId = pt.tabletId;
                return tablet;
            }).collect(Collectors.toList());
            request.isVectorized = true;
            PTabletWriterOpenResult result = cluster.getBackend(nodeId).tabletWriterOpen(request);
            if (result.status.statusCode != 0) {
                if (errMsg == null) {
                    errMsg = result.status.errorMsgs.get(0);
                }
                LOG.warn("open tablet writer failed: node:{} {}", nodeId, errMsg);
                return false;
            } else {
                return true;
            }
        }

        void cancel() {
            PTabletWriterCancelRequest request = new PTabletWriterCancelRequest();
            request.id = loadId;
            request.indexId = indexId;
            request.txnId = txnId;
            cluster.getBackend(nodeId).tabletWriterCancel(request);
        }

        boolean close() {
            PTabletWriterAddChunkRequest request = new PTabletWriterAddChunkRequest();
            request.id = loadId;
            request.indexId = indexId;
            request.txnId = txnId;
            request.partitionIds = partitionsWithData;
            request.eos = true;
            PTabletWriterAddBatchResult result = cluster.getBackend(nodeId).tabletWriterAddChunk(request);
            if (result.tabletVec != null) {
                result.tabletVec.forEach(tablet -> {
                    TTabletCommitInfo commitInfo = new TTabletCommitInfo();
                    commitInfo.backendId = nodeId;
                    commitInfo.tabletId = tablet.tabletId;
                    tabletCommitInfos.add(commitInfo);
                });
            }
            if (result.status.statusCode != 0) {
                if (errMsg == null) {
                    errMsg = result.status.errorMsgs.get(0);
                }
                LOG.warn("close tablet writer failed: node:{} {}", nodeId, result.status.errorMsgs.get(0));
                return false;
            } else {
                return true;
            }
        }
    }

    class IndexChannel {
        long indexId;
        // nodeId -> list of tablets with partition information
        Map<Long, NodeChannel> nodeChannels;
        Set<Long> errorNodes = new HashSet<>();
        String errMsg;

        private boolean has_intolerable_failure() {
            return errorNodes.size() >= (numReplica + 1) / 2;
        }

        String getErrorMessage() {
            return String.format("index:%d has intolerable failure: %s %s numReplica:%s ", indexId, errMsg, errorNodes,
                    numReplica);
        }

        boolean open() {
            for (NodeChannel ch : nodeChannels.values()) {
                if (!ch.open()) {
                    if (errMsg == null) {
                        errMsg = ch.getErrMsg();
                    }
                    errorNodes.add(ch.nodeId);
                    if (has_intolerable_failure()) {
                        return false;
                    }
                }
            }
            return true;
        }

        void cancel() {
            for (NodeChannel ch : nodeChannels.values()) {
                ch.cancel();
            }
        }

        boolean close() {
            for (NodeChannel ch : nodeChannels.values()) {
                if (!ch.close()) {
                    if (errMsg == null) {
                        errMsg = ch.getErrMsg();
                    }
                    errorNodes.add(ch.nodeId);
                    if (has_intolerable_failure()) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    PseudoCluster cluster;
    PUniqueId loadId;
    long txnId;
    int numReplica;
    List<Long> indexes;
    List<Partition> partitions;
    Map<Long, List<Long>> tabletIdToNodeIds;
    Map<Long, String> nodeIdToHost;

    List<IndexChannel> indexChannels;
    List<Long> partitionsWithData;

    List<TTabletCommitInfo> tabletCommitInfos = new ArrayList<>();

    public PseudoOlapTableSink(PseudoCluster cluster, TDataSink tDataSink) {
        this.cluster = cluster;
        TOlapTableSink tOlapTableSink = tDataSink.olap_table_sink;
        loadId = new PUniqueId();
        loadId.hi = tOlapTableSink.load_id.hi;
        loadId.lo = tOlapTableSink.load_id.lo;
        txnId = tOlapTableSink.txn_id;
        numReplica = tOlapTableSink.num_replicas;
        indexes = tOlapTableSink.schema.indexes.stream().map(i -> i.id).collect(Collectors.toList());
        partitions = tOlapTableSink.partition.partitions.stream().map(p -> {
            Partition partition = new Partition();
            partition.id = p.id;
            partition.indexToTabletIds = p.indexes.stream().collect(Collectors.toMap(e -> e.index_id, e -> e.tablets));
            return partition;
        }).collect(Collectors.toList());
        tabletIdToNodeIds = tOlapTableSink.location.tablets.stream().collect(Collectors.toMap(e -> e.tablet_id, e -> e.node_ids));
        nodeIdToHost = tOlapTableSink.nodes_info.nodes.stream().collect(Collectors.toMap(e -> e.id, e -> e.host));

        indexChannels = new ArrayList<>();
        for (long indexId : indexes) {
            IndexChannel indexChannel = new IndexChannel();
            indexChannel.indexId = indexId;
            indexChannel.nodeChannels = new HashMap<>();
            for (Partition partition : partitions) {
                partition.indexToTabletIds.get(indexId).forEach(tabletId -> {
                    List<Long> nodeIds = tabletIdToNodeIds.get(tabletId);
                    nodeIds.forEach(nodeId -> {
                        NodeChannel nodeChannel = indexChannel.nodeChannels.computeIfAbsent(
                                nodeId, k -> new NodeChannel(indexId, nodeId));
                        nodeChannel.tablets.add(new PartitionTablet(partition.id, tabletId));
                    });
                });
            }
            indexChannels.add(indexChannel);
        }

        partitionsWithData = partitions.stream().map(p -> p.id).collect(Collectors.toList());
    }

    public boolean open() {
        for (IndexChannel indexChannel : indexChannels) {
            if (!indexChannel.open()) {
                return false;
            }
        }
        return true;
    }

    public void cancel() {
        for (IndexChannel indexChannel : indexChannels) {
            indexChannel.cancel();
        }
    }

    public boolean close() {
        for (IndexChannel indexChannel : indexChannels) {
            if (!indexChannel.close()) {
                return false;
            }
        }
        return true;
    }

    public String getErrorMessage() {
        StringBuilder stringBuilder = new StringBuilder();
        for (IndexChannel indexChannel : indexChannels) {
            if (indexChannel.has_intolerable_failure()) {
                stringBuilder.append(indexChannel.getErrorMessage());
                stringBuilder.append("|");
            }
        }
        return stringBuilder.toString();
    }

    public List<TTabletCommitInfo> getTabletCommitInfos() {
        return tabletCommitInfos;
    }

    public Map<String, String> getLoadCounters() {
        Map result = new HashMap();
        int nrows = 1000;
        result.put("dpp.norm.ALL", Integer.toString(nrows));
        result.put("dpp.abnorm.ALL", Integer.toString(0));
        result.put("unselected.rows", Integer.toString(0));
        result.put("loaded.bytes", Integer.toString(nrows * 1000));
        return result;
    }
}
