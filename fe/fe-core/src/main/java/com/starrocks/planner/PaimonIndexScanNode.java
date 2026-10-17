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

import com.starrocks.catalog.PaimonTable;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.index.ConnectorIndexShard;
import com.starrocks.connector.index.IndexTable;
import com.starrocks.connector.paimon.PaimonGlobalIndexRequest;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.THdfsFileFormat;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TPaimonGlobalIndexScanRange;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.paimon.table.FileStoreTable;

import java.util.ArrayList;
import java.util.List;

/** Scan node that evaluates one Paimon Global Index row-id range per scan range. */
public final class PaimonIndexScanNode extends ScanNode {
    private final IndexTable indexTable;
    private final List<TScanRangeLocations> scanRanges = new ArrayList<>();
    private final CloudConfiguration cloudConfiguration;
    private String tablePath;
    private long snapshotId;

    public PaimonIndexScanNode(PlanNodeId id, TupleDescriptor descriptor, IndexTable indexTable) {
        super(id, descriptor, "PaimonIndexScanNode");
        this.indexTable = indexTable;
        CatalogConnector connector = GlobalStateMgr.getCurrentState().getConnectorMgr()
                .getConnector(indexTable.getInnerTable().getCatalogName());
        this.cloudConfiguration = connector == null ? null : connector.getMetadata().getCloudConfiguration();
    }

    /** Builds request-bound ranges and returns result predicates for an upper SelectNode. */
    public List<ScalarOperator> setupScanRangeLocations(ScalarOperator predicate) {
        PaimonGlobalIndexRequest request = PaimonGlobalIndexRequest.parseTransport(extractRequest(predicate));
        String queryJson = request.toJson();
        snapshotId = request.getSnapshotId();

        PaimonTable table = (PaimonTable) indexTable.getInnerTable();
        if (!(table.getNativeTable() instanceof FileStoreTable)) {
            throw new IllegalStateException("Paimon Global Index requires a FileStoreTable");
        }
        FileStoreTable nativeTable = (FileStoreTable) table.getNativeTable();
        tablePath = nativeTable.location().toString();
        if (!tablePath.equals(request.getTablePath()) || !"main".equals(request.getBranch())) {
            throw new IllegalArgumentException("Paimon Global Index request targets a different table or branch");
        }
        if (!request.matchesSchema(nativeTable.schema().id(), nativeTable.schema().timeMillis())) {
            throw new IllegalArgumentException("Paimon Global Index request targets a different schema");
        }

        ConnectorMetadata metadata = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getOptionalMetadata(table.getCatalogName())
                .orElseThrow(() -> new IllegalStateException("Missing connector metadata for "
                        + table.getCatalogName()));
        List<ConnectorIndexShard> shards = metadata.getIndexShards(
                table, snapshotId, request.getRequiredIndexes());
        if (shards.isEmpty()) {
            throw new IllegalStateException("Paimon Global Index does not completely cover snapshot " + snapshotId);
        }

        for (int shardId = 0; shardId < shards.size(); shardId++) {
            ConnectorIndexShard shard = shards.get(shardId);
            scanRanges.add(createScanRange(shardId, shard, queryJson, tablePath, snapshotId));
        }
        // args is an input binding, not a filter over the raw JSON emitted by the scanner.
        // The specialized scanner does not evaluate conjuncts over its index_result output.
        List<ScalarOperator> residualPredicates = new ArrayList<>(Utils.extractConjuncts(predicate));
        residualPredicates.removeIf(PaimonIndexScanNode::referencesArgs);
        return residualPredicates;
    }

    public String getTablePath() {
        return tablePath;
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRanges;
    }

    @Override
    protected void toThrift(TPlanNode message) {
        message.setNode_type(TPlanNodeType.HDFS_SCAN_NODE);
        THdfsScanNode scanNode = new THdfsScanNode();
        scanNode.setTuple_id(desc.getId().asInt());
        if (cloudConfiguration != null) {
            HdfsScanNode.setCloudConfigurationToThrift(scanNode, cloudConfiguration);
        }
        message.setHdfs_scan_node(scanNode);
    }

    @Override
    protected String getNodeExplainString(String prefix, com.starrocks.thrift.TExplainLevel detailLevel) {
        return prefix + "TABLE: " + indexTable.getName() + "\n"
                + prefix + "SNAPSHOT: " + snapshotId + "\n"
                + prefix + "SHARDS: " + scanRanges.size() + "\n";
    }

    private static TScanRangeLocations createScanRange(
            int shardId, ConnectorIndexShard shard, String queryJson, String tablePath, long snapshotId) {
        TPaimonGlobalIndexScanRange request = new TPaimonGlobalIndexScanRange();
        request.setProtocol_version(PaimonGlobalIndexRequest.CURRENT_VERSION);
        request.setShard_id(shardId);
        request.setRange_from(shard.getFrom());
        request.setRange_to(shard.getTo());
        request.setQuery_json(queryJson);
        request.setTable_path(tablePath);
        request.setSnapshot_id(snapshotId);

        THdfsScanRange hdfs = new THdfsScanRange();
        hdfs.setPaimon_global_index_scan_range(request);
        hdfs.setFull_path(tablePath + "/#global-index/" + shardId);
        hdfs.setOffset(0);
        long rangeLength = shard.getTo() == Long.MAX_VALUE
                ? Long.MAX_VALUE : shard.getTo() - shard.getFrom() + 1;
        hdfs.setLength(rangeLength);
        hdfs.setFile_length(rangeLength);
        hdfs.setFile_format(THdfsFileFormat.UNKNOWN);

        TScanRange range = new TScanRange();
        range.setHdfs_scan_range(hdfs);
        TScanRangeLocations locations = new TScanRangeLocations();
        locations.setScan_range(range);
        locations.setLocations(new ArrayList<>());
        return locations;
    }

    static String extractRequest(ScalarOperator predicate) {
        String request = null;
        for (ScalarOperator conjunct : Utils.extractConjuncts(predicate)) {
            if (!referencesArgs(conjunct)) {
                continue;
            }
            if (request != null || !(conjunct instanceof BinaryPredicateOperator)
                    || ((BinaryPredicateOperator) conjunct).getBinaryType() != BinaryType.EQ) {
                throw invalidArgsPredicate();
            }
            ScalarOperator left = conjunct.getChild(0);
            ScalarOperator right = conjunct.getChild(1);
            if (isArgsColumn(left)
                    && right instanceof ConstantOperator && !((ConstantOperator) right).isNull()
                    && right.getType().isStringType()) {
                request = String.valueOf(((ConstantOperator) right).getValue());
            } else if (isArgsColumn(right)
                    && left instanceof ConstantOperator && !((ConstantOperator) left).isNull()
                    && left.getType().isStringType()) {
                request = String.valueOf(((ConstantOperator) left).getValue());
            } else {
                throw invalidArgsPredicate();
            }
        }
        if (request == null || request.isEmpty()) {
            throw invalidArgsPredicate();
        }
        return request;
    }

    private static boolean referencesArgs(ScalarOperator predicate) {
        return Utils.extractColumnRef(predicate).stream().anyMatch(PaimonIndexScanNode::isArgsColumn);
    }

    private static boolean isArgsColumn(ScalarOperator operator) {
        return operator instanceof ColumnRefOperator
                && IndexTable.ARGS_COLUMN_NAME.equalsIgnoreCase(((ColumnRefOperator) operator).getName());
    }

    private static IllegalArgumentException invalidArgsPredicate() {
        return new IllegalArgumentException("Paimon Global Index scan requires exactly one non-empty string "
                + "args = '<versioned request>' binding and no other predicates on args");
    }
}
