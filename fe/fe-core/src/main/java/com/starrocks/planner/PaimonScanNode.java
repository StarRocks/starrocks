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
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.paimon.PaimonConnector;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.InstantiationUtil;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.starrocks.thrift.TExplainLevel.VERBOSE;
import static java.nio.charset.StandardCharsets.UTF_8;

public class PaimonScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(PaimonScanNode.class);
    private final AtomicLong partitionIdGen = new AtomicLong(0L);
    private PaimonTable paimonTable;
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();
    private final List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();
    private CloudConfiguration cloudConfiguration = null;

    public PaimonScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        paimonTable = (PaimonTable) desc.getTable();
        setupCloudCredential();
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    public PaimonTable getPaimonTable() {
        return paimonTable;
    }

    private void setupCloudCredential() {
        String catalog = paimonTable.getCatalogName();
        if (catalog == null) {
            return;
        }
        PaimonConnector connector = (PaimonConnector) GlobalStateMgr.getCurrentState().getConnectorMgr().
                getConnector(catalog);
        if (connector != null) {
            cloudConfiguration = connector.getCloudConfiguration();
        }
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.addValue("paimonTable=" + paimonTable.getName());
        return helper.toString();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocationsList;
    }

    public void setupScanRangeLocations(DescriptorTable descTbl) {
        ReadBuilder readBuilder = paimonTable.getNativeTable().newReadBuilder();
        // TODO(wangriyu): readBuilder::withFilter
        List<Split> splits = readBuilder.newScan().plan().splits();
        for (Split split : splits) {
            addScanRangeLocations(split);
        }
    }

    private void addScanRangeLocations(Split split) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setUse_paimon_jni_reader(true);
        hdfsScanRange.setPaimon_split_info(encodeObjectToString(split));
        // TODO(wangriyu): file length
        hdfsScanRange.setFile_length(-1);

        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
        scanRangeLocations.addToLocations(scanRangeLocation);

        scanRangeLocationsList.add(scanRangeLocations);
    }

    private long nextPartitionId() {
        return partitionIdGen.getAndIncrement();
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(paimonTable.getName()).append("\n");

        if (null != sortColumn) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }
        if (!scanNodePredicates.getPartitionConjuncts().isEmpty()) {
            output.append(prefix).append("PARTITION PREDICATES: ").append(
                    getExplainString(scanNodePredicates.getPartitionConjuncts())).append("\n");
        }
        if (!scanNodePredicates.getNonPartitionConjuncts().isEmpty()) {
            output.append(prefix).append("NON-PARTITION PREDICATES: ").append(
                    getExplainString(scanNodePredicates.getNonPartitionConjuncts())).append("\n");
        }
        if (!scanNodePredicates.getNoEvalPartitionConjuncts().isEmpty()) {
            output.append(prefix).append("NO EVAL-PARTITION PREDICATES: ").append(
                    getExplainString(scanNodePredicates.getNoEvalPartitionConjuncts())).append("\n");
        }
        if (!scanNodePredicates.getMinMaxConjuncts().isEmpty()) {
            output.append(prefix).append("MIN/MAX PREDICATES: ").append(
                    getExplainString(scanNodePredicates.getMinMaxConjuncts())).append("\n");
        }

        List<String> partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                paimonTable.getCatalogName(), paimonTable.getDbName(), paimonTable.getTableName());

        output.append(prefix).append(
                String.format("partitions=%s/%s", scanNodePredicates.getSelectedPartitionIds().size(),
                        partitionNames.size() == 0 ? 1 : partitionNames.size()));
        output.append("\n");

        // TODO: support it in verbose
        if (detailLevel != VERBOSE) {
            output.append(prefix).append(String.format("cardinality=%s", cardinality));
            output.append("\n");
        }

        output.append(prefix).append(String.format("avgRowSize=%s", avgRowSize));
        output.append("\n");

        output.append(prefix).append(String.format("numNodes=%s", numNodes));
        output.append("\n");

        if (detailLevel == TExplainLevel.VERBOSE) {
            for (SlotDescriptor slotDescriptor : desc.getSlots()) {
                Type type = slotDescriptor.getOriginType();
                if (type.isComplexType()) {
                    output.append(prefix).append(String.format("Pruned type: %d <-> [%s]\n", slotDescriptor.getId().asInt(), type));
                }
            }
        }

        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return scanRangeLocationsList.size();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
        THdfsScanNode tHdfsScanNode = new THdfsScanNode();
        tHdfsScanNode.setTuple_id(desc.getId().asInt());
        msg.hdfs_scan_node = tHdfsScanNode;

        List<Expr> noEvalPartitionConjuncts = scanNodePredicates.getNoEvalPartitionConjuncts();
        String partitionSqlPredicate = getExplainString(noEvalPartitionConjuncts);
        for (Expr expr : noEvalPartitionConjuncts) {
            msg.hdfs_scan_node.addToPartition_conjuncts(expr.treeToThrift());
        }
        msg.hdfs_scan_node.setPartition_sql_predicates(partitionSqlPredicate);

        // put non-partition conjuncts into conjuncts
        if (msg.isSetConjuncts()) {
            msg.conjuncts.clear();
        }

        List<Expr> nonPartitionConjuncts = scanNodePredicates.getNonPartitionConjuncts();
        for (Expr expr : nonPartitionConjuncts) {
            msg.addToConjuncts(expr.treeToThrift());
        }
        String sqlPredicate = getExplainString(nonPartitionConjuncts);
        msg.hdfs_scan_node.setSql_predicates(sqlPredicate);

        List<Expr> minMaxConjuncts = scanNodePredicates.getMinMaxConjuncts();
        if (!minMaxConjuncts.isEmpty()) {
            String minMaxSqlPredicate = getExplainString(minMaxConjuncts);
            for (Expr expr : minMaxConjuncts) {
                msg.hdfs_scan_node.addToMin_max_conjuncts(expr.treeToThrift());
            }
            msg.hdfs_scan_node.setMin_max_tuple_id(scanNodePredicates.getMinMaxTuple().getId().asInt());
            msg.hdfs_scan_node.setMin_max_sql_predicates(minMaxSqlPredicate);
        }

        if (paimonTable != null) {
            msg.hdfs_scan_node.setTable_name(paimonTable.getName());
        }

        if (cloudConfiguration != null) {
            TCloudConfiguration tCloudConfiguration = new TCloudConfiguration();
            cloudConfiguration.toThrift(tCloudConfiguration);
            msg.hdfs_scan_node.setCloud_configuration(tCloudConfiguration);
        }
    }

    @Override
    public boolean canUsePipeLine() {
        return true;
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    private static final Base64.Encoder BASE64_ENCODER =
            java.util.Base64.getUrlEncoder().withoutPadding();

    public static <T> String encodeObjectToString(T t) {
        try {
            byte[] bytes = InstantiationUtil.serializeObject(t);
            return new String(BASE64_ENCODER.encode(bytes), UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
