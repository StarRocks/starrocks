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

import com.aliyun.odps.table.read.split.InputSplitWithRowRange;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.OdpsTable;
import com.starrocks.common.UserException;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * full scan on JDBC table.
 */
public class OdpsScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(OdpsScanNode.class);
    private OdpsTable table;
    private CloudConfiguration cloudConfiguration = null;
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();

    private final List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();

    public OdpsScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        table = (OdpsTable) desc.getTable();
        setupCloudCredential();
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    private void setupCloudCredential() {
        String catalog = table.getCatalogName();
        if (catalog == null) {
            return;
        }
        CatalogConnector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalog);
        Preconditions.checkState(connector != null,
                String.format("connector of catalog %s should not be null", catalog));
        cloudConfiguration = connector.getMetadata().getCloudConfiguration();
        Preconditions.checkState(cloudConfiguration != null,
                String.format("cloudConfiguration of catalog %s should not be null", catalog));
    }

    public void setupScanRangeLocations(TupleDescriptor tupleDescriptor, ScalarOperator predicate) {
        List<String> fieldNames =
                tupleDescriptor.getSlots().stream().map(s -> s.getColumn().getName()).collect(Collectors.toList());
        List<RemoteFileInfo> fileInfos = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFileInfos(
                table.getCatalogName(), table, null, -1, predicate, fieldNames, -1);
        RemoteFileDesc remoteFileDesc = fileInfos.get(0).getFiles().get(0);
        List<InputSplitWithRowRange> splits = remoteFileDesc.getOdpsSplitsInfo();
        if (splits.isEmpty()) {
            LOG.warn("There is no odps splits on {}.{} and predicate: [{}]",
                    table.getDbName(), table.getTableName(), predicate);
            return;
        }
        for (InputSplitWithRowRange split : splits) {
            TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

            THdfsScanRange hdfsScanRange = new THdfsScanRange();
            hdfsScanRange.setRelative_path(split.getSessionId());
            hdfsScanRange.setOffset(split.getRowRange().getStartIndex());
            hdfsScanRange.setLength(split.getRowRange().getNumRecord());
            hdfsScanRange.setUse_odps_jni_reader(true);
            hdfsScanRange.setFile_length(split.getRowRange().getNumRecord());
            TScanRange scanRange = new TScanRange();
            scanRange.setHdfs_scan_range(hdfsScanRange);
            scanRangeLocations.setScan_range(scanRange);

            com.starrocks.thrift.TScanRangeLocation
                    scanRangeLocation =
                    new com.starrocks.thrift.TScanRangeLocation(new com.starrocks.thrift.TNetworkAddress("-1", -1));
            scanRangeLocations.addToLocations(scanRangeLocation);

            scanRangeLocationsList.add(scanRangeLocations);
        }
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocationsList;
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.addValue("odpsTable=" + table.getName());
        return helper.toString();
    }

    @Override
    public void finalizeStats(Analyzer analyzer) throws UserException {
        computeStats(analyzer);
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(table.getDbName()).append(".").append(table.getTableName())
                .append("\n");
        return output.toString();
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
        THdfsScanNode tHdfsScanNode = new THdfsScanNode();
        tHdfsScanNode.setTuple_id(desc.getId().asInt());
        tHdfsScanNode.setCan_use_min_max_count_opt(false);

        String explainString = getExplainString(conjuncts);
        LOG.info("Explain string: " + explainString);
        tHdfsScanNode.setSql_predicates(explainString);

        if (table != null) {
            tHdfsScanNode.setTable_name(table.getTableName());
        }
        HdfsScanNode.setScanOptimizeOptionToThrift(tHdfsScanNode, this);
        HdfsScanNode.setCloudConfigurationToThrift(tHdfsScanNode, cloudConfiguration);
        msg.hdfs_scan_node = tHdfsScanNode;
    }

    @Override
    public int getNumInstances() {
        return scanRangeLocationsList.size();
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        super.computeStats(analyzer);
    }

}
