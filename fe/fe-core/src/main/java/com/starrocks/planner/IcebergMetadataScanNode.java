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

import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.common.UserException;
import com.starrocks.connector.RemoteMetaSplit;
import com.starrocks.connector.iceberg.IcebergMetaSpec;
import com.starrocks.connector.metadata.iceberg.LogicalIcebergMetadataTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.hadoop.shaded.com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class IcebergMetadataScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(IcebergMetadataScanNode.class);
    private final LogicalIcebergMetadataTable table;
    private String icebergPredicate = "";

    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();
    private final List<TScanRangeLocations> result = new ArrayList<>();
    private String temporalClause;
    private String serializedTable;
    private boolean loadColumnStats;

    public IcebergMetadataScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName, String temporalClause) {
        super(id, desc, planNodeName);
        this.table = (LogicalIcebergMetadataTable) desc.getTable();
        this.temporalClause = temporalClause;
    }

    public void preProcessIcebergPredicate(String icebergPredicate) {
        this.icebergPredicate = icebergPredicate;
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return result;
    }

    public void setupScanRangeLocations() throws UserException {
        String catalogName = table.getCatalogName();
        String originDbName = table.getOriginDb();
        String originTableName = table.getOriginTable();

        long snapshotId = -1;
        // TODO(stephen): parse version by AstBuilder
        if (!Strings.isNullOrEmpty(temporalClause)) {
            String kw = "for version as of";
            temporalClause = temporalClause.substring(kw.length()).trim();
            snapshotId = Long.parseLong(temporalClause);
        }


        IcebergMetaSpec serializedMetaSpec = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getSerializedMetaSpec(catalogName, originDbName, originTableName, snapshotId, icebergPredicate).cast();

        this.serializedTable = serializedMetaSpec.getTable();
        this.loadColumnStats = serializedMetaSpec.loadColumnStats();
        serializedMetaSpec.getSplits().forEach(this::addSplitScanRangeLocations);
    }

    private void addSplitScanRangeLocations(RemoteMetaSplit split) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setUse_iceberg_jni_metadata_reader(true);

        hdfsScanRange.setSerialized_split(split.getSerializeSplit());
        hdfsScanRange.setFile_length(split.length());
        hdfsScanRange.setLength(split.length());

        // for distributed scheduler
        hdfsScanRange.setFull_path(split.path());
        hdfsScanRange.setOffset(0);

        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
        scanRangeLocations.addToLocations(scanRangeLocation);

        result.add(scanRangeLocations);
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

        tHdfsScanNode.setSerialized_table(serializedTable);
        tHdfsScanNode.setSerialized_predicate(icebergPredicate);
        tHdfsScanNode.setLoad_column_stats(loadColumnStats);

        msg.hdfs_scan_node = tHdfsScanNode;
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    @Override
    protected boolean supportTopNRuntimeFilter() {
        return true;
    }

}
