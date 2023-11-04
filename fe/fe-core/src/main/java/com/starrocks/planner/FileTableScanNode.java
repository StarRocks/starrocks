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
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.FileTable;
import com.starrocks.catalog.Type;
import com.starrocks.connector.RemoteFileBlockDesc;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteScanRangeLocations;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

public class FileTableScanNode extends ScanNode {
    private final List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();
    private final FileTable fileTable;
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();
    private CloudConfiguration cloudConfiguration = null;

    public FileTableScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        this.fileTable = (FileTable) desc.getTable();
        setupCredential();
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    public FileTable getFileTable() {
        return fileTable;
    }

    private void setupCredential() {
        cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(fileTable.getFileProperties());
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.addValue("fileTable=" + fileTable.getName());
        return helper.toString();
    }

    public void setupScanRangeLocations() throws Exception {
        List<RemoteFileDesc> files = fileTable.getFileDescs();
        for (RemoteFileDesc file : files) {
            addScanRangeLocations(file);
        }
    }

    private void addScanRangeLocations(RemoteFileDesc file) {
        for (RemoteFileBlockDesc blockDesc : file.getBlockDescs()) {
            TScanRangeLocations scanRangeLocs = new TScanRangeLocations();

            THdfsScanRange hdfsScanRange = new THdfsScanRange();
            hdfsScanRange.setRelative_path(file.getFileName());
            if (fileTable.getTableLocation().endsWith("/")) {
                hdfsScanRange.setFull_path(fileTable.getTableLocation() + file.getFileName());
            } else {
                hdfsScanRange.setFull_path(fileTable.getTableLocation());
            }
            hdfsScanRange.setOffset(blockDesc.getOffset());
            hdfsScanRange.setLength(blockDesc.getLength());
            hdfsScanRange.setFile_length(file.getLength());
            hdfsScanRange.setModification_time(file.getModificationTime());
            hdfsScanRange.setFile_format(fileTable.getFileFormat().toThrift());
            if (RemoteScanRangeLocations.isTextFormat(hdfsScanRange.getFile_format())) {
                hdfsScanRange.setText_file_desc(file.getTextFileFormatDesc().toThrift());
            }

            TScanRange scanRange = new TScanRange();
            scanRange.setHdfs_scan_range(hdfsScanRange);
            scanRangeLocs.setScan_range(scanRange);

            if (blockDesc.getReplicaHostIds().length == 0) {
                String message = String.format("hdfs file block has no host. file = %s/%s",
                        fileTable.getTableLocation(), file.getFileName());
                throw new StarRocksPlannerException(message, ErrorType.INTERNAL_ERROR);
            }
            for (long hostId : blockDesc.getReplicaHostIds()) {
                String host = blockDesc.getDataNodeIp(hostId);
                TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress(host, -1));
                scanRangeLocs.addToLocations(scanRangeLocation);
            }
            scanRangeLocationsList.add(scanRangeLocs);
        }
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocationsList;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(fileTable.getName()).append("\n");

        if (!scanNodePredicates.getNonPartitionConjuncts().isEmpty()) {
            output.append(prefix).append("NON-PARTITION PREDICATES: ").append(
                    getExplainString(scanNodePredicates.getNonPartitionConjuncts())).append("\n");
        }
        if (!scanNodePredicates.getMinMaxConjuncts().isEmpty()) {
            output.append(prefix).append("MIN/MAX PREDICATES: ").append(
                    getExplainString(scanNodePredicates.getMinMaxConjuncts())).append("\n");
        }

        output.append(prefix).append(String.format("cardinality=%s", cardinality));
        output.append("\n");

        output.append(prefix).append(String.format("avgRowSize=%s", avgRowSize));
        output.append("\n");

        if (detailLevel == TExplainLevel.VERBOSE) {
            for (SlotDescriptor slotDescriptor : desc.getSlots()) {
                Type type = slotDescriptor.getOriginType();
                if (type.isComplexType()) {
                    output.append(prefix)
                            .append(String.format("Pruned type: %d <-> [%s]\n", slotDescriptor.getId().asInt(), type));
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

        if (fileTable != null) {
            // don't set column_name so that the BE will get the column by name not by position
            msg.hdfs_scan_node.setTable_name(fileTable.getName());
        }

        if (CollectionUtils.isNotEmpty(columnAccessPaths)) {
            msg.hdfs_scan_node.setColumn_access_paths(columnAccessPathToThrift());
        }

        HdfsScanNode.setScanOptimizeOptionToThrift(tHdfsScanNode, this);
        HdfsScanNode.setCloudConfigurationToThrift(tHdfsScanNode, cloudConfiguration);
        HdfsScanNode.setMinMaxConjunctsToThrift(tHdfsScanNode, this, this.getScanNodePredicates());
        HdfsScanNode.setNonPartitionConjunctsToThrift(msg, this, this.getScanNodePredicates());
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }
}
