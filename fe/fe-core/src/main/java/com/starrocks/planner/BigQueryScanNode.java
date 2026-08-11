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
import com.google.common.base.Preconditions;
import com.starrocks.catalog.BigQueryTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.bigquery.BigQueryRemoteFileDesc;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCloudType;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Physical scan node for BigQuery external catalog tables and views.
 * Scan ranges correspond to BigQuery Storage Read API streams.
 */
public class BigQueryScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(BigQueryScanNode.class);

    private final BigQueryTable table;
    private CloudConfiguration cloudConfiguration;
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();
    private final List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();

    public BigQueryScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        table = (BigQueryTable) desc.getTable();
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

    @SuppressWarnings("unchecked")
    public void setupScanRangeLocations(TupleDescriptor tupleDescriptor, ScalarOperator predicate,
                                        List<PartitionKey> partitionKeys) {
        List<String> fieldNames = tupleDescriptor.getSlots().stream()
                .map(s -> s.getColumn().getName())
                .collect(Collectors.toList());

        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setPartitionKeys(partitionKeys)
                .setPredicate(predicate)
                .setFieldNames(fieldNames)
                .build();

        List<RemoteFileInfo> fileInfos = GlobalStateMgr.getCurrentState()
                .getMetadataMgr().getRemoteFiles(table, params);

        if (fileInfos == null || fileInfos.isEmpty()) {
            LOG.warn("No BigQuery streams returned for {}.{}", table.getCatalogDBName(), table.getCatalogTableName());
            return;
        }

        RemoteFileInfo remoteFileInfo = fileInfos.get(0);
        // The common params map (project_id, credentials_base64, etc.) is stored in the attachment.
        Map<String, String> commonParams = remoteFileInfo.getAttachment() != null
                ? (Map<String, String>) remoteFileInfo.getAttachment()
                : new HashMap<>();

        List<RemoteFileDesc> fileDescs = remoteFileInfo.getFiles();
        if (fileDescs == null || fileDescs.isEmpty()) {
            LOG.warn("BigQuery read session has 0 streams for {}.{}", table.getCatalogDBName(),
                    table.getCatalogTableName());
            return;
        }

        for (RemoteFileDesc desc : fileDescs) {
            BigQueryRemoteFileDesc bqDesc = (BigQueryRemoteFileDesc) desc;
            TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
            THdfsScanRange hdfsScanRange = new THdfsScanRange();

            Map<String, String> splitInfo = new HashMap<>(commonParams);
            splitInfo.put("read_session_name", bqDesc.getReadSessionName());
            splitInfo.put("read_stream_name", bqDesc.getReadStreamName());
            splitInfo.put("stream_index", String.valueOf(bqDesc.getStreamIndex()));

            hdfsScanRange.setBigquery_split_infos(splitInfo);
            hdfsScanRange.setUse_bigquery_jni_reader(true);
            // Set a nominal length so the BE load balancer has a non-zero value.
            hdfsScanRange.setFile_length(1);
            hdfsScanRange.setLength(1);

            TScanRange scanRange = new TScanRange();
            scanRange.setHdfs_scan_range(hdfsScanRange);
            scanRangeLocations.setScan_range(scanRange);

            com.starrocks.thrift.TScanRangeLocation location =
                    new com.starrocks.thrift.TScanRangeLocation(
                            new com.starrocks.thrift.TNetworkAddress("-1", -1));
            scanRangeLocations.addToLocations(location);
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
        helper.addValue("bigqueryTable=" + table.getName());
        return helper.toString();
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ")
                .append(table.getCatalogDBName()).append(".").append(table.getCatalogTableName())
                .append("\n");
        if (table.isView()) {
            output.append(prefix).append("  (BigQuery VIEW — materialised at query time)\n");
        }
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

        String explainString = getExplainString(conjuncts);
        LOG.info("BigQuery scan explain: {}", explainString);
        tHdfsScanNode.setSql_predicates(explainString);

        if (table != null) {
            tHdfsScanNode.setTable_name(table.getCatalogTableName());
        }
        HdfsScanNode.setScanOptimizeOptionToThrift(tHdfsScanNode, this);

        TCloudConfiguration tCloudConfiguration = new TCloudConfiguration();
        cloudConfiguration.toThrift(tCloudConfiguration);
        tCloudConfiguration.setCloud_type(TCloudType.GCP);
        tHdfsScanNode.setCloud_configuration(tCloudConfiguration);

        msg.hdfs_scan_node = tHdfsScanNode;
        setConnectorCatalogType(msg);
    }
}
