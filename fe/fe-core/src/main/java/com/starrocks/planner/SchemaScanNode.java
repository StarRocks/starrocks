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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/SchemaScanNode.java

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

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TFrontend;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TSchemaScanNode;
import com.starrocks.thrift.TUserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static com.starrocks.catalog.InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;

/**
 * Full scan of an SCHEMA table.
 */
public class SchemaScanNode extends ScanNode {

    private static final Logger LOG = LogManager.getLogger(SchemaScanNode.class);

    private String catalogName;
    private final String tableName;
    private String schemaDb;
    private String schemaTable;
    private String schemaWild;
    private String frontendIP;
    private int frontendPort;
    private Long jobId;
    private String label;

    // only used for BE related schema scans
    private Long beId = null;
    private Long tableId = null;
    private Long partitionId = null;
    private Long tabletId = null;
    private Long txnId = null;
    private String type = null;
    private String state = null;
    private Long logStartTs = null;
    private Long logEndTs = null;
    private String logLevel = null;
    private String logPattern = null;
    private Long logLimit = null;
    private List<TFrontend> frontends = null;

    private List<TScanRangeLocations> beScanRanges = null;

    public void setSchemaDb(String schemaDb) {
        this.schemaDb = schemaDb;
    }

    public void setSchemaTable(String schemaTable) {
        this.schemaTable = schemaTable;
    }

    public String getSchemaDb() {
        return schemaDb;
    }

    public String getSchemaTable() {
        return schemaTable;
    }

    public void setUser(String user) {
    }

    public void setUserIp(String userIp) {
    }

    public void setFrontendIP(String frontendIP) {
        this.frontendIP = frontendIP;
    }

    public void setFrontendPort(int frontendPort) {
        this.frontendPort = frontendPort;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public String getLabel() {
        return label;
    }

    public Long getJobId() {
        return jobId;
    }

    public String getTableName() {
        return tableName;
    }

    public List<TFrontend> getFrontends() {
        return frontends;
    }

    /**
     * Constructs node to scan given data files of table 'tbl'.
     */
    public SchemaScanNode(PlanNodeId id, TupleDescriptor desc) {
        super(id, desc, "SCAN SCHEMA");
        this.tableName = desc.getTable().getName();
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        return helper.addValue(super.debugString()).toString();
    }

    @Override
    public void finalizeStats(Analyzer analyzer) throws UserException {
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.SCHEMA_SCAN_NODE;
        msg.schema_scan_node = new TSchemaScanNode(desc.getId().asInt(), tableName);

        if (catalogName != null) {
            msg.schema_scan_node.setCatalog_name(catalogName);
        } else {
            msg.schema_scan_node.setCatalog_name(DEFAULT_INTERNAL_CATALOG_NAME);
        }

        if (schemaDb != null) {
            msg.schema_scan_node.setDb(schemaDb);
        } else {
            if (tableName.equalsIgnoreCase("GLOBAL_VARIABLES")) {
                msg.schema_scan_node.setDb("GLOBAL");
            } else if (tableName.equalsIgnoreCase("SESSION_VARIABLES")) {
                msg.schema_scan_node.setDb("SESSION");
            }
        }

        if (schemaTable != null) {
            msg.schema_scan_node.setTable(schemaTable);
        }
        if (schemaWild != null) {
            msg.schema_scan_node.setWild(schemaWild);
        }

        ConnectContext ctx = ConnectContext.get();
        if (ctx != null) {
            msg.schema_scan_node.setThread_id(ConnectContext.get().getConnectionId());
        }
        msg.schema_scan_node.setIp(frontendIP);
        msg.schema_scan_node.setPort(frontendPort);

        TUserIdentity tCurrentUser = ConnectContext.get().getCurrentUserIdentity().toThrift();
        msg.schema_scan_node.setCurrent_user_ident(tCurrentUser);

        if (tableId != null) {
            msg.schema_scan_node.setTable_id(tableId);
        }

        if (partitionId != null) {
            msg.schema_scan_node.setPartition_id(partitionId);
        }

        if (tabletId != null) {
            msg.schema_scan_node.setTablet_id(tabletId);
        }

        if (jobId != null) {
            msg.schema_scan_node.setJob_id(jobId);
        }

        if (label != null) {
            msg.schema_scan_node.setLabel(label);
        }
        if (txnId != null) {
            msg.schema_scan_node.setTxn_id(txnId);
        }
        if (type != null) {
            msg.schema_scan_node.setType(type);
        }
        if (state != null) {
            msg.schema_scan_node.setState(state);
        }
        if (logStartTs != null) {
            msg.schema_scan_node.setLog_start_ts(logStartTs);
        }
        if (logEndTs != null) {
            msg.schema_scan_node.setLog_end_ts(logEndTs);
        }
        if (logLevel != null) {
            msg.schema_scan_node.setLog_level(logLevel);
        }
        if (logPattern != null) {
            msg.schema_scan_node.setLog_pattern(logPattern);
        }
        if (logLimit != null) {
            msg.schema_scan_node.setLog_limit(logLimit);
        }
        // setting limit for the scanner may cause query to return less rows than expected
        // but this is for the purpose of protect FE resource usage, so it's acceptable
        if (getLimit() > 0) {
            msg.schema_scan_node.setLimit(getLimit());
        }
        if (frontends != null) {
            msg.schema_scan_node.setFrontends(frontends);
        }
    }

    public void setBeId(long beId) {
        this.beId = beId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public void setPartitionId(long partitionId) {
        this.partitionId = partitionId;
    }

    public void setTabletId(long tabletId) {
        this.tabletId = tabletId;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setState(String state) {
        this.state = state;
    }

    public void setLogStartTs(long logStartTs) {
        this.logStartTs = logStartTs;
    }

    public void setLogEndTs(long logEndTs) {
        this.logEndTs = logEndTs;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public void setLogPattern(String logPattern) {
        this.logPattern = logPattern;
    }

    public void setLogLimit(long logLimit) {
        this.logLimit = logLimit;
    }

    public boolean isBeSchemaTable() {
        return SystemTable.isBeSchemaTable(tableName);
    }

    public void computeFeNodes() {
        for (Frontend fe : GlobalStateMgr.getCurrentState().getNodeMgr().getFrontends(null /* all */)) {
            if (!fe.isAlive()) {
                continue;
            }
            if (frontends == null) {
                frontends = Lists.newArrayList();
            }
            TFrontend feInfo = new TFrontend();
            feInfo.setId(fe.getNodeName());
            feInfo.setIp(fe.getHost());
            feInfo.setHttp_port(Config.http_port);
            frontends.add(feInfo);
        }
    }

    public void computeBeScanRanges() {
        for (Backend be : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend().values()) {
            // if user specifies BE id, we try to scan all BEs(including bad BE)
            // if user doesn't specify BE id, we only scan live BEs
            if ((be.isAlive() && beId == null) || (beId != null && beId.equals(be.getId()))) {
                if (beScanRanges == null) {
                    beScanRanges = Lists.newArrayList();
                }
                TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
                TScanRangeLocation location = new TScanRangeLocation();
                location.setBackend_id(be.getId());
                location.setServer(new TNetworkAddress(be.getHost(), be.getBePort()));
                scanRangeLocations.addToLocations(location);
                TScanRange scanRange = new TScanRange();
                scanRangeLocations.setScan_range(scanRange);
                beScanRanges.add(scanRangeLocations);
            }
        }
    }

    /**
     * We query MySQL Meta to get request's data location
     * extra result info will pass to backend ScanNode
     */
    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return beScanRanges;
    }

    @Override
    public int getNumInstances() {
        return beScanRanges == null ? 1 : beScanRanges.size();
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }
}
