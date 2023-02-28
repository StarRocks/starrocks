// This file is made available under Elastic License 2.0.
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
import com.starrocks.catalog.SchemaTable;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TSchemaScanNode;
import com.starrocks.thrift.TUserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Full scan of an SCHEMA table.
 */
// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class SchemaScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(SchemaTable.class);

    private final String tableName;
    private String schemaDb;
    private String schemaTable;
    private String schemaWild;
    private String user;
    private String userIp;
    private String frontendIP;
    private int frontendPort;

    // only used for BE related schema scans
    private Long beId = null;
    private Long tableId = null;
    private Long partitionId = null;
    private Long tabletId = null;
    private Long txnId = null;
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
        this.user = user;
    }

    public void setUserIp(String userIp) {
        this.userIp = userIp;
    }

    public void setFrontendIP(String frontendIP) {
        this.frontendIP = frontendIP;
    }

    public void setFrontendPort(int frontendPort) {
        this.frontendPort = frontendPort;
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
        // Convert predicates to MySQL columns and filters.
        schemaDb = analyzer.getSchemaDb();
        schemaTable = analyzer.getSchemaTable();
        schemaWild = analyzer.getSchemaWild();
        user = analyzer.getQualifiedUser();
        userIp = analyzer.getContext().getRemoteIP();
        frontendIP = FrontendOptions.getLocalHostAddress();
        frontendPort = Config.rpc_port;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.SCHEMA_SCAN_NODE;
        msg.schema_scan_node = new TSchemaScanNode(desc.getId().asInt(), tableName);
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

    public boolean isBeSchemaTable() {
        return SchemaTable.isBeSchemaTable(tableName);
    }

    public void computeBeScanRanges() {
        for (Backend be : GlobalStateMgr.getCurrentSystemInfo().getIdToBackend().values()) {
            // if user specifies BE id, we try to scan all BEs(including bad BE)
            // if user doesn't specify BE id, we only scan live BEs
            if ((be.isAlive() && beId == null) || be.getId() == beId) {
                if (beScanRanges == null) {
                    beScanRanges = Lists.newArrayList();
                }
                TScanRangeLocations locations = new TScanRangeLocations();
                TScanRangeLocation location = new TScanRangeLocation();
                location.setBackend_id(be.getId());
                location.setServer(new TNetworkAddress(be.getHost(), be.getBePort()));
                locations.addToLocations(location);
                beScanRanges.add(locations);
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

}
