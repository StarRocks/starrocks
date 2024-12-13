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

import com.starrocks.catalog.system.SystemTable;
import com.starrocks.server.GlobalStateMgr;
<<<<<<< HEAD
=======
import com.starrocks.server.WarehouseManager;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TNodesInfo;
import com.starrocks.thrift.TSchemaTableSink;

public class SchemaTableSink extends DataSink {
    private final String tableName;

<<<<<<< HEAD
    public SchemaTableSink(SystemTable table) {
        tableName = table.getName();
=======
    private long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;

    public SchemaTableSink(SystemTable table, long warehouseId) {
        tableName = table.getName();
        this.warehouseId = warehouseId;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + " SCHEMA TABLE(" + tableName + ") SINK\n");
        strBuilder.append(prefix + "  " + DataPartition.UNPARTITIONED.getExplainString(explainLevel));
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink tDataSink = new TDataSink(TDataSinkType.SCHEMA_TABLE_SINK);
        TSchemaTableSink sink = new TSchemaTableSink();
<<<<<<< HEAD
        TNodesInfo info = GlobalStateMgr.getCurrentState().createNodesInfo(GlobalStateMgr.getCurrentState().getClusterId());
=======
        TNodesInfo info = GlobalStateMgr.getCurrentState().createNodesInfo(warehouseId,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        sink.setTable(tableName);
        sink.setNodes_info(info);
        tDataSink.setSchema_table_sink(sink);
        return tDataSink;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return null;
    }

    @Override
    public boolean canUsePipeLine() {
        // @TODO(silverbullet233): need to be adapted on pipeline engine
        return false;
    }
}
