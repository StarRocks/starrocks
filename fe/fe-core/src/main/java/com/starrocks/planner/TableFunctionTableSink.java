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

import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TTableFunctionTableSink;

public class TableFunctionTableSink extends DataSink {
    private final TableFunctionTable table;
    private final CloudConfiguration cloudConfiguration;

    public TableFunctionTableSink(TableFunctionTable targetTable) {
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(
                targetTable.getProperties());
        this.table = targetTable;
        this.cloudConfiguration = cloudConfiguration;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        return prefix + "TABLE FUNCTION TABLE SINK\n" +
                prefix + "  PATH: " + table.getPath() + "\n" +
                prefix + "  FORMAT: " + table.getFormat() + "\n" +
                prefix + "  PARTITION BY: " + table.getPartitionColumnNames() + "\n" +
                prefix + "  SINGLE: " + table.isWriteSingleFile() + "\n" +
                prefix + "  " + DataPartition.RANDOM.getExplainString(explainLevel);
    }

    @Override
    protected TDataSink toThrift() {
        TTableFunctionTableSink tTableFunctionTableSink = new TTableFunctionTableSink();
        tTableFunctionTableSink.setTarget_table(table.toTTableFunctionTable());
        TCloudConfiguration tCloudConfiguration = new TCloudConfiguration();
        cloudConfiguration.toThrift(tCloudConfiguration);
        tTableFunctionTableSink.setCloud_configuration(tCloudConfiguration);
        TDataSink tDataSink = new TDataSink(TDataSinkType.TABLE_FUNCTION_TABLE_SINK);
        tDataSink.setTable_function_table_sink(tTableFunctionTableSink);
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
        return true;
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    public boolean isWriteSingleFile() { return table.isWriteSingleFile(); }
}