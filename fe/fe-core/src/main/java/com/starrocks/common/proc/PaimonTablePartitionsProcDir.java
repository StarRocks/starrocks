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

package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.paimon.Partition;
import com.starrocks.server.GlobalStateMgr;

import java.util.ArrayList;
import java.util.List;

public class PaimonTablePartitionsProcDir extends PartitionsProcDir {
    private Table table;

    public PaimonTablePartitionsProcDir(Table table, PartitionType partitionType) {
        super(partitionType);
        this.table = table;
        this.createTitleNames();
    }

    @Override
    public void createTitleNames() {
        ImmutableList.Builder<String> builder =
                new ImmutableList.Builder<String>().add("PartitionName").add("VisibleVersionTime").add("State")
                        .add("PartitionKey").add("RowCount").add("DataSize").add("FileCount");
        this.titleNames = builder.build();

    }

    public List<List<Comparable>> getPartitionInfos() {
        Preconditions.checkNotNull(table);
        PaimonTable paimonTable = (PaimonTable) table;
        // get info
        List<List<Comparable>> partitionInfos = new ArrayList<List<Comparable>>();
        List<String> listPartitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .listPartitionNames(table.getCatalogName(), paimonTable.getCatalogDBName(),
                        paimonTable.getCatalogTableName(), ConnectorMetadatRequestContext.DEFAULT);

        List<PartitionInfo> tblPartitionInfo = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getPartitions(table.getCatalogName(), table, listPartitionNames);

        for (PartitionInfo partitionInfo : tblPartitionInfo) {
            partitionInfos.add(getPartitionInfo(partitionInfo));
        }
        return partitionInfos;
    }

    private List<Comparable> getPartitionInfo(PartitionInfo tblPartitionInfo) {
        List<Comparable> partitionInfo = new ArrayList<Comparable>();
        Partition partition = (Partition) tblPartitionInfo;
        // PartitionName
        partitionInfo.add(partition.getPartitionName());
        // VisibleVersionTime
        partitionInfo.add(TimeUtils.longToTimeString(partition.getModifiedTime()));
        // State
        partitionInfo.add("Normal");
        // partition key
        partitionInfo.add(table.isUnPartitioned() ? "" : String.join(",", table.getPartitionColumnNames()));

        partitionInfo.add(partition.getRecordCount());
        partitionInfo.add(partition.getFileSizeInBytes());
        partitionInfo.add(partition.getFileCount());

        return partitionInfo;
    }


    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String partitionIdOrName) throws AnalysisException {
        throw new AnalysisException("paimon not support");
    }
}
