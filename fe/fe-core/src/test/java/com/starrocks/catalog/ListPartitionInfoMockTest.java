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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.StarRocksException;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TWriteQuorumType;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import com.starrocks.type.TypeFactory;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ListPartitionInfoMockTest {

    @Test
    public void testMultiListPartition(@Mocked OlapTable dstTable,
                                       @Mocked GlobalStateMgr globalStateMgr,
                                       @Mocked GlobalTransactionMgr globalTransactionMgr) throws StarRocksException {

        DescriptorTable descTable = new DescriptorTable();
        TupleDescriptor tuple = descTable.createTupleDescriptor("DstTable");
        // k1
        SlotDescriptor k1 = descTable.addSlotDescriptor(tuple);
        k1.setColumn(new Column("k1", IntegerType.BIGINT));
        k1.setIsMaterialized(true);

        // k2
        SlotDescriptor k2 = descTable.addSlotDescriptor(tuple);
        k2.setColumn(new Column("k2", TypeFactory.createVarcharType(25)));
        k2.setIsMaterialized(true);
        // v1
        SlotDescriptor v1 = descTable.addSlotDescriptor(tuple);
        v1.setColumn(new Column("v1", TypeFactory.createVarcharType(25)));
        v1.setIsMaterialized(true);
        // v2
        SlotDescriptor v2 = descTable.addSlotDescriptor(tuple);
        v2.setColumn(new Column("v2", IntegerType.BIGINT));
        v2.setIsMaterialized(true);

        ListPartitionInfo listPartitionInfo = new ListPartitionInfo(PartitionType.LIST,
                Lists.newArrayList(new Column("dt", StringType.STRING), new Column("province", StringType.STRING)));
        List<String> multiItems = Lists.newArrayList("dt", "shanghai");
        List<List<String>> multiValues = new ArrayList<>();
        multiValues.add(multiItems);

        listPartitionInfo.setMultiValues(1, multiValues);
        listPartitionInfo.setReplicationNum(1, (short) 3);
        MaterializedIndex index = new MaterializedIndex(1, MaterializedIndex.IndexState.NORMAL);
        HashDistributionInfo distInfo = new HashDistributionInfo(
                3, Lists.newArrayList(new Column("id", IntegerType.BIGINT)));
        Partition partition = new Partition(1, 11, "p1", index, distInfo);

        Map<ColumnId, Column> idToColumn = Maps.newTreeMap(ColumnId.CASE_INSENSITIVE_ORDER);
        idToColumn.put(ColumnId.create("dt"), new Column("dt", StringType.STRING));
        idToColumn.put(ColumnId.create("province"), new Column("province", StringType.STRING));
        new Expectations() {{
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getGlobalTransactionMgr();
                result = globalTransactionMgr;
                globalTransactionMgr.getTransactionState(anyLong, anyLong);
                result = new TransactionState();
                globalStateMgr.getNodeMgr().getClusterInfo();
                result = new SystemInfoService();
                dstTable.getId();
                result = 1;
                dstTable.getPartitions();
                result = Lists.newArrayList(partition);
                dstTable.getPartition(1L);
                result = partition;
                dstTable.getPartitionInfo();
                result = listPartitionInfo;
                dstTable.getDefaultDistributionInfo();
                result = distInfo;
                dstTable.getIdToColumn();
                result = idToColumn;
            }};

        OlapTableSink sink = new OlapTableSink(dstTable, tuple, Lists.newArrayList(1L),
                TWriteQuorumType.MAJORITY, false, false, false);
        sink.init(new TUniqueId(1, 2), 3, 4, 1000);
        sink.complete();

        Assertions.assertTrue(sink.toThrift() instanceof TDataSink);
    }

}
