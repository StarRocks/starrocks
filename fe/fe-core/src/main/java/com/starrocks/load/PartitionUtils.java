// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.common.DdlException;
import com.starrocks.persist.AddPartitionsInfo;
import com.starrocks.persist.PartitionPersistInfo;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.stream.Collectors;

public class PartitionUtils {
    public static void createAndAddTempPartitionsForTable(Database db, OlapTable targetTable,
                                                          String postfix, List<Long> sourcePartitionIds,
                                                          List<Long> tmpPartitionIds) throws DdlException {
        List<Partition> newTempPartitions = GlobalStateMgr.getCurrentState().createTempPartitionsFromPartitions(
                db, targetTable, postfix, sourcePartitionIds, tmpPartitionIds);
        if (!db.writeLockAndCheckExist()) {
            throw new DdlException("create and add partition failed. database:{}" + db.getFullName() + " not exist");
        }
        try {
            List<Partition> sourcePartitions = sourcePartitionIds.stream()
                    .map(id -> targetTable.getPartition(id)).collect(Collectors.toList());
            PartitionInfo partitionInfo = targetTable.getPartitionInfo();
            List<PartitionPersistInfo> partitionInfoList = Lists.newArrayListWithCapacity(newTempPartitions.size());
            for (int i = 0; i < newTempPartitions.size(); i++) {
                targetTable.addTempPartition(newTempPartitions.get(i));
                long sourcePartitionId = sourcePartitions.get(i).getId();
                partitionInfo.addPartition(newTempPartitions.get(i).getId(),
                        partitionInfo.getDataProperty(sourcePartitionId),
                        partitionInfo.getReplicationNum(sourcePartitionId),
                        partitionInfo.getIsInMemory(sourcePartitionId));
                Partition partition = newTempPartitions.get(i);
                // range is null for UNPARTITIONED type
                Range<PartitionKey> range = null;
                if (partitionInfo.getType() == PartitionType.RANGE) {
                    RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                    rangePartitionInfo.setRange(partition.getId(), true,
                            rangePartitionInfo.getRange(sourcePartitionId));
                    range = rangePartitionInfo.getRange(partition.getId());
                }
                PartitionPersistInfo info =
                        new PartitionPersistInfo(db.getId(), targetTable.getId(), partition,
                                range,
                                partitionInfo.getDataProperty(partition.getId()),
                                partitionInfo.getReplicationNum(partition.getId()),
                                partitionInfo.getIsInMemory(partition.getId()),
                                true);
                partitionInfoList.add(info);
            }
            AddPartitionsInfo infos = new AddPartitionsInfo(partitionInfoList);
            GlobalStateMgr.getCurrentState().getEditLog().logAddPartitions(infos);
        } finally {
            db.writeUnlock();
        }
    }
}
