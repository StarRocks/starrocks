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


package com.starrocks.load;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.DdlException;
import com.starrocks.persist.AddPartitionsInfoV2;
import com.starrocks.persist.ListPartitionPersistInfo;
import com.starrocks.persist.PartitionPersistInfoV2;
import com.starrocks.persist.RangePartitionPersistInfo;
import com.starrocks.persist.SinglePartitionPersistInfo;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

public class PartitionUtils {
    private static final Logger LOG = LogManager.getLogger(PartitionUtils.class);

    public static void createAndAddTempPartitionsForTable(Database db, OlapTable targetTable,
                                                          String postfix, List<Long> sourcePartitionIds,
                                                          List<Long> tmpPartitionIds) throws DdlException {
        List<Partition> newTempPartitions = GlobalStateMgr.getCurrentState().createTempPartitionsFromPartitions(
                db, targetTable, postfix, sourcePartitionIds, tmpPartitionIds);
        if (!db.writeLockAndCheckExist()) {
            throw new DdlException("create and add partition failed. database:{}" + db.getFullName() + " not exist");
        }
        boolean success = false;
        try {
            // should check whether targetTable exists
            Table tmpTable = db.getTable(targetTable.getId());
            if (tmpTable == null) {
                throw new DdlException("create partition failed because target table does not exist");
            }
            if (sourcePartitionIds.stream().anyMatch(id -> targetTable.getPartition(id) == null)) {
                throw new DdlException("create partition failed because src partitions changed");
            }
            List<Partition> sourcePartitions = sourcePartitionIds.stream()
                    .map(id -> targetTable.getPartition(id)).collect(Collectors.toList());
            PartitionInfo partitionInfo = targetTable.getPartitionInfo();
            List<PartitionPersistInfoV2> partitionInfoV2List = Lists.newArrayListWithCapacity(newTempPartitions.size());
            for (int i = 0; i < newTempPartitions.size(); i++) {
                targetTable.addTempPartition(newTempPartitions.get(i));
                long sourcePartitionId = sourcePartitions.get(i).getId();
                partitionInfo.addPartition(newTempPartitions.get(i).getId(),
                        partitionInfo.getDataProperty(sourcePartitionId),
                        partitionInfo.getReplicationNum(sourcePartitionId),
                        partitionInfo.getIsInMemory(sourcePartitionId),
                        partitionInfo.getDataCacheInfo(sourcePartitionId));
                Partition partition = newTempPartitions.get(i);

                PartitionPersistInfoV2 info;
                if (partitionInfo.isRangePartition()) {
                    RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                    rangePartitionInfo.setRange(partition.getId(), true,
                            rangePartitionInfo.getRange(sourcePartitionId));
                    Range<PartitionKey> range = rangePartitionInfo.getRange(partition.getId());

                    info = new RangePartitionPersistInfo(db.getId(), targetTable.getId(),
                            partition, partitionInfo.getDataProperty(partition.getId()),
                            partitionInfo.getReplicationNum(partition.getId()),
                            partitionInfo.getIsInMemory(partition.getId()), true,
                            range, partitionInfo.getDataCacheInfo(partition.getId()));
                } else if (partitionInfo instanceof SinglePartitionInfo) {
                    info = new SinglePartitionPersistInfo(db.getId(), targetTable.getId(),
                            partition, partitionInfo.getDataProperty(partition.getId()),
                            partitionInfo.getReplicationNum(partition.getId()),
                            partitionInfo.getIsInMemory(partition.getId()), true,
                            partitionInfo.getDataCacheInfo(partition.getId()));
                } else if (partitionInfo instanceof ListPartitionInfo) {
                    ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;

                    listPartitionInfo.setIdToIsTempPartition(partition.getId(), true);
                    List<String> values = listPartitionInfo.getIdToValues().get(sourcePartitionId);
                    if (values != null) {
                        listPartitionInfo.setValues(partition.getId(), values);
                        List<LiteralExpr> literalExprs = listPartitionInfo.getLiteralExprValues().get(sourcePartitionId);
                        listPartitionInfo.setDirectLiteralExprValues(partition.getId(), literalExprs);
                    }

                    List<List<String>> multiValues = listPartitionInfo.getIdToMultiValues().get(sourcePartitionId);
                    if (multiValues != null) {
                        listPartitionInfo.setMultiValues(partition.getId(), multiValues);
                        List<List<LiteralExpr>> multiLiteralExprs =
                                listPartitionInfo.getMultiLiteralExprValues().get(sourcePartitionId);
                        listPartitionInfo.setDirectMultiLiteralExprValues(partition.getId(), multiLiteralExprs);
                    }
                    info = new ListPartitionPersistInfo(db.getId(), targetTable.getId(),
                            partition, partitionInfo.getDataProperty(partition.getId()),
                            partitionInfo.getReplicationNum(partition.getId()),
                            partitionInfo.getIsInMemory(partition.getId()), true,
                            values, multiValues);
                } else {
                    throw new DdlException("Unsupported partition persist info.");
                }
                partitionInfoV2List.add(info);
            }

            AddPartitionsInfoV2 infos = new AddPartitionsInfoV2(partitionInfoV2List);
            GlobalStateMgr.getCurrentState().getEditLog().logAddPartitions(infos);

            success = true;
        } finally {
            if (!success) {
                try {
                    clearTabletsFromInvertedIndex(newTempPartitions);
                } catch (Throwable t) {
                    LOG.warn("clear tablets from inverted index failed", t);
                }
            }
            db.writeUnlock();
        }
    }

    public static void clearTabletsFromInvertedIndex(List<Partition> partitions) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        for (Partition partition : partitions) {
            for (MaterializedIndex materializedIndex : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                for (Tablet tablet : materializedIndex.getTablets()) {
                    invertedIndex.deleteTablet(tablet.getId());
                }
            }
        }
    }
}
