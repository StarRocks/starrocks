// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.loadv2.dpp;

import org.apache.spark.Partitioner;
import com.starrocks.common.PartitionType;
import com.starrocks.load.loadv2.etl.EtlJobConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class StarRocksListPartitioner extends Partitioner {
    private EtlJobConfig.EtlPartitionInfo partitionInfo;
    private List<PartitionListKey> partitionListKeys;
    List<Integer> partitionKeyIndexes;

    public StarRocksListPartitioner(EtlJobConfig.EtlPartitionInfo partitionInfo,
                                    List<Integer> partitionKeyIndexes,
                                    List<PartitionListKey> partitionListKeys) {
        this.partitionInfo = partitionInfo;
        this.partitionListKeys = partitionListKeys;
        this.partitionKeyIndexes = partitionKeyIndexes;
    }

    @Override
    public int numPartitions() {
        if (partitionInfo == null) {
            return 0;
        }
        PartitionType partitionType = PartitionType.getByType(partitionInfo.partitionType);
        if (partitionType == PartitionType.UNPARTITIONED) {
            return 0;
        }
        if (partitionType == PartitionType.RANGE) {
            return 0;
        }
        return  partitionInfo.partitions.size();
    }

    @Override
    public int getPartition(Object var1) {
        PartitionType partitionType = PartitionType.getByType(partitionInfo.partitionType);
        if (partitionType == PartitionType.UNPARTITIONED) {
            return 0;
        }
        if (partitionType == PartitionType.RANGE) {
            return 0;
        }
        DppColumns key = (DppColumns) var1;
        // get the partition columns from key as partition key
        DppColumns partitionKey = new DppColumns(key, partitionKeyIndexes);
        for (int i = 0; i < partitionListKeys.size(); ++i) {
            if (partitionListKeys.get(i).isRowContained(partitionKey)) {
                return i;
            }
        }
        return -1;
    }

    public static class PartitionListKey implements Serializable {
        public List<DppColumns> inKeys;

        public PartitionListKey() {
            this.inKeys = new ArrayList<>();
        }

        public boolean isRowContained(DppColumns row) {
            return inKeys.contains(row);
        }

        @Override
        public String toString() {
            return "PartitionListKey{" +
                    "inKeys=" + inKeys +
                    '}';
        }
    }
}
