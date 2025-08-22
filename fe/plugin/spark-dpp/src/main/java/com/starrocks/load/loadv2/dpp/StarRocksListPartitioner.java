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

package com.starrocks.load.loadv2.dpp;

import com.starrocks.common.PartitionType;
import com.starrocks.load.loadv2.etl.EtlJobConfig;
import org.apache.spark.Partitioner;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StarRocksListPartitioner extends Partitioner {
    private EtlJobConfig.EtlPartitionInfo partitionInfo;
    private List<PartitionListKey> partitionListKeys;
    List<Integer> partitionKeyIndexes;
    Map<DppColumns, Integer> partitionsMap;

    public StarRocksListPartitioner(EtlJobConfig.EtlPartitionInfo partitionInfo,
                                    List<Integer> partitionKeyIndexes,
                                    List<PartitionListKey> partitionListKeys) {
        this.partitionInfo = partitionInfo;
        this.partitionListKeys = partitionListKeys;
        this.partitionKeyIndexes = partitionKeyIndexes;
        this.partitionsMap = new HashMap<>();
        for (int i = 0; i < partitionListKeys.size(); ++i) {
            PartitionListKey partitionListKey = partitionListKeys.get(i);
            for (DppColumns dppColumns : partitionListKey.inKeys) {
                partitionsMap.put(dppColumns, i);
            }
        }
    }

    @Override
    public int numPartitions() {
        if (partitionInfo == null) {
            return 0;
        }
        return  partitionInfo.partitions.size();
    }

    @Override
    public int getPartition(Object var1) {
        PartitionType partitionType = PartitionType.getByType(partitionInfo.partitionType);
        if (partitionType != PartitionType.LIST) {
            return -1;
        }
        DppColumns key = (DppColumns) var1;
        // get the partition columns from key as partition key
        DppColumns partitionKey = new DppColumns(key, partitionKeyIndexes);
        if (partitionsMap.containsKey(partitionKey)) {
            return partitionsMap.get(partitionKey);
        }
        return -1;
    }

    public static class PartitionListKey implements Serializable {
        public List<DppColumns> inKeys;

        public PartitionListKey() {
            this.inKeys = new ArrayList<>();
        }

        @Override
        public String toString() {
            return "PartitionListKey{" +
                    "inKeys=" + inKeys +
                    '}';
        }
    }
}
