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

package com.starrocks.connector;

import com.google.common.collect.Maps;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Map;
import java.util.Optional;

// Used to check whether data in hdfs has changed.
// Hdfs has api to check whether a directory has changed after update,
// So just check the directory of partition, not list all files under parition directory.
public class DirectoryBasedUpdateArbitrator extends TableUpdateArbitrator {
    @Override
    public Map<String, Optional<HivePartitionDataInfo>> getPartitionDataInfos() {
        Map<String, Optional<HivePartitionDataInfo>> partitionDataInfos = Maps.newHashMap();
        List<String> partitionNameToFetch = partitionNames;
        if (partitionLimit >= 0 && partitionLimit < partitionNames.size()) {
            partitionNameToFetch = partitionNames.subList(partitionNames.size() - partitionLimit, partitionNames.size());
        }
        List<PartitionInfo> partitions =
                GlobalStateMgr.getCurrentState().getMetadataMgr().getRemotePartitions(table, partitionNameToFetch);
        for (int i = 0; i < partitionNameToFetch.size(); i++) {
            PartitionInfo partitionInfo = partitions.get(i);
            HivePartitionDataInfo hivePartitionDataInfo = new HivePartitionDataInfo(partitionInfo.getModifiedTime(), 1);
            partitionDataInfos.put(partitionNameToFetch.get(i), Optional.of(hivePartitionDataInfo));
        }
        return partitionDataInfos;
    }
}
