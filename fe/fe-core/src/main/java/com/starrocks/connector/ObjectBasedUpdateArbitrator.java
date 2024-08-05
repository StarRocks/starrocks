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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// used to check whether data in object storage system has changed.
// for data in system like: s3/oss
public class ObjectBasedUpdateArbitrator extends TableUpdateArbitrator {

    @Override
    public Map<String, Optional<HivePartitionDataInfo>> getPartitionDataInfos() {
        Map<String, Optional<HivePartitionDataInfo>> partitionDataInfos = Maps.newHashMap();
        List<String> partitionNameToFetch = partitionNames;
        if (partitionLimit >= 0 && partitionLimit < partitionNames.size()) {
            partitionNameToFetch = partitionNames.subList(partitionNames.size() - partitionLimit, partitionNames.size());
        }
        GetRemoteFilesParams params =
                GetRemoteFilesParams.newBuilder().setPartitionNames(partitionNames).setCheckPartitionExistence(false).build();
        List<RemoteFileInfo> remoteFileInfos =
                GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFiles(table, params);
        for (int i = 0; i < partitionNameToFetch.size(); i++) {
            RemoteFileInfo remoteFileInfo = remoteFileInfos.get(i);
            List<RemoteFileDesc> remoteFileDescs = remoteFileInfo.getFiles();
            if (remoteFileDescs != null) {
                long lastFileModifiedTime = Long.MIN_VALUE;
                int fileNumber = remoteFileDescs.size();
                Optional<RemoteFileDesc> maxLastModifiedTimeFile = remoteFileDescs.stream()
                        .max(Comparator.comparingLong(RemoteFileDesc::getModificationTime));
                if (maxLastModifiedTimeFile.isPresent()) {
                    lastFileModifiedTime = maxLastModifiedTimeFile.get().getModificationTime();
                }
                HivePartitionDataInfo hivePartitionDataInfo = new HivePartitionDataInfo(lastFileModifiedTime, fileNumber);
                partitionDataInfos.put(partitionNameToFetch.get(i), Optional.of(hivePartitionDataInfo));
            } else {
                partitionDataInfos.put(partitionNameToFetch.get(i), Optional.empty());
            }
        }
        return partitionDataInfos;
    }
}
