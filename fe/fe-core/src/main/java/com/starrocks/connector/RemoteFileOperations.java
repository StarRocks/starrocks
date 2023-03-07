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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.Partition;
import com.starrocks.sql.PlannerProfile;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RemoteFileOperations {
    public static final String HMS_PARTITIONS_REMOTE_FILES = "HMS.PARTITIONS.LIST_FS_PARTITIONS";
    protected CachingRemoteFileIO remoteFileIO;
    private final ExecutorService executor;
    private final boolean isRecursive;
    private final boolean enableCatalogLevelCache;

    public RemoteFileOperations(CachingRemoteFileIO remoteFileIO,
                                ExecutorService executor,
                                boolean isRecursive,
                                boolean enableCatalogLevelCache) {
        this.remoteFileIO = remoteFileIO;
        this.executor = executor;
        this.isRecursive = isRecursive;
        this.enableCatalogLevelCache = enableCatalogLevelCache;
    }

    public List<RemoteFileInfo> getRemoteFiles(List<Partition> partitions) {
        return getRemoteFiles(partitions, Optional.empty());
    }

    public List<RemoteFileInfo> getRemoteFiles(List<Partition> partitions, Optional<String> hudiTableLocation) {
        Map<RemotePathKey, Partition> pathKeyToPartition = Maps.newHashMap();
        for (Partition partition : partitions) {
            RemotePathKey key = RemotePathKey.of(partition.getFullPath(), isRecursive, hudiTableLocation);
            pathKeyToPartition.put(key, partition);
        }

        int cacheMissSize = partitions.size();
        if (enableCatalogLevelCache) {
            cacheMissSize = cacheMissSize - remoteFileIO.getPresentRemoteFiles(
                    Lists.newArrayList(pathKeyToPartition.keySet())).size();
        }

        List<RemoteFileInfo> resultRemoteFiles = Lists.newArrayList();
        List<Future<Map<RemotePathKey, List<RemoteFileDesc>>>> futures = Lists.newArrayList();
        List<Map<RemotePathKey, List<RemoteFileDesc>>> result = Lists.newArrayList();

        PlannerProfile.addCustomProperties(HMS_PARTITIONS_REMOTE_FILES, String.format("%s", cacheMissSize));

        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer(HMS_PARTITIONS_REMOTE_FILES)) {
            for (Partition partition : partitions) {
                RemotePathKey pathKey = RemotePathKey.of(partition.getFullPath(), isRecursive, hudiTableLocation);
                Future<Map<RemotePathKey, List<RemoteFileDesc>>> future = executor.submit(() ->
                        remoteFileIO.getRemoteFiles(pathKey));
                futures.add(future);
            }

            for (Future<Map<RemotePathKey, List<RemoteFileDesc>>> future : futures) {
                try {
                    result.add(future.get());
                } catch (InterruptedException | ExecutionException e) {
                    throw new StarRocksConnectorException("Failed to get remote files, msg: %s", e.getMessage());
                }
            }
        }

        for (Map<RemotePathKey, List<RemoteFileDesc>> pathToDesc : result) {
            resultRemoteFiles.addAll(fillFileInfo(pathToDesc, pathKeyToPartition));
        }

        return resultRemoteFiles;
    }

    public List<RemoteFileInfo> getPresentFilesInCache(Collection<Partition> partitions) {
        return getPresentFilesInCache(partitions, Optional.empty());
    }

    public List<RemoteFileInfo> getPresentFilesInCache(Collection<Partition> partitions, Optional<String> hudiTableLocation) {
        Map<RemotePathKey, Partition> pathKeyToPartition = partitions.stream()
                .collect(Collectors.toMap(partition -> RemotePathKey.of(partition.getFullPath(), isRecursive, hudiTableLocation),
                        Function.identity()));

        List<RemotePathKey> paths = partitions.stream()
                .map(partition -> RemotePathKey.of(partition.getFullPath(), isRecursive, hudiTableLocation))
                .collect(Collectors.toList());

        Map<RemotePathKey, List<RemoteFileDesc>> presentFiles = remoteFileIO.getPresentRemoteFiles(paths);
        return fillFileInfo(presentFiles, pathKeyToPartition);
    }

    public List<RemoteFileInfo> getRemoteFileInfoForStats(List<Partition> partitions, Optional<String> hudiTableLocation) {
        if (enableCatalogLevelCache) {
            return getPresentFilesInCache(partitions, hudiTableLocation);
        } else {
            return getRemoteFiles(partitions, hudiTableLocation);
        }
    }

    private List<RemoteFileInfo> fillFileInfo(
            Map<RemotePathKey, List<RemoteFileDesc>> files,
            Map<RemotePathKey, Partition> partitions) {
        List<RemoteFileInfo> result = Lists.newArrayList();
        for (Map.Entry<RemotePathKey, List<RemoteFileDesc>> entry : files.entrySet()) {
            RemotePathKey key = entry.getKey();
            List<RemoteFileDesc> remoteFileDescs = entry.getValue();
            Partition partition = partitions.get(key);
            result.add(buildRemoteFileInfo(partition, remoteFileDescs));
        }

        return result;
    }

    private RemoteFileInfo buildRemoteFileInfo(Partition partition, List<RemoteFileDesc> fileDescs) {
        RemoteFileInfo.Builder builder = RemoteFileInfo.builder()
                .setFormat(partition.getInputFormat())
                .setFullPath(partition.getFullPath())
                .setFiles(fileDescs.stream()
                        .map(desc -> desc.setTextFileFormatDesc(partition.getTextFileFormatDesc()))
                        .map(desc -> desc.setSplittable(partition.isSplittable()))
                        .collect(Collectors.toList()));

        return builder.build();
    }

    public void invalidateAll() {
        remoteFileIO.invalidateAll();
    }
}
