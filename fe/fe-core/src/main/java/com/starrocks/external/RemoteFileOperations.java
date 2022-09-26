// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external;

import com.google.common.collect.Lists;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.external.hive.Partition;

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
    protected CachingRemoteFileIO remoteFileIO;
    private final ExecutorService executor;
    private final boolean isRecursive;

    public RemoteFileOperations(CachingRemoteFileIO remoteFileIO, ExecutorService executor, boolean isRecursive) {
        this.remoteFileIO = remoteFileIO;
        this.executor = executor;
        this.isRecursive = isRecursive;
    }

    public List<RemoteFileInfo> getRemoteFiles(List<Partition> partitions) {
        return getRemoteFiles(partitions, Optional.empty());
    }

    public List<RemoteFileInfo> getRemoteFiles(List<Partition> partitions, Optional<String> hudiTableLocation) {
        Map<RemotePathKey, Partition> pathKeyToPartition = partitions.stream()
                .collect(Collectors.toMap(
                        partition -> RemotePathKey.of(partition.getFullPath(), isRecursive, hudiTableLocation),
                        Function.identity()));

        List<RemoteFileInfo> resultRemoteFiles = Lists.newArrayList();
        List<Future<Map<RemotePathKey, List<RemoteFileDesc>>>> futures = Lists.newArrayList();
        for (Partition partition : partitions) {
            RemotePathKey pathKey = RemotePathKey.of(partition.getFullPath(), isRecursive, hudiTableLocation);
            Future<Map<RemotePathKey, List<RemoteFileDesc>>> future = executor.submit(() -> remoteFileIO.getRemoteFiles(pathKey));
            futures.add(future);
        }

        List<Map<RemotePathKey, List<RemoteFileDesc>>> result = Lists.newArrayList();
        for (Future<Map<RemotePathKey, List<RemoteFileDesc>>> future : futures) {
            try {
                result.add(future.get());
            } catch (InterruptedException | ExecutionException e) {
                throw new StarRocksConnectorException("Failed to get remote files, msg: %s", e.getMessage());
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
}
