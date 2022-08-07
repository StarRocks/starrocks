package com.starrocks.external;

import com.google.common.collect.Lists;

import com.starrocks.external.elasticsearch.HivePartitionStatistics;
import com.starrocks.external.hive.Partition;
import com.starrocks.external.hive.RemoteFileDesc;
import com.starrocks.external.hive.RemoteFileInfo;
import com.starrocks.external.hive.RemotePathKey;
import com.starrocks.external.hive.StarRocksConnectorException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RemoteFileOperations {
    protected CachingRemoteFileIO remoteFileIO;
    private final ExecutorService executor = Executors.newFixedThreadPool(64);

    public RemoteFileOperations(CachingRemoteFileIO remoteFileIO) {
        this.remoteFileIO = remoteFileIO;
    }

    public List<RemoteFileInfo> getRemoteFiles(List<Partition> partitions) {
        Map<RemotePathKey, Partition> pathKeyToPartition = partitions.stream()
                .collect(Collectors.toMap(partition -> RemotePathKey.of(partition.getFullPath(), false),
                        Function.identity()));

        List<RemoteFileInfo> resultRemoteFiles = Lists.newArrayList();
        List<Future<Map<RemotePathKey, List<RemoteFileDesc>>>> futures = Lists.newArrayList();
        for (Partition partition : partitions) {
            Future<Map<RemotePathKey, List<RemoteFileDesc>>> future = executor.submit(() ->
                    remoteFileIO.getRemoteFiles(RemotePathKey.of(partition.getFullPath(), false)));
            futures.add(future);
        }

        List<Map<RemotePathKey, List<RemoteFileDesc>>> result = Lists.newArrayList();
        for (Future<Map<RemotePathKey, List<RemoteFileDesc>>> future : futures) {
            try {
                result.add(future.get());
            } catch (InterruptedException | ExecutionException e) {
                throw new StarRocksConnectorException(e.getMessage());
            }
        }

        for (Map<RemotePathKey, List<RemoteFileDesc>> pathToDesc : result) {
            resultRemoteFiles.addAll(fillFileInfo(pathToDesc, pathKeyToPartition));
        }

        return resultRemoteFiles;
    }

    public List<RemoteFileInfo> getPresentFilesInCache(Collection<Partition> partitions) {
        Map<RemotePathKey, Partition> pathKeyToPartition = partitions.stream()
                .collect(Collectors.toMap(partition -> RemotePathKey.of(partition.getFullPath(), false),
                        Function.identity()));

        List<RemotePathKey> paths = partitions.stream()
                .map(partition -> RemotePathKey.of(partition.getFullPath(), false))
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
