// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.ObjectStorageUtils;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.RemoteFileBlockDesc;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileIO;
import com.starrocks.connector.RemotePathKey;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HiveRemoteFileIO implements RemoteFileIO {
    private static final Logger LOG = LogManager.getLogger(HiveRemoteFileIO.class);

    private final Configuration configuration;

    // only used for ut.
    private FileSystem fileSystem;

    // blockHost is ip:port
    private final Map<String, Long> blockHostToId = new ConcurrentHashMap<>();
    private final Map<Long, String> idToBlockHost = new ConcurrentHashMap<>();
    private long hostId = 0;
    private static final int UNKNOWN_STORAGE_ID = -1;

    public HiveRemoteFileIO(Configuration configuration) {
        this.configuration = configuration;
    }

    public Map<RemotePathKey, List<RemoteFileDesc>> getRemoteFiles(RemotePathKey pathKey) {
        ImmutableMap.Builder<RemotePathKey, List<RemoteFileDesc>> resultPartitions = ImmutableMap.builder();
        String path = ObjectStorageUtils.formatObjectStoragePath(pathKey.getPath());
        List<RemoteFileDesc> fileDescs = Lists.newArrayList();
        try {
            URI uri = new Path(path).toUri();
            FileSystem fileSystem;

            if (!FeConstants.runningUnitTest) {
                fileSystem = FileSystem.get(uri, configuration);
            } else {
                fileSystem = this.fileSystem;
            }

            RemoteIterator<LocatedFileStatus> blockIterator;
            if (!pathKey.isRecursive()) {
                blockIterator = fileSystem.listLocatedStatus(new Path(uri.getPath()));
            } else {
                blockIterator = fileSystem.listFiles(new Path(uri.getPath()), true);
            }
            while (blockIterator.hasNext()) {
                LocatedFileStatus locatedFileStatus = blockIterator.next();
                if (!isValidDataFile(locatedFileStatus)) {
                    continue;
                }
                String locateName = locatedFileStatus.getPath().toUri().getPath();
                String fileName = PartitionUtil.getSuffixName(uri.getPath(), locateName);

                BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
                List<RemoteFileBlockDesc> fileBlockDescs = getRemoteFileBlockDesc(blockLocations);
                fileDescs.add(new RemoteFileDesc(fileName, "", locatedFileStatus.getLen(),
                              locatedFileStatus.getModificationTime(), ImmutableList.copyOf(fileBlockDescs),
                              ImmutableList.of()));
            }
        } catch (Exception e) {
            LOG.error("Failed to get hive remote file's metadata on path: {}", path, e);
            throw new StarRocksConnectorException("Failed to get hive remote file's metadata on path: %s. msg: %s",
                    pathKey, e.getMessage());
        }

        return resultPartitions.put(pathKey, fileDescs).build();
    }

    private boolean isValidDataFile(FileStatus fileStatus) {
        if (fileStatus.isDirectory()) {
            return false;
        }

        String lcFileName = fileStatus.getPath().getName().toLowerCase();
        return !(lcFileName.startsWith(".") || lcFileName.startsWith("_") ||
                lcFileName.endsWith(".copying") || lcFileName.endsWith(".tmp"));
    }

    protected List<RemoteFileBlockDesc> getRemoteFileBlockDesc(BlockLocation[] blockLocations) throws IOException {
        List<RemoteFileBlockDesc> fileBlockDescs = Lists.newArrayList();
        for (BlockLocation blockLocation : blockLocations) {
            fileBlockDescs.add(buildRemoteFileBlockDesc(
                    blockLocation.getOffset(),
                    blockLocation.getLength(),
                    getReplicaHostIds(blockLocation.getNames()))
            );
        }
        return fileBlockDescs;
    }

    public RemoteFileBlockDesc buildRemoteFileBlockDesc(long offset, long length, long[] replicaHostIds) {
        return new RemoteFileBlockDesc(offset,
                length,
                replicaHostIds,
                new long[] {UNKNOWN_STORAGE_ID},
                this);
    }

    public long[] getReplicaHostIds(String[] hostNames) {
        long[] replicaHostIds = new long[hostNames.length];
        for (int j = 0; j < hostNames.length; j++) {
            String name = hostNames[j];
            replicaHostIds[j] = getHostId(name);
        }
        return replicaHostIds;
    }

    public long getHostId(String hostName) {
        return blockHostToId.computeIfAbsent(hostName, k -> {
            long newId = hostId++;
            idToBlockHost.put(newId, hostName);
            return newId;
        });
    }

    public String getHdfsDataNodeIp(long hostId) {
        String hostPort = idToBlockHost.get(hostId);
        return hostPort.split(":")[0];
    }

    @VisibleForTesting
    public void setFileSystem(FileSystem fs) {
        this.fileSystem = fs;
    }
}
