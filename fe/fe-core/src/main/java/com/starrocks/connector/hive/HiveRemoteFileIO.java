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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Stack;
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
                blockIterator = listFilesRecursive(fileSystem, new Path(uri.getPath()));
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
        } catch (FileNotFoundException e) {
            LOG.warn("Hive remote file on path: {} not existed, ignore it", path, e);
        } catch (Exception e) {
            LOG.error("Failed to get hive remote file's metadata on path: {}", path, e);
            throw new StarRocksConnectorException("Failed to get hive remote file's metadata on path: %s. msg: %s",
                    pathKey, e.getMessage());
        }

        return resultPartitions.put(pathKey, fileDescs).build();
    }

    private RemoteIterator<LocatedFileStatus> listFilesRecursive(FileSystem fileSystem, Path f)
        throws FileNotFoundException, IOException {
        return new RemoteIterator<LocatedFileStatus>() {
            private Stack<RemoteIterator<LocatedFileStatus>> itors = new Stack<>();
            private RemoteIterator<LocatedFileStatus> curItor = fileSystem.listLocatedStatus(f);
            private LocatedFileStatus curFile;

            @Override
            public boolean hasNext() throws IOException {
                while (curFile == null) {
                    if (curItor.hasNext()) {
                        handleFileStat(curItor.next());
                    } else if (!itors.empty()) {
                        curItor = itors.pop();
                    } else {
                        return false;
                    }
                }
                return true;
            }

            // Process the input stat.
            // If it is a file, return the file stat.
            // If it is a valid directory, traverse it.
            private void handleFileStat(LocatedFileStatus stat) throws IOException {
                if (stat.isFile()) {
                    curFile = stat;
                } else if (isValidDirectory(stat)) {
                    try {
                        RemoteIterator<LocatedFileStatus> newDirItor = fileSystem.listLocatedStatus(stat.getPath());
                        itors.push(curItor);
                        curItor = newDirItor;
                    } catch (FileNotFoundException ignored) {
                        LOG.debug("Directory {} deleted while attempting for recursive listing", stat.getPath());
                    }
                }
            }

            @Override
            public LocatedFileStatus next() throws IOException {
                if (hasNext()) {
                    LocatedFileStatus result = curFile;
                    curFile = null;
                    return result;
                }
                throw new java.util.NoSuchElementException("No more entry in " + f);
            }
        };
    }
    private boolean isValidDataFile(FileStatus fileStatus) {
        if (!fileStatus.isFile()) {
            return false;
        }
        String lcFileName = fileStatus.getPath().getName().toLowerCase();
        return !(lcFileName.startsWith(".") || lcFileName.startsWith("_") ||
                lcFileName.endsWith(".copying") || lcFileName.endsWith(".tmp"));
    }

    private boolean isValidDirectory(FileStatus fileStatus) {
        if (!fileStatus.isDirectory()) {
            return false;
        }
        String dirName = fileStatus.getPath().getName();
        return !(dirName.startsWith("."));
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
