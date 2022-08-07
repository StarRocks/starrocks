package com.starrocks.external;

import com.google.common.collect.Lists;
import com.starrocks.external.hive.RemoteFileDesc;
import com.starrocks.external.hive.RemotePathKey;
import org.apache.hadoop.fs.BlockLocation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class RemoteFileIO {
    // blockHost is ip:port
    private final Map<String, Long> blockHostToId = new ConcurrentHashMap<>();
    private final Map<Long, String> idToBlockHost = new ConcurrentHashMap<>();
    private long hostId = 0;
    private static final int UNKNOWN_STORAGE_ID = -1;

    protected abstract Map<RemotePathKey, List<RemoteFileDesc>> getRemoteFiles(RemotePathKey pathKey);

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

    public long[] getReplicaHostIds(String[] hostNames) {
        long[] replicaHostIds = new long[hostNames.length];
        for (int j = 0; j < hostNames.length; j++) {
            String name = hostNames[j];
            replicaHostIds[j] = getHostId(name);
        }
        return replicaHostIds;
    }


    public RemoteFileBlockDesc buildRemoteFileBlockDesc(long offset, long length, long[] replicaHostIds) {
        return new RemoteFileBlockDesc(offset,
                length,
                replicaHostIds,
                // TODO get storageId through blockStorageLocation.getVolumeIds()
                // because this function is a rpc call, we give a fake value now.
                // Set it to real value, when planner needs this param.
                new long[] {UNKNOWN_STORAGE_ID},
                this);
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
}
