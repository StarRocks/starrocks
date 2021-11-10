// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.external.hive;

public class HdfsFileBlockDesc {
    private long offset;
    private long length;
    private long[] replicaHostIds;
    private long[] diskIds;
    private HiveMetaClient metaClient;
    private boolean splittable;

    public HdfsFileBlockDesc(long offset, long length, long[] replicaHostIds,
                             long[] diskIds, HiveMetaClient metaClient) {
        this.offset = offset;
        this.length = length;
        this.replicaHostIds = replicaHostIds;
        this.diskIds = diskIds;
        this.metaClient = metaClient;
        this.splittable = false;
    }

    public HdfsFileBlockDesc(long offset, long length, long[] replicaHostIds,
                             long[] diskIds, HiveMetaClient metaClient, boolean splittable) {
        this(offset, length, replicaHostIds, diskIds, metaClient);
        this.splittable = splittable;
    }

    public String getDataNodeIp(long hostId) {
        return metaClient.getHdfsDataNodeIp(hostId);
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

    public long[] getReplicaHostIds() {
        return replicaHostIds;
    }

    public long[] getDiskIds() {
        return diskIds;
    }

    public boolean isSplittable() {
        return splittable;
    }
}
