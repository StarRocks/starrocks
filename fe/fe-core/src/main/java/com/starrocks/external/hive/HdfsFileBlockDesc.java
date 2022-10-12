// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

public class HdfsFileBlockDesc {
    private long offset;
    private long length;
    private long[] replicaHostIds;
    private long[] diskIds;
    private HiveMetaClient metaClient;

    public HdfsFileBlockDesc(long offset, long length, long[] replicaHostIds,
                             long[] diskIds, HiveMetaClient metaClient) {
        this.offset = offset;
        this.length = length;
        this.replicaHostIds = replicaHostIds;
        this.diskIds = diskIds;
        this.metaClient = metaClient;
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
}
