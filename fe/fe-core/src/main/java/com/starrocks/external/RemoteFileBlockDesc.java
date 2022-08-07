package com.starrocks.external;


public class RemoteFileBlockDesc {
    private long offset;
    private long length;
    private long[] replicaHostIds;
    private long[] diskIds;
    private RemoteFileIO remoteFileIO;

    public RemoteFileBlockDesc(long offset, long length, long[] replicaHostIds,
                             long[] diskIds, RemoteFileIO remoteFileIO) {
        this.offset = offset;
        this.length = length;
        this.replicaHostIds = replicaHostIds;
        this.diskIds = diskIds;
        this.remoteFileIO = remoteFileIO;
    }

    public String getDataNodeIp(long hostId) {
        return remoteFileIO.getHdfsDataNodeIp(hostId);
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

