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

import com.starrocks.connector.hive.HiveRemoteFileIO;

public class RemoteFileBlockDesc {
    private long offset;
    private long length;
    private long[] replicaHostIds;
    private long[] diskIds;
    private HiveRemoteFileIO remoteFileIO;

    public RemoteFileBlockDesc(long offset, long length, long[] replicaHostIds,
                               long[] diskIds, HiveRemoteFileIO remoteFileIO) {
        this.offset = offset;
        this.length = length;
        this.replicaHostIds = replicaHostIds;
        this.diskIds = diskIds;
        this.remoteFileIO = remoteFileIO;
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

    public String getDataNodeIp(long hostId) {
        return remoteFileIO.getHdfsDataNodeIp(hostId);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("RemoteFileBlockDesc{");
        sb.append("offset=").append(offset);
        sb.append(", length=").append(length);
        sb.append(", replicaHostIds=");

        if (replicaHostIds == null) {
            sb.append("null");
        } else {
            sb.append('[');
            for (int i = 0; i < replicaHostIds.length; ++i) {
                sb.append(i == 0 ? "" : ", ").append(replicaHostIds[i]);
                sb.append(']');
            }
        }
        sb.append(", diskIds=");
        if (diskIds == null) {
            sb.append("null");
        } else {
            sb.append('[');
            for (int i = 0; i < diskIds.length; ++i) {
                sb.append(i == 0 ? "" : ", ").append(diskIds[i]);
                sb.append(']');
            }
        }
        sb.append(", remoteFileIO=").append(remoteFileIO);
        sb.append('}');
        return sb.toString();
    }
}
