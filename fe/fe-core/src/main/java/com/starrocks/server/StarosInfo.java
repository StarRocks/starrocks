// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.server;

import com.starrocks.catalog.lake.ShardDelete;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class StarosInfo {

    private static final Logger LOG = LogManager.getLogger(StarosInfo.class);

    private final ShardDelete shardDelete;

    public StarosInfo() {
        this.shardDelete = new ShardDelete();
    }

    public ShardDelete getShardDelete() {
        return shardDelete;
    }

    public long loadShardDeleteInfo(DataInputStream in, long checksum) throws IOException {
        shardDelete.read(in);
        return checksum;
    }

    public long saveShardDeleteInfo(DataOutputStream out, long checksum) throws IOException {
        shardDelete.write(out);
        return checksum;
    }
}
