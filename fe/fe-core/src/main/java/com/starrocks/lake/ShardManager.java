// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

public class ShardManager implements Writable {

    private static final Logger LOG = LogManager.getLogger(ShardManager.class);

    @SerializedName(value = "ShardDeleter")
    private final ShardDeleter shardDeleter;

    public ShardManager() {
        this.shardDeleter = new ShardDeleter();
    }

    public ShardDeleter getShardDeleter() {
        return shardDeleter;
    }

    public long saveShardManager(DataOutputStream out, long checksum) throws IOException {
        write(out);
        return checksum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static ShardManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ShardManager.class);
    }
}
