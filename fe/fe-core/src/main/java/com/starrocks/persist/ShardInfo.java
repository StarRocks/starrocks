// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist;

import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

public class ShardInfo implements Writable {

    @SerializedName(value = "shardIds")
    private Set<Long> shardIds;

    public ShardInfo() {
        this.shardIds = Sets.newHashSet();
    }

    public ShardInfo(Set<Long> ids) {
        this.shardIds = ids;
    }

    public Set<Long> getShardIds() {
        return this.shardIds;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static ShardInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ShardInfo.class);
    }

}




