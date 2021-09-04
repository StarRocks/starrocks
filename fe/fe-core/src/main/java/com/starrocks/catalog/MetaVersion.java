// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MetaVersion implements Writable {

    @SerializedName(value = "communityVersion")
    private int communityVersion;
    @SerializedName(value = "starrocksVersion")
    private int starrocksVersion;

    public MetaVersion() {

    }

    public MetaVersion(int communityVersion, int starrocksVersion) {
        this.communityVersion = communityVersion;
        this.starrocksVersion = starrocksVersion;
    }

    public int getCommunityVersion() {
        return communityVersion;
    }

    public int getStarRocksVersion() {
        return starrocksVersion;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String jsonStr = GsonUtils.GSON.toJson(this);
        Text.writeString(out, jsonStr);
    }

    public static MetaVersion read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, MetaVersion.class);
    }
}
