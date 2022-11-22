// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.binlog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TBinlogConfig;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BinlogConfig implements Writable {
    @SerializedName("version")
    private long version;

    @SerializedName("binlogEnable")
    private boolean binlogEnable;

    @SerializedName("binlogTtlSecond")
    private long binlogTtlSecond;

    @SerializedName("binlogMaxSize")
    private long binlogMaxSize;

    public BinlogConfig(long version, boolean binlogEnable, long binlogTtlSecond, long binlogMaxSize) {
        this.version = version;
        this.binlogEnable = binlogEnable;
        this.binlogTtlSecond = binlogTtlSecond;
        this.binlogMaxSize = binlogMaxSize;
    }

    public BinlogConfig(BinlogConfig binlogConfig) {
        this.version = binlogConfig.getVersion();
        this.binlogEnable = binlogConfig.getBinlogEnable();
        this.binlogTtlSecond = binlogConfig.getBinlogTtlSecond();
        this.binlogMaxSize = binlogConfig.getBinlogMaxSize();
    }

    public BinlogConfig(boolean binlogEnable, long binlogTtlSecond, long binlogMaxSize) {
        this(-1, binlogEnable, binlogTtlSecond, binlogMaxSize);
    }
    public BinlogConfig() {
        this(0, false, Config.task_ttl_second, Config.binlog_max_size);
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public boolean getBinlogEnable() {
        return binlogEnable;
    }

    public void setBinlogEnable(boolean binlogEnable) {
        this.binlogEnable = binlogEnable;
    }

    public long getBinlogTtlSecond() {
        return binlogTtlSecond;
    }

    public void setBinlogTtlSecond(Long binlogTtlSecond) {
        this.binlogTtlSecond = binlogTtlSecond;
    }

    public long getBinlogMaxSize() {
        return binlogMaxSize;
    }

    public void setBinlogMaxSize(Long binlogMaxSize) {
        this.binlogMaxSize = binlogMaxSize;
    }

    public void incVersion() {
        version++;
    }

    public boolean isSameParameter(BinlogConfig binlogConfig) {
        if (binlogConfig == null) {
            return false;
        }
        return binlogEnable == binlogConfig.getBinlogEnable() && binlogTtlSecond == binlogTtlSecond &&
                binlogMaxSize == binlogConfig.getBinlogMaxSize();
    }



    public TBinlogConfig toTBinlogConfig() {
        TBinlogConfig tBinlogConfig = new TBinlogConfig();
        tBinlogConfig.setVersion(version);
        tBinlogConfig.setBinlog_enable(binlogEnable);
        tBinlogConfig.setBinlog_ttl_second(binlogTtlSecond);
        tBinlogConfig.setBinlog_max_size(binlogMaxSize);
        return tBinlogConfig;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static BinlogConfig read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), BinlogConfig.class);
    }

}