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

package com.starrocks.binlog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TBinlogConfig;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BinlogConfig implements Writable {
    @SerializedName("configVersion")
    private long configVersion;

    @SerializedName("binlogEnable")
    private boolean binlogEnable;

    @SerializedName("binlogTtlSecond")
    private long binlogTtlSecond;

    @SerializedName("binlogMaxSize")
    private long binlogMaxSize;

    public static final long INVALID = -1;

    public BinlogConfig(long version, boolean binlogEnable, long binlogTtlSecond, long binlogMaxSize) {
        this.configVersion = version;
        this.binlogEnable = binlogEnable;
        this.binlogTtlSecond = binlogTtlSecond;
        this.binlogMaxSize = binlogMaxSize;
    }

    public BinlogConfig(BinlogConfig binlogConfig) {
        this.configVersion = binlogConfig.getVersion();
        this.binlogEnable = binlogConfig.getBinlogEnable();
        this.binlogTtlSecond = binlogConfig.getBinlogTtlSecond();
        this.binlogMaxSize = binlogConfig.getBinlogMaxSize();
    }

    // binlog config version starts from 0
    // -1 indicates invalid
    public BinlogConfig() {
        this(INVALID, false, Config.binlog_ttl_second, Config.binlog_max_size);
    }

    public void buildFromProperties(Map<String, String> properties) {
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_VERSION)) {
            configVersion = Long.parseLong(properties.get(
                    PropertyAnalyzer.PROPERTIES_BINLOG_VERSION));

        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE)) {
            binlogEnable = Boolean.parseBoolean(properties.get(
                    PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE));
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_TTL)) {
            binlogTtlSecond = Long.parseLong(properties.get(
                    PropertyAnalyzer.PROPERTIES_BINLOG_TTL));

        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE)) {
            binlogMaxSize = Long.parseLong(properties.get(
                    PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE));
        }
    }

    public long getVersion() {
        return configVersion;
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
        configVersion++;
    }

    public Map<String, String> toProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_VERSION, String.valueOf(configVersion));
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE, String.valueOf(binlogEnable));
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE, String.valueOf(binlogMaxSize));
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_TTL, String.valueOf(binlogTtlSecond));
        return properties;
    }

    public TBinlogConfig toTBinlogConfig() {
        TBinlogConfig tBinlogConfig = new TBinlogConfig();
        tBinlogConfig.setVersion(configVersion);
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
    @Override
    public String toString() {
        return String.format("{ binlog version : %d,\n " +
                "binlog_enable : %b,\n " +
                "binlog_ttl_second : %d,\n " +
                "binlog_max_size : %d }", configVersion, binlogEnable, binlogTtlSecond, binlogMaxSize);
    }

}