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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/system/BackendHbResponse.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.system;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Backend heartbeat response contains Backend's be port, http port and brpc port
 */
public class BackendHbResponse extends HeartbeatResponse implements Writable {

    @SerializedName(value = "beId")
    private long beId;
    @SerializedName(value = "bePort")
    private int bePort;
    @SerializedName(value = "httpPort")
    private int httpPort;
    @SerializedName(value = "brpcPort")
    private int brpcPort;

    @SerializedName(value = "starletPort")
    private int starletPort;
    @SerializedName(value = "version")
    private String version = "";
    @SerializedName(value = "cpuCores")
    private int cpuCores;
    @SerializedName(value = "rebootTime")
    private long rebootTime = -1L;
    private boolean isSetStoragePath = false;

    public BackendHbResponse() {
        super(HeartbeatResponse.Type.BACKEND);
    }

    public BackendHbResponse(long beId, int bePort, int httpPort, int brpcPort,
                             int starletPort, long hbTime, String version, int cpuCores, boolean isSetStoragePath) {
        this(beId, bePort, httpPort, brpcPort, starletPort, hbTime, version, cpuCores);
        this.isSetStoragePath = isSetStoragePath;
    }

    public BackendHbResponse(long beId, int bePort, int httpPort, int brpcPort,
                             int starletPort, long hbTime, String version, int cpuCores) {
        super(HeartbeatResponse.Type.BACKEND);
        this.beId = beId;
        this.status = HbStatus.OK;
        this.bePort = bePort;
        this.httpPort = httpPort;
        this.brpcPort = brpcPort;
        this.starletPort = starletPort;
        this.hbTime = hbTime;
        this.version = version;
        this.cpuCores = cpuCores;
    }

    public BackendHbResponse(long beId, String errMsg) {
        super(HeartbeatResponse.Type.BACKEND);
        this.status = HbStatus.BAD;
        this.beId = beId;
        this.msg = errMsg;
    }

    public long getRebootTime() {
        return rebootTime;
    }

    public void setRebootTime(long rebootTime) {
        this.rebootTime = rebootTime * 1000;
    }

    public long getBeId() {
        return beId;
    }

    public int getBePort() {
        return bePort;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public int getBrpcPort() {
        return brpcPort;
    }

    public int getStarletPort() {
        return starletPort;
    }

    public String getVersion() {
        return version;
    }

    public int getCpuCores() {
        return cpuCores;
    }

    public boolean isSetStoragePath() {
        return isSetStoragePath;
    }

    public static BackendHbResponse read(DataInput in) throws IOException {
        BackendHbResponse result = new BackendHbResponse();
        result.readFields(in);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(beId);
        out.writeInt(bePort);
        out.writeInt(httpPort);
        out.writeInt(brpcPort);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        beId = in.readLong();
        bePort = in.readInt();
        httpPort = in.readInt();
        brpcPort = in.readInt();
    }

}
