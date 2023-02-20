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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/system/HeartbeatResponse.java

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
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This the superclass of all kinds of heartbeat response
 */
public class HeartbeatResponse implements Writable {
    public enum Type {
        FRONTEND,
        BACKEND,
        BROKER
    }

    public enum HbStatus {
        OK, BAD
    }

    public enum AliveStatus {
        ALIVE, NOT_ALIVE
    }

    @SerializedName(value = "type")
    protected Type type;
    protected boolean isTypeRead = false;

    @SerializedName(value = "status")
    protected HbStatus status;

    @SerializedName(value = "msg")
    protected String msg;

    @SerializedName(value = "hbTime")
    protected long hbTime;

    @SerializedName(value = "aliveStatus")
    public AliveStatus aliveStatus;

    public HeartbeatResponse(Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public HbStatus getStatus() {
        return status;
    }

    public String getMsg() {
        return msg;
    }

    public long getHbTime() {
        return hbTime;
    }

    public void setTypeRead(boolean isTypeRead) {
        this.isTypeRead = isTypeRead;
    }

    public static HeartbeatResponse read(DataInput in) throws IOException {
        HeartbeatResponse result = null;
        Type type = Type.valueOf(Text.readString(in));
        if (type == Type.FRONTEND) {
            result = new FrontendHbResponse();
        } else if (type == Type.BACKEND) {
            result = new BackendHbResponse();
        } else if (type == Type.BROKER) {
            result = new BrokerHbResponse();
        } else {
            throw new IOException("Unknown job type: " + type.name());
        }

        result.setTypeRead(true);
        result.readFields(in);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public void readFields(DataInput in) throws IOException {
        if (!isTypeRead) {
            type = Type.valueOf(Text.readString(in));
            isTypeRead = true;
        }

        status = HbStatus.valueOf(Text.readString(in));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("type: ").append(type.name());
        sb.append(", status: ").append(status.name());
        sb.append(", msg: ").append(msg);
        return sb.toString();
    }
}
