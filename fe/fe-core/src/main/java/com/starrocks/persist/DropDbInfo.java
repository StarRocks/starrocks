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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/DropDbInfo.java

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

package com.starrocks.persist;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DropDbInfo implements Writable {
    @SerializedName(value = "dbName")
    private String dbName;
    @SerializedName(value = "forceDrop")
    private boolean forceDrop = false;

    public DropDbInfo() {
        this("", false);
    }

    public DropDbInfo(String dbName, boolean forceDrop) {
        // compatible with old version
        this.dbName = ClusterNamespace.getFullName(dbName);
        if (this.dbName == null) {
            this.dbName = "";
        }
        this.forceDrop = forceDrop;
    }

    public String getDbName() {
        return ClusterNamespace.getNameFromFullName(dbName);
    }

    public boolean isForceDrop() {
        return forceDrop;
    }

    private void readFields(DataInput in) throws IOException {
        dbName = Text.readString(in);
    }

    public static DropDbInfo read(DataInput in) throws IOException {
        if (GlobalStateMgr.getCurrentStateJournalVersion() < FeMetaVersion.VERSION_89) {
            DropDbInfo info = new DropDbInfo();
            info.readFields(in);
            return info;
        } else {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, DropDbInfo.class);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dbName, forceDrop);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof DropDbInfo)) {
            return false;
        }

        DropDbInfo info = (DropDbInfo) obj;

        return (this.getDbName().equals(info.getDbName()))
                && (forceDrop == info.isForceDrop());
    }

}
