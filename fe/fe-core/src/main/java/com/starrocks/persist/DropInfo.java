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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/DropInfo.java

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
import com.starrocks.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DropInfo implements Writable {
    @SerializedName("db")
    private long dbId;
    @SerializedName("tb")
    private long tableId;
    @SerializedName("idx")
    private long indexId;
    @SerializedName("fd")
    private boolean forceDrop = false;

    public DropInfo() {
    }

    public DropInfo(long dbId, long tableId, long indexId, boolean forceDrop) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.indexId = indexId;
        this.forceDrop = forceDrop;
    }

    public long getDbId() {
        return this.dbId;
    }

    public long getTableId() {
        return this.tableId;
    }

    public long getIndexId() {
        return this.indexId;
    }

    public boolean isForceDrop() {
        return forceDrop;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(dbId);
        out.writeLong(tableId);
        out.writeBoolean(forceDrop);
        if (indexId == -1L) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeLong(indexId);
        }
    }

    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        forceDrop = in.readBoolean();
        boolean hasIndexId = in.readBoolean();
        if (hasIndexId) {
            indexId = in.readLong();
        } else {
            indexId = -1L;
        }
    }

    public static DropInfo read(DataInput in) throws IOException {
        DropInfo dropInfo = new DropInfo();
        dropInfo.readFields(in);
        return dropInfo;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dbId, tableId);
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof DropInfo)) {
            return false;
        }

        DropInfo info = (DropInfo) obj;

        return (dbId == info.dbId) && (tableId == info.tableId) && (indexId == info.indexId)
                && (forceDrop == info.forceDrop);
    }
}
