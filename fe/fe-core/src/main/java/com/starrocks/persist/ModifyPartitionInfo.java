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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/ModifyPartitionInfo.java

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
import com.starrocks.catalog.DataProperty;
import com.starrocks.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ModifyPartitionInfo implements Writable {

    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "partitionId")
    private long partitionId;
    @SerializedName(value = "dataProperty")
    private DataProperty dataProperty;
    @SerializedName(value = "replicationNum")
    private short replicationNum;
    @SerializedName(value = "isInMemory")
    private boolean isInMemory;
    @SerializedName(value = "dataCacheEnable")
    private boolean dataCacheEnable;

    public ModifyPartitionInfo() {
        // for persist
    }

    public ModifyPartitionInfo(long dbId, long tableId, long partitionId,
                               DataProperty dataProperty, short replicationNum,
                               boolean isInMemory, boolean dataCacheEnable) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.dataProperty = dataProperty;
        this.replicationNum = replicationNum;
        this.isInMemory = isInMemory;
        this.dataCacheEnable = dataCacheEnable;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public DataProperty getDataProperty() {
        return dataProperty;
    }

    public short getReplicationNum() {
        return replicationNum;
    }

    public boolean isInMemory() {
        return isInMemory;
    }

    public boolean getDataCacheEnable() {
        return dataCacheEnable;
    }

    public void setDataCacheEnable(boolean isEnable) {
        this.dataCacheEnable = isEnable;
    }

    public static ModifyPartitionInfo read(DataInput in) throws IOException {
        ModifyPartitionInfo info = new ModifyPartitionInfo();
        info.readFields(in);
        return info;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dbId, tableId);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ModifyPartitionInfo)) {
            return false;
        }
        ModifyPartitionInfo otherInfo = (ModifyPartitionInfo) other;
        return dbId == otherInfo.getDbId() && tableId == otherInfo.getTableId() &&
                dataProperty.equals(otherInfo.getDataProperty()) && replicationNum == otherInfo.getReplicationNum()
                && isInMemory == otherInfo.isInMemory() && dataCacheEnable == otherInfo.getDataCacheEnable();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(dbId);
        out.writeLong(tableId);
        out.writeLong(partitionId);

        if (dataProperty == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            dataProperty.write(out);
        }

        out.writeShort(replicationNum);
        out.writeBoolean(isInMemory);
        //Writing in and out dataCacheEnable must be commented out, otherwise there will be compatibility
        //issues when reading old logs. In fact, in the new version, persistence no longer needs to
        //be implemented through write/readFields, but through gson.
        //out.writeBoolean(dataCacheEnable);
    }

    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        partitionId = in.readLong();

        boolean hasDataProperty = in.readBoolean();
        if (hasDataProperty) {
            dataProperty = DataProperty.read(in);
        } else {
            dataProperty = null;
        }

        replicationNum = in.readShort();
        isInMemory = in.readBoolean();
        //dataCacheEnable = in.readBoolean();
    }

}
