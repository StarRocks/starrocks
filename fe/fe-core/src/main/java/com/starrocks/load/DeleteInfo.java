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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/DeleteInfo.java

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

package com.starrocks.load;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.server.GlobalStateMgr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DeleteInfo implements Writable {

    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "tableName")
    private String tableName;
    @SerializedName(value = "partitionId")
    private long partitionId;
    @SerializedName(value = "partitionName")
    private String partitionName;
    @SerializedName(value = "partitionVersion")
    private long partitionVersion;
    private List<ReplicaPersistInfo> replicaInfos;

    @SerializedName(value = "deleteConditions")
    private List<String> deleteConditions;
    @SerializedName(value = "createTimeMs")
    private long createTimeMs;

    public DeleteInfo() {
        this.replicaInfos = new ArrayList<ReplicaPersistInfo>();
        this.deleteConditions = Lists.newArrayList();
    }

    public DeleteInfo(long dbId, long tableId, String tableName, long partitionId, String partitionName,
                      long partitionVersion, List<String> deleteConditions) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.tableName = tableName;
        this.partitionId = partitionId;
        this.partitionName = partitionName;
        this.partitionVersion = partitionVersion;
        this.replicaInfos = new ArrayList<ReplicaPersistInfo>();
        this.deleteConditions = deleteConditions;

        this.createTimeMs = System.currentTimeMillis();

    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public long getPartitionVersion() {
        return partitionVersion;
    }

    public List<ReplicaPersistInfo> getReplicaPersistInfos() {
        return this.replicaInfos;
    }

    public void addReplicaPersistInfo(ReplicaPersistInfo info) {
        this.replicaInfos.add(info);
    }

    public void setDeleteConditions(List<String> deleteConditions) {
        this.deleteConditions = deleteConditions;
    }

    public List<String> getDeleteConditions() {
        return deleteConditions;
    }

    public long getCreateTimeMs() {
        return createTimeMs;
    }

    public void updatePartitionVersionInfo(long newVersion) {
        this.partitionVersion = newVersion;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(dbId);
        out.writeLong(tableId);
        out.writeLong(partitionId);
        out.writeLong(partitionVersion);
        out.writeLong(0); // write a version_hash for compatibility
        out.writeInt(replicaInfos.size());
        for (ReplicaPersistInfo info : replicaInfos) {
            info.write(out);
        }

        Text.writeString(out, tableName);
        Text.writeString(out, partitionName);

        out.writeInt(deleteConditions.size());
        for (String deleteCond : deleteConditions) {
            Text.writeString(out, deleteCond);
        }

        out.writeLong(createTimeMs);

        out.writeBoolean(false);

    }

    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        partitionId = in.readLong();
        partitionVersion = in.readLong();
        in.readLong(); // read a version_hash for compatibility
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            ReplicaPersistInfo info = ReplicaPersistInfo.read(in);
            replicaInfos.add(info);
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_11) {
            tableName = Text.readString(in);
            partitionName = Text.readString(in);

            size = in.readInt();
            for (int i = 0; i < size; i++) {
                String deleteCond = Text.readString(in);
                deleteConditions.add(deleteCond);
            }

            createTimeMs = in.readLong();
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_19) {
            boolean hasAsyncDeleteJob = in.readBoolean();
            Preconditions.checkState(!hasAsyncDeleteJob, "async delete job is deprecated");
        }
    }
}
