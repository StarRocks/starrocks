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

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.JsonWriter;

import java.util.List;

public class PartitionVersionRecoveryInfo extends JsonWriter {

    @SerializedName("pList")
    private List<PartitionVersion> partitionVersions;
    @SerializedName("rt")
    private long recoverTime;

    // for GSON deserialize
    public PartitionVersionRecoveryInfo() {
    }

    public PartitionVersionRecoveryInfo(List<PartitionVersion> partitionVersions, long recoverTime) {
        this.partitionVersions = partitionVersions;
        this.recoverTime = recoverTime;
    }

    public List<PartitionVersion> getPartitionVersions() {
        return this.partitionVersions;
    }

    public long getRecoverTime() {
        return recoverTime;
    }

    public static class PartitionVersion {
        @SerializedName("dbId")
        private long dbId;
        @SerializedName("tId")
        private long tableId;
        @SerializedName("pId")
        private long partitionId;
        @SerializedName("v")
        private long version;

        // for GSON deserialize
        public PartitionVersion() {

        }

        public PartitionVersion(long dbId, long tableId, long partitionId, long version) {
            this.dbId = dbId;
            this.tableId = tableId;
            this.partitionId = partitionId;
            this.version = version;
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

        public long getVersion() {
            return version;
        }

        public void setDbId(long dbId) {
            this.dbId = dbId;
        }

        public void setTableId(long tableId) {
            this.tableId = tableId;
        }

        public void setPartitionId(long partitionId) {
            this.partitionId = partitionId;
        }

        public void setVersion(long version) {
            this.version = version;
        }

        @Override
        public String toString() {
            return String.format("dbId=%d, tableId=%d, partitionId=%d, version=%d",
                    dbId, tableId, partitionId, version);
        }
    }
}
