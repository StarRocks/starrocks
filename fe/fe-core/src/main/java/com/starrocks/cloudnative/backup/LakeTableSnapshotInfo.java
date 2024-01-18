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


package com.starrocks.cloudnative.backup;

import com.google.gson.annotations.SerializedName;
import com.starrocks.backup.SnapshotInfo;

import java.util.Objects;

public class LakeTableSnapshotInfo extends SnapshotInfo {
    @SerializedName(value = "version")
    private long version;

    public LakeTableSnapshotInfo(long dbId, long tblId, long partitionId, long indexId, long tabletId,
                                 long beId, int schemaHash, long version) {
        super(dbId, tblId, partitionId, indexId, tabletId, beId, schemaHash, "", null);
        this.version = version;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        LakeTableSnapshotInfo that = (LakeTableSnapshotInfo) o;
        return version == that.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), version);
    }
}
