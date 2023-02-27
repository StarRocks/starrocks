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

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class MvId {
    @SerializedName(value = "dbId")
    private final long dbId;
    @SerializedName(value = "id")
    private final long id;

    public MvId(long dbId, long id) {
        this.dbId = dbId;
        this.id = id;
    }

    public long getDbId() {
        return dbId;
    }

    public long getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MvId mvId = (MvId) o;
        return dbId == mvId.dbId && id == mvId.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbId, id);
    }

    @Override
    public String toString() {
        return "MvId{" +
                "dbId=" + dbId +
                ", id=" + id +
                '}';
    }
}