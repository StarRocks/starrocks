//  Copyright 2021-present StarRocks, Inc. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.starrocks.load.pipe;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class PipeId {
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "id")
    private long id;

    public PipeId(long dbId, long id) {
        this.dbId = dbId;
        this.id = id;
    }

    public long getDbId() {
        return dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PipeId pipeId = (PipeId) o;
        return dbId == pipeId.dbId && id == pipeId.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbId, id);
    }

    @Override
    public String toString() {
        return "PipeId{" +
                "dbId=" + dbId +
                ", id=" + id +
                '}';
    }
}
