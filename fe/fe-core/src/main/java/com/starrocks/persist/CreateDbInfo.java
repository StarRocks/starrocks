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
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CreateDbInfo implements Writable {
    @SerializedName(value = "i")
    private long id;

    @SerializedName(value = "n")
    private String dbName;

    @SerializedName(value = "svId")
    private String storageVolumeId;

    public CreateDbInfo(long id, String dbName) {
        this.id = id;
        this.dbName = dbName;
    }

    public long getId() {
        return id;
    }

    public String getDbName() {
        return dbName;
    }

    public void setStorageVolumeId(String storageVolumeId) {
        this.storageVolumeId = storageVolumeId;
    }

    public String getStorageVolumeId() {
        return storageVolumeId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static CreateDbInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CreateDbInfo.class);
    }
}
