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

public class ColumnRenameInfo implements Writable {
    @SerializedName("db")
    private long dbId;
    @SerializedName("tb")
    private long tableId;
    @SerializedName("cn")
    private String columnName;
    @SerializedName("ncn")
    private String newColumnName;

    public ColumnRenameInfo() {
        // for persist
    }

    public long getDbId() {
        return dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getNewColumnName() {
        return newColumnName;
    }

    public void setNewColumnName(String newColumnName) {
        this.newColumnName = newColumnName;
    }

    public ColumnRenameInfo(long dbId, long tableId, String columnName, String newColumnName) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.columnName = columnName;
        this.newColumnName = newColumnName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static ColumnRenameInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), ColumnRenameInfo.class);
    }

}
