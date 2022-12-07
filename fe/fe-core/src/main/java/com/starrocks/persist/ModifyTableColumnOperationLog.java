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
import com.starrocks.catalog.Column;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ModifyTableColumnOperationLog implements Writable {

    @SerializedName(value = "dbName")
    private String dbName;
    @SerializedName(value = "tableName")
    private String tableName;
    @SerializedName(value = "columns")
    private List<Column> columns = new ArrayList<>();

    public ModifyTableColumnOperationLog(String dbName, String tableName, List<Column> columns) {
        // compatible with old version
        this.dbName = ClusterNamespace.getFullName(dbName);
        this.tableName = tableName;
        this.columns = columns;
    }

    public String getDbName() {
        return ClusterNamespace.getNameFromFullName(dbName);
    }

    public String getTableName() {
        return tableName;
    }

    public List<Column> getColumns() {
        return columns;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static ModifyTableColumnOperationLog read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), ModifyTableColumnOperationLog.class);
    }
}
