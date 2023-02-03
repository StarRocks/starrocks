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
import com.starrocks.connector.persist.ConnectorTableInfo;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ConnectorTableInfoLog implements Writable {
    public enum Operation {
        ADD,
        DELETE
    }

    @SerializedName(value = "operation")
    private Operation operation;

    @SerializedName(value = "catalog")
    private String catalog;

    @SerializedName(value = "db")
    private String db;

    @SerializedName(value = "tableIdentifier")
    private String tableIdentifier;

    @SerializedName(value = "connectorTableInfo")
    private ConnectorTableInfo connectorTableInfo;

    public ConnectorTableInfoLog(Operation operation, String catalog, String db, String tableIdentifier,
                                 ConnectorTableInfo connectorTableInfo) {
        this.operation = operation;
        this.catalog = catalog;
        this.db = db;
        this.tableIdentifier = tableIdentifier;
        this.connectorTableInfo = connectorTableInfo;
    }

    @Override
    public String toString() {
        return "ConnectorTableInfoLog {" +
                "operation=" + operation +
                ", catalog=" + catalog +
                ", db=" + db +
                ", tableIdentifier=" + tableIdentifier +
                ", connectorTableInfo=" + connectorTableInfo +
                "}";
    }

    public boolean isAddOperation() {
        return operation == Operation.ADD;
    }

    public boolean isDeleteOperation() {
        return operation == Operation.DELETE;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getDb() {
        return db;
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    public ConnectorTableInfo getConnectorTableInfo() {
        return connectorTableInfo;
    }

    public static ConnectorTableInfoLog read(DataInput input) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(input), ConnectorTableInfoLog.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
}
