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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/CreateTableInfo.java

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
import com.starrocks.catalog.Table;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CreateTableInfo implements Writable {
    public static final Logger LOG = LoggerFactory.getLogger(CreateTableInfo.class);

    @SerializedName(value = "d")
    private String dbName;
    @SerializedName(value = "t")
    private Table table;
    @SerializedName(value = "svId")
    private String storageVolumeId;

    public CreateTableInfo() {
        // for persist
    }

    public CreateTableInfo(String dbName, Table table, String storageVolumeId) {
        this.dbName = dbName;
        this.table = table;
        this.storageVolumeId = storageVolumeId;
    }

    public String getDbName() {
        return dbName;
    }

    public Table getTable() {
        return table;
    }

    public String getStorageVolumeId() {
        return storageVolumeId;
    }

    public void write(DataOutput out) throws IOException {
        // compatible with old version
        Text.writeString(out, ClusterNamespace.getFullName(dbName));
        table.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        dbName = ClusterNamespace.getNameFromFullName(Text.readString(in));

        table = Table.read(in);
    }

    public static CreateTableInfo read(DataInput in) throws IOException {
        CreateTableInfo createTableInfo = new CreateTableInfo();
        createTableInfo.readFields(in);
        return createTableInfo;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dbName, table);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CreateTableInfo)) {
            return false;
        }

        CreateTableInfo info = (CreateTableInfo) obj;

        return (dbName.equals(info.dbName))
                && (table.equals(info.table));
    }
}
