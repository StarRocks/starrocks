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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/TableAddOrDropColumnsInfo.java

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

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Index;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * PersistInfo for Table properties
 */
public class TableAddOrDropColumnsInfo implements Writable {

    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "indexSchemaMap")
    private Map<Long, List<Column>> indexSchemaMap;
    @SerializedName(value = "indexes")
    private List<Index> indexes;
    @SerializedName(value = "jobId")
    private long jobId;
    @SerializedName(value = "txnId")
    private long txnId;
    @SerializedName(value = "indexToNewSchemaId")
    private Map<Long, Long> indexToNewSchemaId;

    public TableAddOrDropColumnsInfo(long dbId, long tableId, Map<Long, List<Column>> indexSchemaMap,
                                     List<Index> indexes, long jobId, long txnId,
                                     Map<Long, Long> indexToNewSchemaId) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.indexSchemaMap = indexSchemaMap;
        this.indexes = indexes;
        this.jobId = jobId;
        this.txnId = txnId;
        this.indexToNewSchemaId = indexToNewSchemaId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public Map<Long, List<Column>> getIndexSchemaMap() {
        return indexSchemaMap;
    }

    public List<Index> getIndexes() {
        return indexes;
    }

    public long getJobId() {
        return jobId;
    }

    public long getTxnId() {
        return txnId;
    }
    
    public Map<Long, Long> getIndexToNewSchemaId() {
        return indexToNewSchemaId;
    }



    public static TableAddOrDropColumnsInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), TableAddOrDropColumnsInfo.class);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof TableAddOrDropColumnsInfo)) {
            return false;
        }

        TableAddOrDropColumnsInfo info = (TableAddOrDropColumnsInfo) obj;

        return (dbId == info.dbId && tableId == info.tableId
                && indexSchemaMap.equals(info.indexSchemaMap) && indexes.equals(info.indexes)
                && jobId == info.jobId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbId, tableId, indexSchemaMap, indexes, jobId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(" dbId: ").append(dbId);
        sb.append(" tableId: ").append(tableId);
        sb.append(" indexSchemaMap: ").append(indexSchemaMap);
        sb.append(" indexes: ").append(indexes);
        sb.append(" jobId: ").append(jobId);
        sb.append(" txnId: ").append(txnId);
        sb.append(" indexToNewSchemaId: ").append(indexToNewSchemaId);
        return sb.toString();
    }
}
