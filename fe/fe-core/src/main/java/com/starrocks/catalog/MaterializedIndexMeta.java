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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/MaterializedIndexMeta.java

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

package com.starrocks.catalog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.OriginStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TStorageType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MaterializedIndexMeta implements Writable, GsonPostProcessable {

    @SerializedName(value = "indexId")
    private long indexId;
    @SerializedName(value = "schema")
    private List<Column> schema = Lists.newArrayList();
    @SerializedName(value = "sortKeyIdxes")
    public List<Integer> sortKeyIdxes = Lists.newArrayList();
    @SerializedName(value = "sortKeyUniqueIds")
    public List<Integer> sortKeyUniqueIds = Lists.newArrayList();
    @SerializedName(value = "schemaVersion")
    private int schemaVersion;
    @SerializedName(value = "schemaHash")
    private int schemaHash;
    @SerializedName(value = "shortKeyColumnCount")
    private short shortKeyColumnCount;
    @SerializedName(value = "storageType")
    private TStorageType storageType;
    @SerializedName(value = "keysType")
    private KeysType keysType;
    @SerializedName(value = "defineStmt")
    private OriginStatement defineStmt;
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "viewDefineSql")
    private String viewDefineSql;
    @SerializedName(value = "isColocateMVIndex")
    private boolean isColocateMVIndex = false;

    private Expr whereClause;

    public MaterializedIndexMeta(long indexId, List<Column> schema, int schemaVersion, int schemaHash,
                                 short shortKeyColumnCount, TStorageType storageType, KeysType keysType,
                                 OriginStatement defineStmt, List<Integer> sortKeyIdxes, List<Integer> sortKeyUniqueIds) {
        this.indexId = indexId;
        Preconditions.checkState(schema != null);
        Preconditions.checkState(schema.size() != 0);
        this.schema = schema;
        this.schemaVersion = schemaVersion;
        this.schemaHash = schemaHash;
        this.shortKeyColumnCount = shortKeyColumnCount;
        Preconditions.checkState(storageType != null);
        this.storageType = storageType;
        Preconditions.checkState(keysType != null);
        this.keysType = keysType;
        this.defineStmt = defineStmt;
        this.sortKeyIdxes = sortKeyIdxes;
        this.sortKeyUniqueIds = sortKeyUniqueIds;
    }

    public MaterializedIndexMeta(long indexId, List<Column> schema, int schemaVersion, int schemaHash,
                                 short shortKeyColumnCount, TStorageType storageType, KeysType keysType,
                                 OriginStatement defineStmt, List<Integer> sortKeyIdxes) {
        this(indexId, schema, schemaVersion, schemaHash, shortKeyColumnCount, storageType, keysType, defineStmt,
                sortKeyIdxes, null);        
    }

    public MaterializedIndexMeta(long indexId, List<Column> schema, int schemaVersion, int schemaHash,
                                 short shortKeyColumnCount, TStorageType storageType, KeysType keysType,
                                 OriginStatement defineStmt) {
        this(indexId, schema, schemaVersion, schemaHash, shortKeyColumnCount, storageType, keysType, defineStmt, null);
    }

    public long getIndexId() {
        return indexId;
    }

    public void setIndexIdForRestore(long indexId) {
        this.indexId = indexId;
    }

    public KeysType getKeysType() {
        return keysType;
    }

    public void setKeysType(KeysType keysType) {
        this.keysType = keysType;
    }

    public TStorageType getStorageType() {
        return storageType;
    }

    public List<Column> getSchema() {
        return schema;
    }

    public List<Integer> getSortKeyIdxes() {
        return sortKeyIdxes;
    }

    public void setSortKeyIdxes(List<Integer> sortKeyIdxes) {
        this.sortKeyIdxes = sortKeyIdxes;
    }

    public List<Integer> getSortKeyUniqueIds() {
        return sortKeyUniqueIds;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public short getShortKeyColumnCount() {
        return shortKeyColumnCount;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public List<Column> getNonAggregatedColumns() {
        return schema.stream().filter(column -> !column.isAggregated())
                .collect(Collectors.toList());
    }

    public String getOriginStmt() {
        if (defineStmt == null) {
            return null;
        } else {
            return defineStmt.originStmt;
        }
    }

    public void setDefineStmt(OriginStatement stmt) {
        this.defineStmt = stmt;
    }

    public OriginStatement getDefineStmt() {
        return this.defineStmt;
    }

    public long getDbId() {
        return dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public String getViewDefineSql() {
        return viewDefineSql;
    }

    public void setViewDefineSql(String viewDefineSql) {
        this.viewDefineSql = viewDefineSql;
    }

    public boolean isColocateMVIndex() {
        return isColocateMVIndex;
    }

    public void setColocateMVIndex(boolean colocateMVIndex) {
        isColocateMVIndex = colocateMVIndex;
    }

    public void setWhereClause(Expr whereClause) {
        this.whereClause = whereClause;
    }

    public Expr getWhereClause() {
        return whereClause;
    }

    // The column names of the materialized view are all lowercase, but the column names may be uppercase
    @VisibleForTesting
    public void setColumnsDefineExpr(Map<String, Expr> columnNameToDefineExpr) {
        for (Map.Entry<String, Expr> entry : columnNameToDefineExpr.entrySet()) {
            for (Column column : schema) {
                if (column.getName().equalsIgnoreCase(entry.getKey())) {
                    column.setDefineExpr(entry.getValue());
                    break;
                }
            }
        }
    }

    @Override
    public int hashCode() {
        return Long.hashCode(indexId);
    }

    public void setSchema(List<Column> newSchema) {
        this.schema = newSchema;
    }

    public void setSchemaVersion(int newSchemaVersion) {
        this.schemaVersion = newSchemaVersion;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MaterializedIndexMeta)) {
            return false;
        }
        MaterializedIndexMeta indexMeta = (MaterializedIndexMeta) obj;
        if (indexMeta.indexId != this.indexId) {
            return false;
        }
        if (indexMeta.schema.size() != this.schema.size() || !indexMeta.schema.containsAll(this.schema)) {
            return false;
        }
        if (indexMeta.schemaVersion != this.schemaVersion) {
            return false;
        }
        if (indexMeta.schemaHash != this.schemaHash) {
            return false;
        }
        if (indexMeta.shortKeyColumnCount != this.shortKeyColumnCount) {
            return false;
        }
        if (indexMeta.storageType != this.storageType) {
            return false;
        }
        if (indexMeta.keysType != this.keysType) {
            return false;
        }
        if (indexMeta.whereClause != null && this.whereClause == null) {
            return false;
        } else if (indexMeta.whereClause == null && this.whereClause != null) {
            return false;
        } else if (indexMeta.whereClause != null && this.whereClause != null &&
                !indexMeta.whereClause.equals(this.whereClause)) {
            return false;
        }
        return true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static MaterializedIndexMeta read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, MaterializedIndexMeta.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // analyze define stmt
        if (defineStmt == null) {
            return;
        }
        Map<String, Expr> columnNameToDefineExpr = MetaUtils.parseColumnNameToDefineExpr(defineStmt);
        if (columnNameToDefineExpr.containsKey(CreateMaterializedViewStmt.WHERE_PREDICATE_COLUMN_NAME)) {
            whereClause = columnNameToDefineExpr.get(CreateMaterializedViewStmt.WHERE_PREDICATE_COLUMN_NAME);
        }
        setColumnsDefineExpr(columnNameToDefineExpr);
    }
}
