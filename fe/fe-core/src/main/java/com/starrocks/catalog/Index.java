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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/Index.java

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

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.IndexDef;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TIndexType;
import com.starrocks.thrift.TOlapTableIndex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Internal representation of index, including index type, name, columns and comments.
 * This class will used in olaptable
 */
public class Index implements Writable {
    @SerializedName(value = "indexName")
    private String indexName;
    @SerializedName(value = "columns")
    private List<String> columns;
    @SerializedName(value = "indexType")
    private IndexDef.IndexType indexType;
    @SerializedName(value = "comment")
    private String comment;

    public Index(String indexName, List<String> columns, IndexDef.IndexType indexType, String comment) {
        this.indexName = indexName;
        this.columns = columns;
        this.indexType = indexType;
        this.comment = comment;
    }

    public Index() {
        this.indexName = null;
        this.columns = null;
        this.indexType = null;
        this.comment = null;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public IndexDef.IndexType getIndexType() {
        return indexType;
    }

    public void setIndexType(IndexDef.IndexType indexType) {
        this.indexType = indexType;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static Index read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Index.class);
    }

    @Override
    public int hashCode() {
        return 31 * (indexName.hashCode() + columns.hashCode() + indexType.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof Index)) {
            return false;
        }

        Index other = (Index) obj;
        return Objects.equals(indexName, other.indexName) && Objects.equals(columns, other.columns)
                && Objects.equals(indexType, other.indexType);

    }

    public Index clone() {
        return new Index(indexName, new ArrayList<>(columns), indexType, comment);
    }

    @Override
    public String toString() {
        return toSql();
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder("INDEX ");
        sb.append(indexName);
        sb.append(" (");
        boolean first = true;
        for (String col : columns) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            sb.append("`" + col + "`");
        }
        sb.append(")");
        if (indexType != null) {
            sb.append(" USING ").append(indexType.toString());
        }
        if (comment != null) {
            sb.append(" COMMENT '" + comment + "'");
        }
        return sb.toString();
    }

    public TOlapTableIndex toThrift() {
        TOlapTableIndex tIndex = new TOlapTableIndex();
        tIndex.setIndex_name(indexName);
        tIndex.setColumns(columns);
        tIndex.setIndex_type(TIndexType.valueOf(indexType.toString()));
        if (columns != null) {
            tIndex.setComment(comment);
        }
        return tIndex;
    }
}
