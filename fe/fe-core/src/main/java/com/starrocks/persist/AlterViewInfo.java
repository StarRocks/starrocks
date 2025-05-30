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

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.common.io.Writable;

import java.util.List;
import java.util.Objects;

public class AlterViewInfo implements Writable {
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "inlineViewDef")
    private String inlineViewDef;
    @SerializedName(value = "sqlMode")
    private long sqlMode;
    @SerializedName(value = "newFullSchema")
    private List<Column> newFullSchema;
    @SerializedName(value = "comment")
    private String comment;
    @SerializedName(value = "security")
    private boolean security;

    public AlterViewInfo() {
        // for persist
        newFullSchema = Lists.newArrayList();
    }

    public AlterViewInfo(long dbId, long tableId, String inlineViewDef, List<Column> newFullSchema, long sqlMode,
                         String comment) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.inlineViewDef = inlineViewDef;
        this.newFullSchema = newFullSchema;
        this.sqlMode = sqlMode;
        this.comment = comment;
    }

    public AlterViewInfo(long dbId, long tableId, boolean security) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.security = security;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    public List<Column> getNewFullSchema() {
        return newFullSchema;
    }

    public long getSqlMode() {
        return sqlMode;
    }

    public String getComment() {
        return comment;
    }

    public boolean getSecurity() {
        return security;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbId, tableId, inlineViewDef, sqlMode, newFullSchema);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof AlterViewInfo)) {
            return false;
        }
        AlterViewInfo otherInfo = (AlterViewInfo) other;
        boolean commentEqual;
        if (comment == null && otherInfo.getComment() == null) {
            commentEqual = true;
        } else if (comment != null) {
            commentEqual = comment.equalsIgnoreCase(otherInfo.getComment());
        } else {
            commentEqual = false;
        }
        return dbId == otherInfo.getDbId() && tableId == otherInfo.getTableId() &&
                inlineViewDef.equalsIgnoreCase(otherInfo.getInlineViewDef()) && sqlMode == otherInfo.getSqlMode() &&
                newFullSchema.equals(otherInfo.getNewFullSchema()) && commentEqual;
    }
}
