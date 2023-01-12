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

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.OriginStatement;

import java.io.DataOutput;
import java.io.IOException;

public class MaterializedColumnMeta implements Writable, GsonPostProcessable {
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "defineStmt")
    private OriginStatement defineStmt;

    private Expr defineExpr;

    private Column column;

    MaterializedColumnMeta(long tableId, OriginStatement defineStmt, Expr expr, Column column) {
        this.tableId = tableId;
        this.defineStmt = defineStmt;
        this.defineExpr = expr;
        this.column = column;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // TODO: do statement parsing here.
    }

    public String getDefineStmt() {
        return defineStmt.originStmt;
    }

    public Column getColumn() {
        return this.column;
    }

    public Expr getDefineExpr() {
        return this.defineExpr;
    }
}
