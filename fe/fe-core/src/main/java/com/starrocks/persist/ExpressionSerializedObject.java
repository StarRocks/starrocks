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

public class ExpressionSerializedObject {
    public ExpressionSerializedObject(String expressionSql) {
        this.expressionSql = expressionSql;
    }

    @SerializedName("expr")
    private String expressionSql;

    public static ExpressionSerializedObject create(ColumnIdExpr expr) {
        String sql = expr.toSql();
        // Use fromSql to check the correctness of SQL and avoid storing incorrect SQL in persistent metadata.
        ColumnIdExpr.fromSql(sql);
        return new ExpressionSerializedObject(sql);
    }

    public ColumnIdExpr deserialize() {
        return ColumnIdExpr.fromSql(expressionSql);
    }

    public String getExpressionSql() {
        return expressionSql;
    }
}
