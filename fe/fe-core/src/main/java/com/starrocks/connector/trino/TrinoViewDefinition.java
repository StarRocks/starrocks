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

package com.starrocks.connector.trino;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class TrinoViewDefinition {
    @SerializedName(value = "originalSql")
    private final String originalSql;

    @SerializedName(value = "catalog")
    private final String catalog;

    @SerializedName(value = "schema")
    private final String schema;

    @SerializedName(value = "columns")
    private final List<ViewColumn> columns;

    @SerializedName(value = "comment")
    private final String comment;

    @SerializedName(value = "owner")
    private final String owner;

    @SerializedName(value = "runAsInvoker")
    private final boolean runAsInvoker;

    public TrinoViewDefinition(String originalSql, String catalog, String schema,
                               List<ViewColumn> columns, String comment, String owner,
                               boolean runAsInvoker) {
        this.originalSql = originalSql;
        this.catalog = catalog;
        this.schema = schema;
        this.columns = columns;
        this.comment = comment;
        this.owner = owner;
        this.runAsInvoker = runAsInvoker;
    }

    public String getOriginalSql() {
        return originalSql;
    }

    public List<ViewColumn> getColumns() {
        return columns;
    }

    public static final class ViewColumn {
        @SerializedName(value = "name")
        private final String name;

        @SerializedName(value = "type")
        private final String type;

        @SerializedName(value = "comment")
        private final String comment;

        public ViewColumn(String name, String type) {
            this(name, type, null);
        }

        public ViewColumn(String name, String type, String comment) {
            this.name = name;
            this.type = type;
            this.comment = comment;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }
    }
}
