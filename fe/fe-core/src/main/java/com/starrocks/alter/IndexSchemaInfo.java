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

package com.starrocks.alter;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.SchemaInfo;

import static java.util.Objects.requireNonNull;

public class IndexSchemaInfo {
    @SerializedName("indexId")
    private final long indexId;
    @SerializedName("indexName")
    private final String indexName;
    @SerializedName("schemaInfo")
    private final SchemaInfo schemaInfo;

    IndexSchemaInfo(long indexId, String indexName, SchemaInfo schemaInfo) {
        this.indexId = indexId;
        this.indexName = indexName;
        this.schemaInfo = requireNonNull(schemaInfo, "schema is null");
    }

    public long getIndexId() {
        return indexId;
    }

    public String getIndexName() {
        return indexName;
    }

    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }
}
