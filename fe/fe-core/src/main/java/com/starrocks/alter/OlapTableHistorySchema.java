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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class OlapTableHistorySchema {

    // This schema can be used by those transaction ids lower than it
    @SerializedName(value = "historyTxnIdThreshold")
    private long historyTxnIdThreshold = -1;
    // index id -> schema info. not thread safety, can be set to null concurrently
    @SerializedName(value = "schemaInfos")
    private volatile List<IndexSchemaInfo> indexSchemaInfos;

    public OlapTableHistorySchema() {
    }

    private OlapTableHistorySchema(Builder builder) {
        this.historyTxnIdThreshold = builder.historyTxnIdThreshold;
        this.indexSchemaInfos = builder.indexSchemaInfos;
    }

    public long getHistoryTxnIdThreshold() {
        return historyTxnIdThreshold;
    }

    public Optional<SchemaInfo> getSchemaByIndexId(long indexId) {
        return getSchemaInfosSafety().flatMap(
                    infos -> infos.stream()
                        .filter(schema -> schema.getIndexId() == indexId)
                        .findFirst().map(IndexSchemaInfo::getSchemaInfo)
                );
    }

    public Optional<SchemaInfo> getSchemaBySchemaId(long schemaId) {
        return getSchemaInfosSafety().flatMap(
                infos -> infos.stream()
                        .filter(schema -> schema.getSchemaInfo().getId() == schemaId)
                        .findFirst().map(IndexSchemaInfo::getSchemaInfo)
        );
    }

    public void setExpire() {
        this.indexSchemaInfos = null;
    }

    public boolean isExpired() {
        return indexSchemaInfos == null;
    }

    private Optional<List<IndexSchemaInfo>> getSchemaInfosSafety() {
        List<IndexSchemaInfo> schemaInfos = indexSchemaInfos;
        return Optional.ofNullable(schemaInfos);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private long historyTxnIdThreshold = -1;
        private List<IndexSchemaInfo> indexSchemaInfos = new ArrayList<>();

        public Builder() {
        }

        public Builder setHistoryTxnIdThreshold(long historyTxnIdThreshold) {
            this.historyTxnIdThreshold = historyTxnIdThreshold;
            return this;
        }

        public Builder addIndexSchema(IndexSchemaInfo schemaInfo) {
            indexSchemaInfos.add(schemaInfo);
            return this;
        }

        public OlapTableHistorySchema build() {
            return new OlapTableHistorySchema(this);
        }
    }
}
