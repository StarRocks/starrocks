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
import com.starrocks.persist.proto.IndexSchemaInfoListPB;
import com.starrocks.persist.proto.IndexSchemaInfoPB;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class IndexSchemaInfo {
    // not change the SerializedName for compatibility
    @SerializedName("indexId")
    private final long indexMetaId;
    @SerializedName("indexName")
    private final String indexName;
    @SerializedName("schemaInfo")
    private final SchemaInfo schemaInfo;

    IndexSchemaInfo(long indexMetaId, String indexName, SchemaInfo schemaInfo) {
        this.indexMetaId = indexMetaId;
        this.indexName = indexName;
        this.schemaInfo = requireNonNull(schemaInfo, "schema is null");
    }

    public long getIndexMetaId() {
        return indexMetaId;
    }

    public String getIndexName() {
        return indexName;
    }

    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    /** Image v3 form. */
    public IndexSchemaInfoPB toProto() {
        IndexSchemaInfoPB.Builder builder = IndexSchemaInfoPB.newBuilder().setIndexMetaId(indexMetaId);
        if (indexName != null) {
            builder.setIndexName(indexName);
        }
        if (schemaInfo != null) {
            builder.setSchemaInfo(schemaInfo.toProto());
        }
        return builder.build();
    }

    public static IndexSchemaInfo fromProto(IndexSchemaInfoPB pb) {
        return new IndexSchemaInfo(pb.getIndexMetaId(), pb.hasIndexName() ? pb.getIndexName() : null,
                pb.hasSchemaInfo() ? SchemaInfo.fromProto(pb.getSchemaInfo()) : null);
    }

    /** A list that can be null travels in a wrapper; the caller handles the null case. */
    static IndexSchemaInfoListPB toListProto(List<IndexSchemaInfo> infos) {
        IndexSchemaInfoListPB.Builder builder = IndexSchemaInfoListPB.newBuilder();
        for (IndexSchemaInfo info : infos) {
            builder.addValues(info.toProto());
        }
        return builder.build();
    }

    static List<IndexSchemaInfo> fromListProto(IndexSchemaInfoListPB pb) {
        List<IndexSchemaInfo> infos = new ArrayList<>(pb.getValuesCount());
        for (IndexSchemaInfoPB info : pb.getValuesList()) {
            infos.add(fromProto(info));
        }
        return infos;
    }
}
