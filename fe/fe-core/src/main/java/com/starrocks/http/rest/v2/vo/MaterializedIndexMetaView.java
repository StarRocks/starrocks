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

package com.starrocks.http.rest.v2.vo;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MaterializedIndexMeta;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class MaterializedIndexMetaView {

    @SerializedName("indexId")
    private Long indexId;

    @SerializedName("keysType")
    private String keysType;

    @SerializedName("columns")
    private List<ColumnView> columns;

    public MaterializedIndexMetaView() {
    }

    /**
     * Create from {@link MaterializedIndexMeta}
     */
    public static MaterializedIndexMetaView createFrom(MaterializedIndexMeta indexMeta) {
        MaterializedIndexMetaView imvo = new MaterializedIndexMetaView();
        imvo.setIndexId(indexMeta.getIndexId());

        Optional.ofNullable(indexMeta.getKeysType())
                .ifPresent(keysType -> imvo.setKeysType(keysType.name()));

        Optional.ofNullable(indexMeta.getSchema())
                .map(columns -> columns.stream()
                        .filter(Objects::nonNull)
                        .map(ColumnView::createFrom)
                        .collect(Collectors.toList()))
                .ifPresent(imvo::setColumns);

        return imvo;
    }

    public Long getIndexId() {
        return indexId;
    }

    public void setIndexId(Long indexId) {
        this.indexId = indexId;
    }

    public String getKeysType() {
        return keysType;
    }

    public void setKeysType(String keysType) {
        this.keysType = keysType;
    }

    public List<ColumnView> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnView> columns) {
        this.columns = columns;
    }
}
