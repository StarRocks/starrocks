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
import com.starrocks.catalog.Index;
import com.starrocks.http.rest.v2.vo.ColumnView.IdView;
import com.starrocks.sql.ast.IndexDef;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class IndexView {

    @SerializedName("indexId")
    private Long indexId;

    @SerializedName("indexName")
    private String indexName;

    @SerializedName("indexType")
    private String indexType;

    @SerializedName("columns")
    private List<IdView> columns;

    @SerializedName("comment")
    private String comment;

    @SerializedName("properties")
    private Map<String, String> properties;

    public IndexView() {
    }

    /**
     * Create from {@link Index}
     */
    public static IndexView createFrom(Index index) {
        IndexView ivo = new IndexView();
        ivo.setIndexId(index.getIndexId());
        ivo.setIndexName(index.getIndexName());

        Optional.ofNullable(index.getIndexType())
                .map(IndexDef.IndexType::getDisplayName)
                .ifPresent(ivo::setIndexType);

        Optional.ofNullable(index.getColumns())
                .map(cols -> cols.stream().map(IdView::createFrom).collect(Collectors.toList()))
                .ifPresent(ivo::setColumns);

        ivo.setComment(index.getComment());
        ivo.setProperties(index.getProperties());
        return ivo;
    }

    public Long getIndexId() {
        return indexId;
    }

    public void setIndexId(Long indexId) {
        this.indexId = indexId;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public List<IdView> getColumns() {
        return columns;
    }

    public void setColumns(List<IdView> columns) {
        this.columns = columns;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
