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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table.TableType;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class TableSchemaView {

    @SerializedName("id")
    private Long id;

    @SerializedName("name")
    private String name;

    @SerializedName("tableType")
    private String tableType;

    @SerializedName("keysType")
    private String keysType;

    @SerializedName("comment")
    private String comment;

    @SerializedName("createTime")
    private Long createTime;

    @SerializedName("columns")
    private List<ColumnView> columns;

    @SerializedName("indexMetas")
    private List<MaterializedIndexMetaView> indexMetas;

    @SerializedName("partitionInfo")
    private PartitionInfoView partitionInfo;

    @SerializedName("defaultDistributionInfo")
    private DistributionInfoView defaultDistributionInfo;

    @SerializedName("colocateGroup")
    private String colocateGroup;

    @SerializedName("indexes")
    private List<IndexView> indexes;

    @SerializedName("baseIndexId")
    private Long baseIndexId;

    @SerializedName("maxIndexId")
    private Long maxIndexId;

    @SerializedName("maxColUniqueId")
    private Integer maxColUniqueId;

    @SerializedName("properties")
    private Map<String, String> properties;

    public TableSchemaView() {
    }

    /**
     * Create from {@link OlapTable}
     */
    public static TableSchemaView createFrom(OlapTable table) {
        TableSchemaView svo = new TableSchemaView();
        svo.setId(table.getId());
        svo.setName(table.getName());
        Optional.ofNullable(table.getType())
                .ifPresent(type -> svo.setTableType(TableType.serialize(type)));
        Optional.ofNullable(table.getKeysType())
                .ifPresent(type -> svo.setKeysType(type.name()));
        svo.setComment(table.getComment());
        svo.setCreateTime(table.getCreateTime());

        Optional.ofNullable(table.getFullSchema())
                .map(columns -> columns.stream()
                        .filter(Objects::nonNull)
                        .map(ColumnView::createFrom)
                        .collect(Collectors.toList()))
                .ifPresent(svo::setColumns);

        Optional.ofNullable(table.getIndexIdToMeta())
                .map(indexIdToMetas -> indexIdToMetas.values().stream()
                        .filter(Objects::nonNull)
                        .sorted(Comparator.comparingLong(MaterializedIndexMeta::getIndexId))
                        .map(MaterializedIndexMetaView::createFrom)
                        .collect(Collectors.toList()))
                .ifPresent(svo::setIndexMetas);

        Optional.ofNullable(table.getPartitionInfo())
                .ifPresent(pi -> svo.setPartitionInfo(PartitionInfoView.createFrom(table, pi)));

        Optional.ofNullable(table.getDefaultDistributionInfo())
                .ifPresent(di -> svo.setDefaultDistributionInfo(DistributionInfoView.createFrom(table, di)));

        svo.setColocateGroup(table.getColocateGroup());

        Optional.ofNullable(table.getIndexes())
                .map(indices -> indices.stream()
                        .filter(Objects::nonNull)
                        .map(IndexView::createFrom)
                        .collect(Collectors.toList()))
                .ifPresent(svo::setIndexes);

        svo.setBaseIndexId(table.getBaseIndexId());
        svo.setMaxIndexId(table.getMaxIndexId());
        svo.setMaxColUniqueId(table.getMaxColUniqueId());

        Optional.ofNullable(table.getTableProperty())
                .ifPresent(prop -> svo.setProperties(prop.getProperties()));

        return svo;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public String getKeysType() {
        return keysType;
    }

    public void setKeysType(String keysType) {
        this.keysType = keysType;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public List<ColumnView> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnView> columns) {
        this.columns = columns;
    }

    public List<MaterializedIndexMetaView> getIndexMetas() {
        return indexMetas;
    }

    public void setIndexMetas(List<MaterializedIndexMetaView> indexMetas) {
        this.indexMetas = indexMetas;
    }

    public PartitionInfoView getPartitionInfo() {
        return partitionInfo;
    }

    public void setPartitionInfo(PartitionInfoView partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

    public DistributionInfoView getDefaultDistributionInfo() {
        return defaultDistributionInfo;
    }

    public void setDefaultDistributionInfo(DistributionInfoView defaultDistributionInfo) {
        this.defaultDistributionInfo = defaultDistributionInfo;
    }

    public String getColocateGroup() {
        return colocateGroup;
    }

    public void setColocateGroup(String colocateGroup) {
        this.colocateGroup = colocateGroup;
    }

    public List<IndexView> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<IndexView> indexes) {
        this.indexes = indexes;
    }

    public Long getBaseIndexId() {
        return baseIndexId;
    }

    public void setBaseIndexId(Long baseIndexId) {
        this.baseIndexId = baseIndexId;
    }

    public Long getMaxIndexId() {
        return maxIndexId;
    }

    public void setMaxIndexId(Long maxIndexId) {
        this.maxIndexId = maxIndexId;
    }

    public Integer getMaxColUniqueId() {
        return maxColUniqueId;
    }

    public void setMaxColUniqueId(Integer maxColUniqueId) {
        this.maxColUniqueId = maxColUniqueId;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
