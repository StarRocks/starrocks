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

package com.starrocks.format.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TableSchema {

    @JsonProperty("id")
    private Long id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("tableType")
    private String tableType;

    @JsonProperty("keysType")
    private String keysType;

    @JsonProperty("comment")
    private String comment;

    @JsonProperty("createTime")
    private Long createTime;

    @JsonProperty("columns")
    private List<Column> columns;

    @JsonProperty("indexMetas")
    private List<MaterializedIndexMeta> indexMetas;

    @JsonProperty("partitionInfo")
    private PartitionInfo partitionInfo;

    @JsonProperty("defaultDistributionInfo")
    private DistributionInfo defaultDistributionInfo;

    @JsonProperty("colocateGroup")
    private String colocateGroup;

    @JsonProperty("indexes")
    private List<Index> indexes;

    @JsonProperty("baseIndexId")
    private Long baseIndexId;

    @JsonProperty("maxIndexId")
    private Long maxIndexId;

    @JsonProperty("maxColUniqueId")
    private Integer maxColUniqueId;

    @JsonProperty("properties")
    private Map<String, String> properties;

    public TableSchema() {
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

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public List<MaterializedIndexMeta> getIndexMetas() {
        return indexMetas;
    }

    public void setIndexMetas(List<MaterializedIndexMeta> indexMetas) {
        this.indexMetas = indexMetas;
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public void setPartitionInfo(PartitionInfo partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

    public DistributionInfo getDefaultDistributionInfo() {
        return defaultDistributionInfo;
    }

    public void setDefaultDistributionInfo(DistributionInfo defaultDistributionInfo) {
        this.defaultDistributionInfo = defaultDistributionInfo;
    }

    public String getColocateGroup() {
        return colocateGroup;
    }

    public void setColocateGroup(String colocateGroup) {
        this.colocateGroup = colocateGroup;
    }

    public List<Index> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<Index> indexes) {
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
