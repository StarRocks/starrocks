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

package com.starrocks.http.model;

import com.starrocks.http.rest.RestBaseResult;

import java.util.List;
import java.util.Map;

public class TableDetailResult extends RestBaseResult {

    private TableSchemaInfoDto table;

    public TableDetailResult(TableSchemaInfoDto tableSchemaInfoDto) {
        this.table = tableSchemaInfoDto;
    }

    public static class TableSchemaInfoDto {
        private String engineType;
        private SchemaInfoDto schemaInfo;
        private DistributionInfoDto distributionInfo;
        private PartitionInfoDto partitionInfo;
        private Map<String, String> properties;

        public String getEngineType() {
            return engineType;
        }

        public void setEngineType(String engineType) {
            this.engineType = engineType;
        }

        public SchemaInfoDto getSchemaInfo() {
            return schemaInfo;
        }

        public void setSchemaInfo(SchemaInfoDto schemaInfo) {
            this.schemaInfo = schemaInfo;
        }

        public DistributionInfoDto getDistributionInfo() {
            return distributionInfo;
        }

        public void setDistributionInfo(DistributionInfoDto distributionInfo) {
            this.distributionInfo = distributionInfo;
        }

        public PartitionInfoDto getPartitionInfo() {
            return partitionInfo;
        }

        public void setPartitionInfo(PartitionInfoDto partitionInfo) {
            this.partitionInfo = partitionInfo;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }
    }

    public static class SchemaInfoDto {
        private Map<String, TableSchemaDto> schemaMap;

        public Map<String, TableSchemaDto> getSchemaMap() {
            return schemaMap;
        }

        public void setSchemaMap(Map<String, TableSchemaDto> schemaMap) {
            this.schemaMap = schemaMap;
        }
    }

    public static class TableSchemaDto {
        private List<SchemaDto> schemaList;
        private boolean isBaseIndex;
        private String keyType;

        public List<SchemaDto> getSchemaList() {
            return schemaList;
        }

        public void setSchemaList(List<SchemaDto> schemaList) {
            this.schemaList = schemaList;
        }

        public boolean isBaseIndex() {
            return isBaseIndex;
        }

        public void setBaseIndex(boolean baseIndex) {
            isBaseIndex = baseIndex;
        }

        public String getKeyType() {
            return keyType;
        }

        public void setKeyType(String keyType) {
            this.keyType = keyType;
        }
    }

    public static class SchemaDto {
        private String field;
        private String type;
        private String isNull;
        private String defaultVal;
        private String key;
        private String aggrType;
        private String comment;

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getIsNull() {
            return isNull;
        }

        public void setIsNull(String isNull) {
            this.isNull = isNull;
        }

        public String getDefaultVal() {
            return defaultVal;
        }

        public void setDefaultVal(String defaultVal) {
            this.defaultVal = defaultVal;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getAggrType() {
            return aggrType;
        }

        public void setAggrType(String aggrType) {
            this.aggrType = aggrType;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }
    }

    public static class PartitionInfoDto {
        private String partitionType;
        private List<String> partitionColumns;

        public String getPartitionType() {
            return partitionType;
        }

        public void setPartitionType(String partitionType) {
            this.partitionType = partitionType;
        }

        public List<String> getPartitionColumns() {
            return partitionColumns;
        }

        public void setPartitionColumns(List<String> partitionColumns) {
            this.partitionColumns = partitionColumns;
        }
    }

    public static class DistributionInfoDto {
        private String distributionInfoType;
        private Integer bucketNum;
        private List<String> distributionColumns;

        public String getDistributionInfoType() {
            return distributionInfoType;
        }

        public void setDistributionInfoType(String distributionInfoType) {
            this.distributionInfoType = distributionInfoType;
        }

        public Integer getBucketNum() {
            return bucketNum;
        }

        public void setBucketNum(Integer bucketNum) {
            this.bucketNum = bucketNum;
        }

        public List<String> getDistributionColumns() {
            return distributionColumns;
        }

        public void setDistributionColumns(List<String> distributionColumns) {
            this.distributionColumns = distributionColumns;
        }
    }
}
