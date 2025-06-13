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

import java.io.Serializable;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Column implements Serializable {

    private static final long serialVersionUID = -2443476992489195839L;

    @JsonProperty("name")
    private String name;

    @JsonProperty("type")
    private Type type;

    @JsonProperty("aggregationType")
    private String aggregationType;

    @JsonProperty("isKey")
    private Boolean key;

    @JsonProperty("isAllowNull")
    private Boolean allowNull;

    @JsonProperty("isAutoIncrement")
    private Boolean autoIncrement;

    @JsonProperty("defaultValueType")
    private String defaultValueType;

    @JsonProperty("defaultValue")
    private String defaultValue;

    @JsonProperty("defaultExpr")
    private String defaultExpr;

    @JsonProperty("comment")
    private String comment;

    @JsonProperty("uniqueId")
    private Integer uniqueId;

    public Column() {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Type implements Serializable {

        private static final long serialVersionUID = 5342044260068193334L;

        @JsonProperty("name")
        private String name;

        @JsonProperty("typeSize")
        private Integer typeSize;

        @JsonProperty("columnSize")
        private Integer columnSize;

        @JsonProperty("precision")
        private Integer precision;

        @JsonProperty("scale")
        private Integer scale;

        @JsonProperty("itemType")
        private Type itemType;

        @JsonProperty("named")
        private Boolean named;

        @JsonProperty("fields")
        private List<Column> fields;

        @JsonProperty("keyType")
        private Type keyType;

        @JsonProperty("valueType")
        private Type valueType;

        public Type() {

        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getTypeSize() {
            return typeSize;
        }

        public void setTypeSize(Integer typeSize) {
            this.typeSize = typeSize;
        }

        public Integer getColumnSize() {
            return columnSize;
        }

        public void setColumnSize(Integer columnSize) {
            this.columnSize = columnSize;
        }

        public Integer getPrecision() {
            return precision;
        }

        public void setPrecision(Integer precision) {
            this.precision = precision;
        }

        public Integer getScale() {
            return scale;
        }

        public void setScale(Integer scale) {
            this.scale = scale;
        }

        public Type getItemType() {
            return itemType;
        }

        public void setItemType(Type itemType) {
            this.itemType = itemType;
        }

        public Boolean getNamed() {
            return named;
        }

        public void setNamed(Boolean named) {
            this.named = named;
        }

        public List<Column> getFields() {
            return fields;
        }

        public void setFields(List<Column> fields) {
            this.fields = fields;
        }

        public Type getKeyType() {
            return keyType;
        }

        public void setKeyType(Type keyType) {
            this.keyType = keyType;
        }

        public Type getValueType() {
            return valueType;
        }

        public void setValueType(Type valueType) {
            this.valueType = valueType;
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getAggregationType() {
        return aggregationType;
    }

    public void setAggregationType(String aggregationType) {
        this.aggregationType = aggregationType;
    }

    public Boolean getKey() {
        return key;
    }

    public void setKey(Boolean key) {
        this.key = key;
    }

    public Boolean getAllowNull() {
        return allowNull;
    }

    public void setAllowNull(Boolean allowNull) {
        this.allowNull = allowNull;
    }

    public Boolean getAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(Boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public String getDefaultValueType() {
        return defaultValueType;
    }

    public void setDefaultValueType(String defaultValueType) {
        this.defaultValueType = defaultValueType;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getDefaultExpr() {
        return defaultExpr;
    }

    public void setDefaultExpr(String defaultExpr) {
        this.defaultExpr = defaultExpr;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Integer getUniqueId() {
        return uniqueId;
    }

    public void setUniqueId(Integer uniqueId) {
        this.uniqueId = uniqueId;
    }

}
