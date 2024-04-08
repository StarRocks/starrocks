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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;

import java.util.Optional;

public class ColumnView {

    @SerializedName("name")
    private String name;

    @SerializedName("primitiveType")
    private String primitiveType;

    @SerializedName("primitiveTypeSize")
    private Integer primitiveTypeSize;

    @SerializedName("columnSize")
    private Integer columnSize;

    @SerializedName("precision")
    private Integer precision;

    @SerializedName("scale")
    private Integer scale;

    @SerializedName("aggregationType")
    private String aggregationType;

    @SerializedName("isKey")
    private Boolean key;

    @SerializedName("isAllowNull")
    private Boolean allowNull;

    @SerializedName("isAutoIncrement")
    private Boolean autoIncrement;

    @SerializedName("defaultValueType")
    private String defaultValueType;

    @SerializedName("defaultValue")
    private String defaultValue;

    @SerializedName("defaultExpr")
    private String defaultExpr;

    @SerializedName("comment")
    private String comment;

    @SerializedName("uniqueId")
    private Integer uniqueId;

    public ColumnView() {
    }

    /**
     * Create from {@link Column}
     */
    public static ColumnView createFrom(Column column) {
        ColumnView cvo = new ColumnView();
        cvo.setName(column.getName());

        Optional.ofNullable(column.getType())
                .ifPresent(type -> {
                    PrimitiveType primitiveType = type.getPrimitiveType();
                    cvo.setPrimitiveType(primitiveType.toString());
                    cvo.setPrimitiveTypeSize(primitiveType.getTypeSize());
                    cvo.setColumnSize(type.getColumnSize());
                    if (type instanceof ScalarType) {
                        ScalarType scalarType = (ScalarType) type;
                        cvo.setPrecision(scalarType.getScalarPrecision());
                        cvo.setScale(scalarType.getScalarScale());
                    }
                });

        Optional.ofNullable(column.getAggregationType())
                .ifPresent(aggType -> cvo.setAggregationType(aggType.toSql()));

        cvo.setKey(column.isKey());
        cvo.setAllowNull(column.isAllowNull());
        cvo.setAutoIncrement(column.isAutoIncrement());
        cvo.setDefaultValueType(column.getDefaultValueType().name());
        cvo.setDefaultValue(column.getDefaultValue());

        Optional.ofNullable(column.getDefaultExpr())
                .ifPresent(defaultExpr -> cvo.setDefaultExpr(defaultExpr.getExpr()));

        cvo.setComment(column.getComment());
        cvo.setUniqueId(column.getUniqueId());
        return cvo;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPrimitiveType() {
        return primitiveType;
    }

    public void setPrimitiveType(String primitiveType) {
        this.primitiveType = primitiveType;
    }

    public Integer getPrimitiveTypeSize() {
        return primitiveTypeSize;
    }

    public void setPrimitiveTypeSize(Integer primitiveTypeSize) {
        this.primitiveTypeSize = primitiveTypeSize;
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
