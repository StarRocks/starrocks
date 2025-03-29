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
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ColumnView {

    @SerializedName("name")
    private String name;

    @SerializedName("type")
    private TypeView type;

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
                .ifPresent(type -> cvo.setType(TypeView.viewOf(type)));

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

    public static class IdView {

        @SerializedName("id")
        private String id;

        public IdView() {
            /* Default Constructor */
        }

        /**
         * Create from {@link ColumnId}
         */
        public static IdView createFrom(ColumnId columnId) {
            IdView ivo = new IdView();
            ivo.setId(columnId.getId());
            return ivo;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }

    public abstract static class TypeView {

        @SerializedName("name")
        protected String name;

        protected TypeView() {
        }

        protected static TypeView viewOf(Type type) {
            TypeView tvo;
            if (type instanceof ScalarType) {
                tvo = ScalarTypeView.createFrom((ScalarType) type);
            } else if (type instanceof ArrayType) {
                tvo = ArrayTypeView.createFrom((ArrayType) type);
            } else if (type instanceof StructType) {
                tvo = StructTypeView.createFrom((StructType) type);
            } else if (type instanceof MapType) {
                tvo = MapTypeView.createFrom((MapType) type);
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported data type: " + Optional.ofNullable(type).map(Type::canonicalName).orElse(null)
                );
            }
            return tvo;
        }

        public String getName() {
            return name;
        }

    }

    public static class ScalarTypeView extends TypeView {

        @SerializedName("typeSize")
        private Integer typeSize;

        @SerializedName("columnSize")
        private Integer columnSize;

        @SerializedName("precision")
        private Integer precision;

        @SerializedName("scale")
        private Integer scale;

        public ScalarTypeView() {
            /* Default Constructor */
        }

        /**
         * Create from {@link ScalarType}
         */
        public static ScalarTypeView createFrom(ScalarType scalarType) {
            ScalarTypeView stvo = new ScalarTypeView();
            PrimitiveType priType = scalarType.getPrimitiveType();
            stvo.name = priType.name();
            stvo.typeSize = scalarType.getTypeSize();
            stvo.columnSize = scalarType.getColumnSize();
            stvo.precision = scalarType.getScalarPrecision();
            stvo.scale = scalarType.getScalarScale();
            return stvo;
        }

        public Integer getTypeSize() {
            return typeSize;
        }

        public Integer getColumnSize() {
            return columnSize;
        }

        public Integer getPrecision() {
            return precision;
        }

        public Integer getScale() {
            return scale;
        }
    }

    public static class ArrayTypeView extends TypeView {

        public static final String TYPE_NAME = "ARRAY";

        @SerializedName("itemType")
        private TypeView itemType;

        public ArrayTypeView() {
            this.name = TYPE_NAME;
        }

        /**
         * Create from {@link ArrayType}
         */
        public static ArrayTypeView createFrom(ArrayType arrayType) {
            ArrayTypeView atvo = new ArrayTypeView();
            atvo.itemType = viewOf(arrayType.getItemType());
            return atvo;
        }

        public TypeView getItemType() {
            return itemType;
        }

    }

    public static class StructTypeView extends TypeView {

        public static final String TYPE_NAME = "STRUCT";

        @SerializedName("fields")
        private List<FieldView> fields;

        public StructTypeView() {
            this.name = TYPE_NAME;
        }

        /**
         * Create from {@link StructType}
         */
        public static StructTypeView createFrom(StructType structType) {
            StructTypeView stvo = new StructTypeView();

            List<StructField> fields = structType.getFields();
            if (CollectionUtils.isEmpty(fields)) {
                return stvo;
            }

            stvo.fields = fields.stream()
                    .map(FieldView::createFrom)
                    .collect(Collectors.toList());

            return stvo;
        }

        public static class FieldView {

            @SerializedName("name")
            private String name;

            @SerializedName("type")
            private TypeView type;

            public FieldView() {
                /* Default Constructor */
            }

            /**
             * Create from {@link StructField}
             */
            public static FieldView createFrom(StructField structField) {
                FieldView fvo = new FieldView();
                fvo.name = structField.getName();
                fvo.type = viewOf(structField.getType());
                return fvo;
            }

            public String getName() {
                return name;
            }

            public TypeView getType() {
                return type;
            }

        }

        public List<FieldView> getFields() {
            return fields;
        }

    }

    public static class MapTypeView extends TypeView {

        public static final String TYPE_NAME = "MAP";

        @SerializedName("keyType")
        private TypeView keyType;

        @SerializedName("valueType")
        private TypeView valueType;

        public MapTypeView() {
            this.name = TYPE_NAME;
        }

        /**
         * Create from {@link MapType}
         */
        public static MapTypeView createFrom(MapType mapType) {
            MapTypeView mtvo = new MapTypeView();
            mtvo.keyType = viewOf(mapType.getKeyType());
            mtvo.valueType = viewOf(mapType.getValueType());
            return mtvo;
        }

        public TypeView getKeyType() {
            return keyType;
        }

        public TypeView getValueType() {
            return valueType;
        }
    }

    /* getters & setters */

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TypeView getType() {
        return type;
    }

    public void setType(TypeView type) {
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
