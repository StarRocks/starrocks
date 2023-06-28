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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/Column.java

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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TColumn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.starrocks.common.util.DateUtils.DATE_TIME_FORMATTER;

/**
 * This class represents the column-related metadata.
 */
public class Column implements Writable, GsonPreProcessable, GsonPostProcessable {

    public static final String CAN_NOT_CHANGE_DEFAULT_VALUE = "Can not change default value";

    @SerializedName(value = "name")
    private String name;
    @SerializedName(value = "type")
    private Type type;
    // column is key: aggregate type is null
    // column is not key and has no aggregate type: aggregate type is none
    // column is not key and has aggregate type: aggregate type is name of aggregate function.
    @SerializedName(value = "aggregationType")
    private AggregateType aggregationType;

    // if isAggregationTypeImplicit is true, the actual aggregation type will not be shown in show create table
    // the key type of table is duplicate or unique: the isAggregationTypeImplicit of value columns are true
    // other cases: the isAggregationTypeImplicit is false
    @SerializedName(value = "isAggregationTypeImplicit")
    private boolean isAggregationTypeImplicit;
    @SerializedName(value = "isKey")
    private boolean isKey;
    @SerializedName(value = "isAllowNull")
    private boolean isAllowNull;
    @SerializedName(value = "isAutoIncrement")
    private boolean isAutoIncrement;
    @SerializedName(value = "defaultValue")
    private String defaultValue;
    // this handle function like now() or simple expression
    @SerializedName(value = "defaultExpr")
    private DefaultExpr defaultExpr;
    @SerializedName(value = "comment")
    private String comment;
    @SerializedName(value = "stats")
    private ColumnStats stats;     // cardinality and selectivity etc.
    // Define expr may exist in two forms, one is analyzed, and the other is not analyzed.
    // Currently, analyzed define expr is only used when creating materialized views, so the define expr in RollupJob must be analyzed.
    // In other cases, such as define expr in `MaterializedIndexMeta`, it may not be analyzed after being relayed.
    private Expr defineExpr; // use to define column in materialize view
    @SerializedName(value = "materializedColumnExpr")
    private GsonUtils.ExpressionSerializedObject generatedColumnExprSerialized;
    private Expr materializedColumnExpr;

    public Column() {
        this.name = "";
        this.type = Type.NULL;
        this.isAggregationTypeImplicit = false;
        this.isKey = false;
        this.stats = new ColumnStats();
    }

    public Column(String name, Type dataType) {
        this(name, dataType, false, null, false, null, "");
        Preconditions.checkArgument(dataType.isValid());
    }

    public Column(String name, Type dataType, boolean isAllowNull) {
        this(name, dataType, false, null, isAllowNull, null, "");
        Preconditions.checkArgument(dataType.isValid());
    }

    public Column(String name, Type type, boolean isKey, AggregateType aggregateType, String defaultValue,
                  String comment) {
        this(name, type, isKey, aggregateType, false,
                new ColumnDef.DefaultValueDef(true, new StringLiteral(defaultValue)), comment);
    }

    public Column(String name, Type type, boolean isKey, AggregateType aggregateType,
                  ColumnDef.DefaultValueDef defaultValue,
                  String comment) {
        this(name, type, isKey, aggregateType, false, defaultValue, comment);
    }

    public Column(String name, Type type, boolean isKey, AggregateType aggregateType, boolean isAllowNull,
                  ColumnDef.DefaultValueDef defaultValueDef, String comment) {
        this.name = name;
        if (this.name == null) {
            this.name = "";
        }

        this.type = type;
        if (this.type == null) {
            this.type = Type.NULL;
        }
        Preconditions.checkArgument(this.type.isComplexType() ||
                this.type.getPrimitiveType() != PrimitiveType.INVALID_TYPE);

        this.aggregationType = aggregateType;
        this.isAggregationTypeImplicit = false;
        this.isKey = isKey;
        this.isAllowNull = isAllowNull;
        if (defaultValueDef != null) {
            if (defaultValueDef.expr instanceof StringLiteral) {
                this.defaultValue = ((StringLiteral) defaultValueDef.expr).getValue();
            } else if (defaultValueDef.expr instanceof NullLiteral) {
                // for default value is null or default value is not set the defaultExpr = null
                this.defaultExpr = null;
            } else {
                this.defaultExpr = new DefaultExpr(defaultValueDef.expr.toSql());
            }
        }
        this.isAutoIncrement = false;
        this.comment = comment;
        this.stats = new ColumnStats();
        this.materializedColumnExpr = null;
    }

    public Column(Column column) {
        this.name = column.getName();
        this.type = column.type;
        this.aggregationType = column.getAggregationType();
        this.isAggregationTypeImplicit = column.isAggregationTypeImplicit();
        this.isKey = column.isKey();
        this.isAllowNull = column.isAllowNull();
        this.defaultValue = column.getDefaultValue();
        this.comment = column.getComment();
        this.stats = column.getStats();
        this.defineExpr = column.getDefineExpr();
        this.defaultExpr = column.defaultExpr;
        Preconditions.checkArgument(this.type.isComplexType() ||
                this.type.getPrimitiveType() != PrimitiveType.INVALID_TYPE);
    }

    public void setName(String newName) {
        this.name = newName;
    }

    public String getName() {
        return this.name;
    }

    public String getNameWithoutPrefix(String prefix) {
        if (isNameWithPrefix(prefix)) {
            return name.substring(prefix.length());
        }
        return name;
    }

    public boolean isNameWithPrefix(String prefix) {
        return this.name.startsWith(prefix);
    }

    public void setIsKey(boolean isKey) {
        this.isKey = isKey;
    }

    public boolean isKey() {
        return this.isKey;
    }

    public PrimitiveType getPrimitiveType() {
        return type.getPrimitiveType();
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public int getStrLen() {
        return ((ScalarType) type).getLength();
    }

    public int getPrecision() {
        return ((ScalarType) type).getScalarPrecision();
    }

    public int getScale() {
        return ((ScalarType) type).getScalarScale();
    }

    public AggregateType getAggregationType() {
        return this.aggregationType;
    }

    public boolean isAggregated() {
        return aggregationType != null && aggregationType != AggregateType.NONE;
    }

    public boolean isAggregationTypeImplicit() {
        return this.isAggregationTypeImplicit;
    }

    public void setAggregationType(AggregateType aggregationType, boolean isAggregationTypeImplicit) {
        this.aggregationType = aggregationType;
        this.isAggregationTypeImplicit = isAggregationTypeImplicit;
    }

    public void setAggregationTypeImplicit(boolean isAggregationTypeImplicit) {
        this.isAggregationTypeImplicit = isAggregationTypeImplicit;
    }

    public boolean isAllowNull() {
        return isAllowNull;
    }

    public boolean isAutoIncrement() {
        return isAutoIncrement;
    }

    public void setIsAllowNull(boolean isAllowNull) {
        this.isAllowNull = isAllowNull;
    }

    public void setIsAutoIncrement(boolean isAutoIncrement) {
        this.isAutoIncrement = isAutoIncrement;
    }

    public DefaultExpr getDefaultExpr() {
        return defaultExpr;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getDefaultValue() {
        return this.defaultValue;
    }

    public void setStats(ColumnStats stats) {
        this.stats = stats;
    }

    public ColumnStats getStats() {
        return this.stats;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }

    // Attention: cause the remove escape character in parser phase, when you want to print the
    // comment, you need add the escape character back
    public String getDisplayComment() {
        return CatalogUtils.addEscapeCharacter(comment);
    }

    public boolean isMaterializedColumn() {
        return materializedColumnExpr != null;
    }

    public int getOlapColumnIndexSize() {
        PrimitiveType type = this.getPrimitiveType();
        if (type == PrimitiveType.CHAR) {
            return getStrLen();
        } else {
            return type.getOlapColumnIndexSize();
        }
    }

    public TColumn toThrift() {
        TColumn tColumn = new TColumn();
        tColumn.setColumn_name(this.name);
        tColumn.setIndex_len(this.getOlapColumnIndexSize());
        tColumn.setType_desc(this.type.toThrift());
        if (null != this.aggregationType) {
            tColumn.setAggregation_type(this.aggregationType.toThrift());
        }
        tColumn.setIs_key(this.isKey);
        tColumn.setIs_allow_null(this.isAllowNull);
        tColumn.setIs_auto_increment(this.isAutoIncrement);
        tColumn.setDefault_value(this.defaultValue);
        // The define expr does not need to be serialized here for now.
        // At present, only serialized(analyzed) define expr is directly used when creating a materialized view.
        // It will not be used here, but through another structure `TAlterMaterializedViewParam`.

        // scalar type or nested type
        // If this field is set, column_type will be ignored.
        tColumn.setType_desc(type.toThrift());
        return tColumn;
    }

    public void checkSchemaChangeAllowed(Column other) throws DdlException {
        if (other.isMaterializedColumn()) {
            return;
        }

        if (Strings.isNullOrEmpty(other.name)) {
            throw new DdlException("Dest column name is empty");
        }

        if (!ColumnType.isSchemaChangeAllowed(type, other.type)) {
            throw new DdlException("Can not change " + getType() + " to " + other.getType());
        }

        if (this.aggregationType != other.aggregationType) {
            throw new DdlException("Can not change aggregation type");
        }

        if (this.isAllowNull && !other.isAllowNull) {
            throw new DdlException("Can not change from nullable to non-nullable");
        }

        // Adding a default value to a column without a default value is not supported
        if (!this.isSameDefaultValue(other)) {
            throw new DdlException(CAN_NOT_CHANGE_DEFAULT_VALUE);
        }

        if ((getPrimitiveType() == PrimitiveType.VARCHAR && other.getPrimitiveType() == PrimitiveType.VARCHAR)
                || (getPrimitiveType() == PrimitiveType.CHAR && other.getPrimitiveType() == PrimitiveType.VARCHAR)
                || (getPrimitiveType() == PrimitiveType.CHAR && other.getPrimitiveType() == PrimitiveType.CHAR)) {
            if (getStrLen() > other.getStrLen()) {
                throw new DdlException("Cannot shorten string length");
            }
        }
        if (getPrimitiveType().isJsonType() && other.getPrimitiveType().isCharFamily()) {
            if (other.getStrLen() <= getPrimitiveType().getTypeSize()) {
                throw new DdlException("JSON needs minimum length of " + getPrimitiveType().getTypeSize());
            }
        }
    }

    private boolean isSameDefaultValue(Column other) {

        DefaultValueType thisDefaultValueType = this.getDefaultValueType();
        DefaultValueType otherDefaultValueType = other.getDefaultValueType();

        if (thisDefaultValueType != otherDefaultValueType) {
            return false;
        }

        if (thisDefaultValueType == DefaultValueType.VARY) {
            return this.getDefaultExpr().getExpr().equalsIgnoreCase(other.getDefaultExpr().getExpr());
        } else if (this.getDefaultValueType() == DefaultValueType.CONST) {
            if (this.getDefaultValue() != null && other.getDefaultValue() != null) {
                return this.getDefaultValue().equals(other.getDefaultValue());
            } else if (this.getDefaultExpr() != null && other.getDefaultExpr() != null) {
                return this.getDefaultExpr().getExpr().equalsIgnoreCase(other.getDefaultExpr().getExpr());
            } else {
                return false;
            }
        }
        return true;
    }

    public boolean nameEquals(String otherColName, boolean ignorePrefix) {
        if (CaseSensibility.COLUMN.getCaseSensibility()) {
            if (!ignorePrefix) {
                return name.equals(otherColName);
            } else {
                return removeNamePrefix(name).equals(removeNamePrefix(otherColName));
            }
        } else {
            if (!ignorePrefix) {
                return name.equalsIgnoreCase(otherColName);
            } else {
                return removeNamePrefix(name).equalsIgnoreCase(removeNamePrefix(otherColName));
            }
        }
    }

    public static String removeNamePrefix(String colName) {
        if (colName.startsWith(SchemaChangeHandler.SHADOW_NAME_PRFIX)) {
            return colName.substring(SchemaChangeHandler.SHADOW_NAME_PRFIX.length());
        }
        if (colName.startsWith(SchemaChangeHandler.SHADOW_NAME_PRFIX_V1)) {
            return colName.substring(SchemaChangeHandler.SHADOW_NAME_PRFIX_V1.length());
        }
        return colName;
    }

    public Expr getDefineExpr() {
        return defineExpr;
    }

    public void setDefineExpr(Expr expr) {
        defineExpr = expr;
    }

    public Expr materializedColumnExpr() {
        if (materializedColumnExpr == null) {
            return null;
        }
        return materializedColumnExpr.clone();
    }

    public void setMaterializedColumnExpr(Expr expr) {
        materializedColumnExpr = expr;
    }

    public List<SlotRef> getRefColumns() {
        List<SlotRef> slots = new ArrayList<>();
        if (defineExpr == null) {
            return null;
        } else {
            defineExpr.collect(SlotRef.class, slots);
            return slots;
        }
    }

    public List<SlotRef> getMaterializedColumnRef() {
        List<SlotRef> slots = new ArrayList<>();
        if (materializedColumnExpr == null) {
            return null;
        } else {
            materializedColumnExpr.collect(SlotRef.class, slots);
            return slots;
        }
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("`").append(name).append("` ");
        String typeStr = type.toSql();
        sb.append(typeStr).append(" ");
        if (isAggregated() && !isAggregationTypeImplicit) {
            sb.append(aggregationType.name()).append(" ");
        }
        if (isAllowNull) {
            sb.append("NULL ");
        } else {
            sb.append("NOT NULL ");
        }
        if (defaultExpr == null && isAutoIncrement) {
            sb.append("AUTO_INCREMENT ");
        } else if (defaultExpr != null) {
            if ("now()".equalsIgnoreCase(defaultExpr.getExpr())) {
                // compatible with mysql
                sb.append("DEFAULT ").append("CURRENT_TIMESTAMP").append(" ");
            } else {
                sb.append("DEFAULT ").append("(").append(defaultExpr.getExpr()).append(") ");
            }
        } else if (defaultValue != null && getPrimitiveType() != PrimitiveType.HLL &&
                getPrimitiveType() != PrimitiveType.BITMAP) {
            sb.append("DEFAULT \"").append(defaultValue).append("\" ");
        } else if (isMaterializedColumn()) {
            sb.append("AS " + materializedColumnExpr.toSql() + " ");
        }
        sb.append("COMMENT \"").append(getDisplayComment()).append("\"");

        return sb.toString();
    }

    public enum DefaultValueType {
        NULL,       // default value is not set or default value is null
        CONST,      // const expr e.g. default "1" or now() function
        VARY        // variable expr e.g. uuid() function
    }

    public DefaultValueType getDefaultValueType() {
        if (defaultValue != null) {
            return DefaultValueType.CONST;
        } else if (defaultExpr != null) {
            if ("now()".equalsIgnoreCase(defaultExpr.getExpr())) {
                return DefaultValueType.CONST;
            } else {
                return DefaultValueType.VARY;
            }
        }
        return DefaultValueType.NULL;
    }

    // if the column have a default value or default expr can be calculated like now(). return calculated value
    // else for a batch of every row different like uuid(). return null
    // consistency requires ConnectContext.get() != null to assurance
    // This function is only used to a const default value like "-1" or now().
    // If the default value is uuid(), this function is not suitable.
    public String calculatedDefaultValue() {
        if (defaultValue != null) {
            return defaultValue;
        }
        if ("now()".equalsIgnoreCase(defaultExpr.getExpr())) {
            // current transaction time
            if (ConnectContext.get() != null) {
                LocalDateTime localDateTime = Instant.ofEpochMilli(ConnectContext.get().getStartTime())
                        .atZone(TimeUtils.getTimeZone().toZoneId()).toLocalDateTime();
                return localDateTime.format(DATE_TIME_FORMATTER);
            } else {
                // should not run up here
                return LocalDateTime.now().format(DATE_TIME_FORMATTER);
            }
        }
        return null;
    }

    // if the column have a default value or default expr can be calculated like now(). return calculated value
    // else for a batch of every row different like uuid(). return null
    // require specify currentTimestamp. this will get the default value of the incoming time
    // base on the incoming time
    // This function is only used to a const default value like "-1" or now().
    // If the default value is uuid(), this function is not suitable.
    public String calculatedDefaultValueWithTime(long currentTimestamp) {
        if (defaultValue != null) {
            return defaultValue;
        }
        if ("now()".equalsIgnoreCase(defaultExpr.getExpr())) {
            LocalDateTime localDateTime = Instant.ofEpochMilli(currentTimestamp)
                    .atZone(TimeUtils.getTimeZone().toZoneId()).toLocalDateTime();
            return localDateTime.format(DATE_TIME_FORMATTER);
        }
        return null;
    }

    public String getMetaDefaultValue(List<String> extras) {
        if (defaultValue != null) {
            return defaultValue;
        } else if (defaultExpr != null) {
            if ("now()".equalsIgnoreCase(defaultExpr.getExpr())) {
                extras.add("DEFAULT_GENERATED");
                return "CURRENT_TIMESTAMP";
            }
        }
        return FeConstants.NULL_STRING;
    }

    public String toSqlWithoutAggregateTypeName() {
        StringBuilder sb = new StringBuilder();
        sb.append("`").append(name).append("` ");
        String typeStr = type.toSql();
        sb.append(typeStr).append(" ");
        if (isAllowNull) {
            sb.append("NULL ");
        } else {
            sb.append("NOT NULL ");
        }
        if (defaultExpr == null && isAutoIncrement) {
            sb.append("AUTO_INCREMENT ");
        } else if (defaultExpr != null) {
            if ("now()".equalsIgnoreCase(defaultExpr.getExpr())) {
                // compatible with mysql
                sb.append("DEFAULT ").append("CURRENT_TIMESTAMP").append(" ");
            } else {
                sb.append("DEFAULT ").append("(").append(defaultExpr.getExpr()).append(") ");
            }
        }
        if (defaultValue != null && getPrimitiveType() != PrimitiveType.HLL &&
                getPrimitiveType() != PrimitiveType.BITMAP) {
            sb.append("DEFAULT \"").append(defaultValue).append("\" ");
        }
        if (isMaterializedColumn()) {
            sb.append("AS " + materializedColumnExpr.toSql() + " ");
        }
        sb.append("COMMENT \"").append(comment).append("\"");

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.name, this.type);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof Column)) {
            return false;
        }

        Column other = (Column) obj;

        if (!this.name.equalsIgnoreCase(other.getName())) {
            return false;
        }
        if (!this.getType().equals(other.getType())) {
            return false;
        }
        if (this.aggregationType != other.getAggregationType()) {
            return false;
        }
        if (this.isAggregationTypeImplicit != other.isAggregationTypeImplicit()) {
            return false;
        }
        if (this.isKey != other.isKey()) {
            return false;
        }
        if (this.isAllowNull != other.isAllowNull) {
            return false;
        }
        if (!this.isSameDefaultValue(other)) {
            return false;
        }

        if (this.getType().isScalarType() && other.getType().isScalarType()) {
            if (this.getStrLen() != other.getStrLen()) {
                return false;
            }
            if (this.getPrecision() != other.getPrecision()) {
                return false;
            }
            if (this.getScale() != other.getScale()) {
                return false;
            }
        }

        if (this.isMaterializedColumn() && !other.isMaterializedColumn()) {
            return false;
        }
        if (this.isMaterializedColumn() &&
                !this.materializedColumnExpr().equals(other.materializedColumnExpr())) {
            return false;
        }

        return comment.equals(other.getComment());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static Column read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Column.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (generatedColumnExprSerialized != null && generatedColumnExprSerialized.expressionSql != null) {
            materializedColumnExpr = SqlParser.parseSqlToExpr(generatedColumnExprSerialized.expressionSql,
                    SqlModeHelper.MODE_DEFAULT);
        }
    }

    @Override
    public void gsonPreProcess() throws IOException {
        if (materializedColumnExpr != null) {
            generatedColumnExprSerialized = new GsonUtils.ExpressionSerializedObject(materializedColumnExpr.toSql());
        }
    }
}
