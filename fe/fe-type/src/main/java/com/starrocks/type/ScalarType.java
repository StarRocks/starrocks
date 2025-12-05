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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/ScalarType.java

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

package com.starrocks.type;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

import java.util.Objects;

/**
 * Describes a scalar type. For most types this class just wraps a PrimitiveType enum,
 * but for types like CHAR and DECIMAL, this class contain additional information.
 * <p>
 * Scalar types have a few ways they can be compared to other scalar types. They can be:
 * 1. completely identical,
 * 2. implicitly castable (convertible without loss of precision)
 * 3. subtype. For example, in the case of decimal, a type can be decimal(*, *)
 * indicating that any decimal type is a subtype of the decimal type.
 */
public class ScalarType extends Type implements Cloneable {

    public static final int MAX_CHAR_LENGTH = 255;

    @SerializedName(value = "type")
    private final PrimitiveType type;

    // Only used for type CHAR.
    @SerializedName(value = "len")
    private int len = -1;

    // Only used if type is DECIMAL. -1 (for both) is used to represent a
    // decimal with any precision and scale.
    // It is invalid to have one by -1 and not the other.
    // TODO: we could use that to store DECIMAL(8,*), indicating a decimal
    // with 8 digits of precision and any valid ([0-8]) scale.
    @SerializedName(value = "precision")
    private int precision;
    @SerializedName(value = "scale")
    private int scale;

    public ScalarType(PrimitiveType type) {
        this.type = type;
    }

    public ScalarType() {
        this.type = PrimitiveType.INVALID_TYPE;
    }

    public PrimitiveType getType() {
        return type;
    }

    @Override
    public String toString() {
        if (type == PrimitiveType.CHAR) {
            if (isWildcardChar()) {
                return "CHAR";
            }
            return "CHAR(" + len + ")";
        } else if (type.isDecimalV2Type()) {
            if (isWildcardDecimal()) {
                return "DECIMAL";
            }
            return "DECIMAL(" + precision + "," + scale + ")";
        } else if (type.isDecimalV3Type()) {
            if (isWildcardDecimal()) {
                return type.toString();
            }
            return type + "(" + precision + "," + scale + ")";
        } else if (type == PrimitiveType.VARCHAR) {
            if (isWildcardVarchar()) {
                return "VARCHAR";
            }
            return "VARCHAR(" + len + ")";
        } else if (type == PrimitiveType.VARBINARY) {
            if (len == -1) {
                return "VARBINARY";
            }
            return "VARBINARY(" + len + ")";
        }
        return type.toString();
    }

    @Override
    public String toSql(int depth) {
        StringBuilder stringBuilder = new StringBuilder();
        switch (type) {
            case CHAR:
                if (len == -1) {
                    stringBuilder.append("char");
                } else {
                    stringBuilder.append("char").append("(").append(len).append(")");
                }
                break;
            case VARCHAR:
                if (len == -1) {
                    stringBuilder.append("varchar");
                } else {
                    stringBuilder.append("varchar").append("(").append(len).append(")");
                }
                break;
            case VARBINARY:
                if (len == -1) {
                    stringBuilder.append("varbinary");
                } else {
                    stringBuilder.append("varbinary").append("(").append(len).append(")");
                }
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                stringBuilder.append("decimal").append("(").append(precision).append(", ").append(scale).append(")");
                break;
            case BOOLEAN:
                return "boolean";
            case TINYINT:
                return "tinyint(4)";
            case SMALLINT:
                return "smallint(6)";
            case INT:
                return "int(11)";
            case BIGINT:
                return "bigint(20)";
            case LARGEINT:
                return "largeint(40)";
            case FLOAT:
            case DOUBLE:
            case DATE:
            case DATETIME:
            case HLL:
            case BITMAP:
            case BINARY:
            case PERCENTILE:
            case JSON:
            case VARIANT:
            case FUNCTION:
                stringBuilder.append(type.toString().toLowerCase());
                break;
            default:
                stringBuilder.append(type);
                break;
        }
        return stringBuilder.toString();
    }

    @Override
    protected String prettyPrint(int lpad) {
        return Strings.repeat(" ", lpad) + toSql();
    }

    @Override
    public boolean isFullyCompatible(Type other) {
        if (!other.isScalarType()) {
            return false;
        }

        if (this.equals(other)) {
            return true;
        }

        ScalarType t = (ScalarType) other;
        if (isBoolean()) {
            return t.isIntegerType() || t.isLargeIntType() || t.isStringType() || t.isNumericType();
        }
        if (isIntegerType() || isLargeIntType()) {
            if (t.isIntegerType() || t.isLargeIntType()) {
                return ordinal() < t.ordinal();
            }
            if (t.isStringType()) {
                return true;
            }
            return false;
        }
        if (isDecimalV3()) {
            if (t.isDecimalV3()) {
                return t.precision >= precision && t.scale >= scale;
            }
            if (t.isStringType() || t.isFloatingPointType()) {
                return true;
            }
            return false;
        }
        // both string
        if (isStringType() && t.isStringType()) {
            return true;
        }
        return false;
    }

    public int decimalPrecision() {
        Preconditions.checkState(type.isDecimalV2Type() || type.isDecimalV3Type());
        return precision;
    }

    public int decimalScale() {
        Preconditions.checkState(type.isDecimalV2Type() || type.isDecimalV3Type());
        return scale;
    }

    @Override
    public PrimitiveType getPrimitiveType() {
        return type;
    }

    public int ordinal() {
        return type.ordinal();
    }

    public int getLength() {
        return len;
    }

    public void setLength(int len) {
        this.len = len;
    }

    // Package-private setter for precision, used by TypeFactory
    void setPrecision(int precision) {
        this.precision = precision;
    }

    // Package-private setter for scale, used by TypeFactory
    void setScale(int scale) {
        this.scale = scale;
    }

    // add scalar infix to override with getPrecision
    public int getScalarScale() {
        return scale;
    }

    public int getScalarPrecision() {
        return precision;
    }

    @Override
    public boolean isWildcardDecimal() {
        return (type.isDecimalV2Type() || type.isDecimalV3Type())
                && precision == -1 && scale == -1;
    }

    @Override
    public boolean isWildcardVarchar() {
        return (type == PrimitiveType.VARCHAR || type == PrimitiveType.HLL) && len == -1;
    }

    @Override
    public boolean isWildcardChar() {
        return type == PrimitiveType.CHAR && len == -1;
    }

    @Override
    public boolean isSupported() {
        // BINARY and UNKNOWN_TYPE is unsupported
        return type != PrimitiveType.BINARY && type != PrimitiveType.UNKNOWN_TYPE;
    }

    @Override
    public int getTypeSize() {
        return type.getTypeSize();
    }

    /**
     * Returns true if this object is of type t.
     * Handles wildcard types. That is, if t is the wildcard type variant
     * of 'this', returns true.
     */
    @Override
    public boolean matchesType(Type t) {
        if (t.isPseudoType()) {
            return t.matchesType(this);
        }
        if (isDecimalV2() && t.isDecimalV2()) {
            return true;
        }
        if (this.isStringType() && t.isStringType()) {
            return true;
        }
        if (this.getPrimitiveType() == t.getPrimitiveType()) {
            Preconditions.checkArgument(t.isScalarType());
            return !this.isDecimalV3()
                    || t.isWildcardDecimal()
                    || this.isWildcardDecimal()
                    || (getScalarScale() == ((ScalarType) t).getScalarScale());
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ScalarType)) {
            return false;
        }
        ScalarType other = (ScalarType) o;
        if (type != other.type) {
            return false;
        }
        if (type.isVariableLengthType()) {
            return len == other.len;
        }
        if (type.isDecimalV2Type() || type.isDecimalV3Type()) {
            return precision == other.precision && scale == other.scale;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + Objects.hashCode(type);
        result = 31 * result + precision;
        result = 31 * result + scale;
        return result;
    }

    @Override
    public ScalarType clone() {
        return (ScalarType) super.clone();
    }

    @Override
    public String canonicalName() {
        if (isDecimalOfAnyVersion()) {
            Preconditions.checkArgument(!isWildcardDecimal());
            return String.format("DECIMAL(%d,%d)", precision, scale);
        } else {
            return toString();
        }
    }

    // This implementation is the same as BE schema_columns_scanner.cpp to_mysql_data_type_string
    public String toMysqlDataTypeString() {
        switch (type) {
            case BOOLEAN:
                return "tinyint";
            case LARGEINT:
                return "bigint unsigned";
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
            case DECIMALV2:
                return "decimal";
            default:
                return type.toString().toLowerCase();
        }
    }

    // This implementation is the same as BE schema_columns_scanner.cpp type_to_string
    public String toMysqlColumnTypeString() {
        switch (type) {
            case BOOLEAN:
                return "tinyint(1)";
            case LARGEINT:
                return "bigint(20) unsigned";
            default:
                return toSql();
        }
    }

    @Override
    protected String toTypeString(int depth) {
        StringBuilder stringBuilder = new StringBuilder();
        switch (type) {
            case DECIMALV2:
                stringBuilder.append("decimal").append("(").append(precision).append(", ").append(scale).append(")");
                break;
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                stringBuilder.append(type.toString().toLowerCase()).append("(").append(precision).append(", ")
                        .append(scale).append(")");
                break;
            default:
                stringBuilder.append(type.toString().toLowerCase());
                break;
        }
        return stringBuilder.toString();
    }
}
