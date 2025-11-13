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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/Type.java

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
import com.starrocks.catalog.combinator.AggStateDesc;

/**
 * Abstract class describing an Impala data type (scalar/complex type).
 * Mostly contains static type instances and helper methods for convenience, as well
 * as abstract methods that subclasses must implement.
 */
public abstract class Type implements Cloneable {
    // used for nested type such as map and struct
    protected Boolean[] selectedFields;

    // Why add AggStateDesc into Type class?
    // 1. AggStateDesc is only used for combinator agg functions, and it's not persisted in Type but rather in Column.
    // 2. Combinator agg functions cannot deduce input's original type, we need this to record the original type in aggStateDesc.
    // eg:
    //  CREATE TABLE test_agg_state_table (
    //        k1  date,
    //        v0 multi_distinct_sum(double),
    //        v1 multi_distinct_sum(float),
    //        v2 multi_distinct_sum(boolean),
    //        v3 multi_distinct_sum(tinyint(4)),
    //        v4 multi_distinct_sum(smallint(6)),
    //        v5 multi_distinct_sum(int(11)),
    //        v6 multi_distinct_sum(bigint(20)),
    //        v7 multi_distinct_sum(largeint(40)),
    //        v8 multi_distinct_sum(decimal(10, 2)),
    //        v9 multi_distinct_sum(decimal(10, 2)),
    //        v10 multi_distinct_sum(decimal(10, 2)))
    //    DISTRIBUTED BY HASH(k1)
    //    PROPERTIES (  "replication_num" = "1");
    // In this case, all column types of v0...v10 are `varbinary`, only use `varbinary` type we cannot deduce the final agg type.
    // eg: select multi_distinct_sum_merge(v0), multi_distinct_sum_merge(v5) from test_agg_state_table
    // Even v0/v5's types are varbinary, but multi_distinct_sum_merge(v0) returns double,
    // multi_distinct_sum_merge(v5) returns bigint.
    // So we need to record the original column's agg state desc in type to be used in FunctionAnalyzer.
    protected AggStateDesc aggStateDesc = null;

    // Maximum nesting depth of a type. This limit may be changed after running more
    // performance tests.
    public static int MAX_NESTING_DEPTH = 15;

    /**
     * The output of this is stored directly in the hive metastore as the column type.
     * The string must match exactly.
     */
    public final String toSql() {
        return toSql(0);
    }

    /**
     * Recursive helper for toSql() to be implemented by subclasses. Keeps track of the
     * nesting depth and terminates the recursion if MAX_NESTING_DEPTH is reached.
     */
    protected abstract String toSql(int depth);

    public final String toTypeString() {
        return toTypeString(0);
    }

    protected abstract String toTypeString(int depth);

    /**
     * Same as toSql() but adds newlines and spaces for better readability of nested types.
     */
    public String prettyPrint() {
        return prettyPrint(0);
    }

    /**
     * Pretty prints this type with lpad number of leading spaces. Used to implement
     * prettyPrint() with space-indented nested types.
     */
    protected abstract String prettyPrint(int lpad);

    /**
     * Used for Nest Type
     */
    public void setSelectedField(ComplexTypeAccessPath accessPath, boolean needSetChildren) {
        throw new IllegalStateException("setSelectedField() is not implemented for type " + toSql());
    }

    /**
     * Used for Nest Type
     */
    public void selectAllFields() {
        throw new IllegalStateException("selectAllFields() is not implemented for type " + toSql());
    }

    public void pruneUnusedSubfields() {
        throw new IllegalStateException("pruneUnusedFields() is not implemented for type " + toSql());
    }

    /**
     * used for test
     */
    public Boolean[] getSelectedFields() {
        return selectedFields;
    }

    public boolean isInvalid() {
        return isScalarType(PrimitiveType.INVALID_TYPE);
    }

    public boolean isValid() {
        return !isInvalid();
    }

    public boolean isUnknown() {
        return isScalarType(PrimitiveType.UNKNOWN_TYPE);
    }

    public boolean isNull() {
        return isScalarType(PrimitiveType.NULL_TYPE);
    }

    public boolean isBoolean() {
        return isScalarType(PrimitiveType.BOOLEAN);
    }

    public boolean isDecimalV2() {
        return getPrimitiveType().isDecimalV2Type();
    }

    public boolean isChar() {
        return isScalarType(PrimitiveType.CHAR);
    }

    public boolean isVarchar() {
        return isScalarType(PrimitiveType.VARCHAR);
    }

    public boolean isWildcardDecimal() {
        return false;
    }

    public boolean isWildcardVarchar() {
        return false;
    }

    public boolean isWildcardChar() {
        return false;
    }

    public boolean isDecimalV3() {
        return getPrimitiveType().isDecimalV3Type();
    }

    public boolean isDecimalOfAnyVersion() {
        return isDecimalV2() || isDecimalV3();
    }

    public boolean isDecimal256() {
        return this.getPrimitiveType() == PrimitiveType.DECIMAL256;
    }

    public boolean isStringType() {
        return PrimitiveType.STRING_TYPE_LIST.contains(this.getPrimitiveType());
    }

    public boolean isStringArrayType() {
        return isArrayType() && ((ArrayType) this).getItemType().isStringType();
    }

    // only metric types have the following constraint:
    // 1. don't support as key column
    // 2. don't support filter
    // 3. don't support group by
    // 4. don't support index
    public boolean isOnlyMetricType() {
        return isScalarType(PrimitiveType.HLL) || isScalarType(PrimitiveType.BITMAP) ||
                isScalarType(PrimitiveType.PERCENTILE);
    }

    public boolean isValidMapKeyType() {
        return !isComplexType() && !isJsonType() && !isOnlyMetricType() && !isFunctionType();
    }

    public boolean canApplyToNumeric() {
        // TODO(mofei) support sum, avg for JSON
        return !isOnlyMetricType() && !isJsonType() && !isFunctionType() && !isBinaryType() && !isStructType() &&
                !isMapType() && !isArrayType() && !isVariantType();
    }

    public boolean canJoinOn() {
        if (isArrayType()) {
            return ((ArrayType) this).getItemType().canJoinOn();
        }
        if (isMapType()) {
            return ((MapType) this).getKeyType().canJoinOn() && ((MapType) this).getValueType().canJoinOn();
        }
        if (isStructType()) {
            for (StructField sf : ((StructType) this).getFields()) {
                if (!sf.getType().canJoinOn()) {
                    return false;
                }
            }
            return true;
        }

        return !isOnlyMetricType() && !isJsonType() && !isFunctionType() &&
                !isVariantType();
    }

    public boolean canGroupBy() {
        if (isArrayType()) {
            return ((ArrayType) this).getItemType().canGroupBy();
        }
        if (isMapType()) {
            return ((MapType) this).getKeyType().canGroupBy() && ((MapType) this).getValueType().canGroupBy();
        }
        if (isStructType()) {
            for (StructField sf : ((StructType) this).getFields()) {
                if (!sf.getType().canGroupBy()) {
                    return false;
                }
            }
            return true;
        }
        return !isOnlyMetricType() && !isJsonType() && !isFunctionType() &&
                !isVariantType();
    }

    public boolean canOrderBy() {
        // TODO(mofei) support order by for JSON
        if (isArrayType()) {
            return ((ArrayType) this).getItemType().canOrderBy();
        }
        return !isOnlyMetricType() && !isJsonType() && !isFunctionType() && !isStructType() &&
                !isMapType() && !isVariantType();
    }

    public boolean canPartitionBy() {
        // TODO(mofei) support partition by for JSON
        if (isArrayType()) {
            return ((ArrayType) this).getItemType().canPartitionBy();
        }
        return !isOnlyMetricType() && !isJsonType() && !isFunctionType() && !isBinaryType() && !isStructType() &&
                !isMapType() && !isVariantType();
    }

    public boolean canDistinct() {
        // TODO(mofei) support distinct by for JSON
        if (isArrayType()) {
            return ((ArrayType) this).getItemType().canDistinct();
        }
        if (isStructType()) {
            return ((StructType) this).getFields().stream().allMatch(sf -> sf.getType().canDistinct());
        }
        if (isMapType()) {
            return ((MapType) this).getKeyType().canDistinct() && ((MapType) this).getValueType().canDistinct();
        }
        return !isOnlyMetricType() && !isJsonType() && !isFunctionType() && !isBinaryType() && !isStructType() &&
                !isMapType() && !isVariantType();
    }

    public boolean canStatistic() {
        // TODO(mofei) support statistic by for JSON
        return !isOnlyMetricType() && !isJsonType() && !isStructType() && !isFunctionType()
                && !isBinaryType() && !isVariantType();
    }

    public boolean canDistributedBy() {
        // TODO(mofei) support distributed by for JSON
        // Allow VARBINARY as distribution key
        return !isComplexType() && !isFloatingPointType() && !isOnlyMetricType() && !isJsonType()
                && !isFunctionType() && !isVariantType();
    }

    public boolean canBeWindowFunctionArgumentTypes() {
        return !(isNull() || isChar() || isTime() || isComplexType()
                || isPseudoType() || isFunctionType() || isBinaryType());
    }

    /**
     * Can be a key of materialized view
     */
    public boolean canBeMVKey() {
        return canDistributedBy();
    }

    public boolean supportBloomFilter() {
        return isScalarType() && !isFloatingPointType() && !isTinyint() && !isBoolean() && !isDecimalV3() &&
                !isJsonType() && !isOnlyMetricType() && !isFunctionType() && !isBinaryType();
    }

    public boolean supportZoneMap() {
        return isScalarType() && (isNumericType() || isDateType() || isStringType());
    }

    public static final String NOT_SUPPORT_JOIN_ERROR_MSG =
            "Type (nested) percentile/hll/bitmap/json not support join";

    public static final String NOT_SUPPORT_GROUP_BY_ERROR_MSG =
            "Type (nested) percentile/hll/bitmap/json not support group-by";

    public static final String NOT_SUPPORT_AGG_ERROR_MSG =
            "Type (nested) percentile/hll/bitmap/json/struct/map not support this aggregation function";

    public static final String NOT_SUPPORT_ORDER_ERROR_MSG =
            "Type (nested) percentile/hll/bitmap/json/struct/map not support order-by";

    public boolean isHllType() {
        return isScalarType(PrimitiveType.HLL);
    }

    public boolean isBitmapType() {
        return isScalarType(PrimitiveType.BITMAP);
    }

    public boolean isJsonType() {
        return isScalarType(PrimitiveType.JSON);
    }

    public boolean isVariantType() {
        return isScalarType(PrimitiveType.VARIANT);
    }

    public boolean isPercentile() {
        return isScalarType(PrimitiveType.PERCENTILE);
    }

    public boolean isScalarType() {
        return this instanceof ScalarType;
    }

    public boolean isScalarType(PrimitiveType t) {
        return isScalarType() && this.getPrimitiveType() == t;
    }

    public boolean isFixedPointType() {
        return PrimitiveType.INTEGER_TYPE_LIST.contains(getPrimitiveType());
    }

    public boolean isFloatingPointType() {
        return isScalarType(PrimitiveType.FLOAT) || isScalarType(PrimitiveType.DOUBLE);
    }

    public boolean isIntegerType() {
        return isScalarType(PrimitiveType.TINYINT) || isScalarType(PrimitiveType.SMALLINT)
                || isScalarType(PrimitiveType.INT) || isScalarType(PrimitiveType.BIGINT);
    }

    public boolean isLargeIntType() {
        return isScalarType(PrimitiveType.LARGEINT);
    }

    public boolean isNumericType() {
        return isFixedPointType() || isFloatingPointType() || isDecimalV2() || isDecimalV3();
    }

    public boolean isExactNumericType() {
        return isFixedPointType() || isDecimalV2() || isDecimalV3();
    }

    public boolean isDateType() {
        return isScalarType(PrimitiveType.DATE) || isScalarType(PrimitiveType.DATETIME);
    }

    public boolean isDatetime() {
        return isScalarType(PrimitiveType.DATETIME);
    }

    public boolean isTime() {
        return isScalarType(PrimitiveType.TIME);
    }

    public boolean isComplexType() {
        return isStructType() || isCollectionType();
    }

    public boolean isCollectionType() {
        return isMapType() || isArrayType();
    }

    public boolean isMapType() {
        return this instanceof MapType;
    }

    public boolean isArrayType() {
        return this instanceof ArrayType;
    }

    public boolean isStructType() {
        return this instanceof StructType;
    }

    public boolean isDate() {
        return isScalarType(PrimitiveType.DATE);
    }

    public boolean isTinyint() {
        return isScalarType(PrimitiveType.TINYINT);
    }

    public boolean isSmallint() {
        return isScalarType(PrimitiveType.SMALLINT);
    }

    public boolean isInt() {
        return isScalarType(PrimitiveType.INT);
    }

    public boolean isBigint() {
        return isScalarType(PrimitiveType.BIGINT);
    }

    public boolean isLargeint() {
        return isScalarType(PrimitiveType.LARGEINT);
    }

    public boolean isFloat() {
        return isScalarType(PrimitiveType.FLOAT);
    }

    public boolean isDouble() {
        return isScalarType(PrimitiveType.DOUBLE);
    }

    public boolean isPseudoType() {
        return this instanceof PseudoType;
    }

    public boolean isFunctionType() {
        return isScalarType(PrimitiveType.FUNCTION);
    }

    public boolean isBinaryType() {
        return isScalarType(PrimitiveType.VARBINARY);
    }

    /**
     * Returns true if Impala supports this type in the metdata. It does not mean we
     * can manipulate data of this type. For tables that contain columns with these
     * types, we can safely skip over them.
     */
    public boolean isSupported() {
        return true;
    }

    public PrimitiveType getPrimitiveType() {
        return PrimitiveType.INVALID_TYPE;
    }

    // Return type data size, used for compute optimizer column statistics
    public int getTypeSize() {
        // TODO(ywb): compute the collection type size later.
        if (isCollectionType()) {
            return 16;
        }
        throw new IllegalStateException("getTypeSize() not implemented for type " + toSql());
    }

    /**
     * Returns true if the other can be fully compatible with this type.
     * fully compatible means that all possible values of this type can be represented by the other type,
     * and no null values will be produced if we cast this as the other.
     * This is closely related to the implementation by BE.
     *
     * @TODO: the currently implementation is conservative, we can add more rules later.
     */
    public abstract boolean isFullyCompatible(Type other);

    /**
     * Returns true if this type is equal to t, or if t is a wildcard variant of this
     * type. Subclasses should override this as appropriate. The default implementation
     * here is to avoid special-casing logic in callers for concrete types.
     */
    public boolean matchesType(Type t) {
        return false;
    }

    /**
     * Returns true if this type exceeds the MAX_NESTING_DEPTH, false otherwise.
     */
    public boolean exceedsMaxNestingDepth() {
        return exceedsMaxNestingDepth(0);
    }

    /**
     * Helper for exceedsMaxNestingDepth(). Recursively computes the max nesting depth,
     * terminating early if MAX_NESTING_DEPTH is reached. Returns true if this type
     * exceeds the MAX_NESTING_DEPTH, false otherwise.
     * <p>
     * Examples of types and their nesting depth:
     * INT --> 1
     * STRUCT<f1:INT> --> 2
     * STRUCT<f1:STRUCT<f2:INT>> --> 3
     * ARRAY<INT> --> 2
     * ARRAY<STRUCT<f1:INT>> --> 3
     * MAP<STRING,INT> --> 2
     * MAP<STRING,STRUCT<f1:INT>> --> 3
     */
    private boolean exceedsMaxNestingDepth(int d) {
        if (d >= MAX_NESTING_DEPTH) {
            return true;
        }
        if (isStructType()) {
            StructType structType = (StructType) this;
            for (StructField f : structType.getFields()) {
                if (f.getType().exceedsMaxNestingDepth(d + 1)) {
                    return true;
                }
            }
        } else if (isArrayType()) {
            ArrayType arrayType = (ArrayType) this;
            return arrayType.getItemType().exceedsMaxNestingDepth(d + 1);
        } else if (isMapType()) {
            MapType mapType = (MapType) this;
            return mapType.getValueType().exceedsMaxNestingDepth(d + 1);
        } else {
            Preconditions.checkState(isScalarType());
        }
        return false;
    }

    /**
     * JDBC data type description
     * Returns the column size for this type.
     * For numeric data this is the maximum precision.
     * For character data this is the length in characters.
     * For datetime types this is the length in characters of the String representation
     * (assuming the maximum allowed precision of the fractional seconds component).
     * For binary data this is the length in bytes.
     * Null is returned for for data types where the column size is not applicable.
     */
    public Integer getColumnSize() {
        if (!isScalarType()) {
            return null;
        }
        if (isNumericType()) {
            return getPrecision();
        }
        ScalarType t = (ScalarType) this;
        switch (t.getPrimitiveType()) {
            case CHAR:
            case VARCHAR:
            case HLL:
                return t.getLength();
            default:
                return null;
        }
    }

    /**
     * JDBC data type description
     * For numeric types, returns the maximum precision for this type.
     * For non-numeric types, returns null.
     */
    public Integer getPrecision() {
        if (!isScalarType()) {
            return null;
        }
        ScalarType t = (ScalarType) this;
        switch (t.getPrimitiveType()) {
            case TINYINT:
                return 3;
            case SMALLINT:
                return 5;
            case INT:
                return 10;
            case BIGINT:
                return 19;
            case LARGEINT:
                return 39;
            case FLOAT:
                return 7;
            case DOUBLE:
                return 15;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                return t.decimalPrecision();
            default:
                return null;
        }
    }

    /**
     * JDBC data type description
     * Returns the number of fractional digits for this type, or null if not applicable.
     * For timestamp/time types, returns the number of digits in the fractional seconds
     * component.
     */
    public Integer getDecimalDigits() {
        if (!isScalarType()) {
            return null;
        }
        ScalarType t = (ScalarType) this;
        switch (t.getPrimitiveType()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return 0;
            case FLOAT:
                return 7;
            case DOUBLE:
                return 15;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                return t.decimalScale();
            default:
                return null;
        }
    }

    public int getIndexSize() {
        if (this.getPrimitiveType() == PrimitiveType.CHAR) {
            return ((ScalarType) this).getLength();
        } else {
            return this.getPrimitiveType().getOlapColumnIndexSize();
        }
    }

    @Override
    public Type clone() {
        try {
            Type cloned = (Type) super.clone();
            if (aggStateDesc != null) {
                cloned.setAggStateDesc(aggStateDesc.clone());
            }
            return cloned;
        } catch (CloneNotSupportedException ex) {
            throw new Error("Something impossible just happened", ex);
        }
    }

    public String canonicalName() {
        return toString();
    }

    // This is used for information_schema.COLUMNS DATA_TYPE
    public String toMysqlDataTypeString() {
        return "unknown";
    }

    // This is used for information_schema.COLUMNS COLUMN_TYPE
    public String toMysqlColumnTypeString() {
        return "unknown";
    }

    // This function is called by Column::getMaxUniqueId()
    // If type is a scalar type, it does not have field Id because scalar type does not have sub fields
    // If type is struct type, it will return the max field id(default value of field id is -1)
    // If type is array type, it will return the max field id of item type
    // if type is map type, it will return the max unique id between key type and value type
    public int getMaxUniqueId() {
        return -1;
    }

    public void setAggStateDesc(AggStateDesc aggStateDesc) {
        this.aggStateDesc = aggStateDesc;
    }

    public AggStateDesc getAggStateDesc() {
        return aggStateDesc;
    }
}
