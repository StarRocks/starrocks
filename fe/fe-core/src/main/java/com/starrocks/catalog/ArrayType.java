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


package com.starrocks.catalog;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

/**
 * Describes an ARRAY type.
 */
public class ArrayType extends Type {
    @SerializedName(value = "itemType")
    private Type itemType;

    public ArrayType(Type itemType) {
        this.itemType = itemType;
    }

    public Type getItemType() {
        return itemType;
    }

    @Override
    public boolean matchesType(Type t) {
        if (t.isPseudoType()) {
            return t.matchesType(this);
        }
        return t.isArrayType() && itemType.matchesType(((ArrayType) t).itemType);
    }

    @Override
    public String toSql(int depth) {
        if (depth >= MAX_NESTING_DEPTH) {
            return "array<...>";
        }
        if (itemType.isDecimalOfAnyVersion()) {
            return String.format("array<%s>", itemType);
        } else {
            return String.format("array<%s>", itemType.toSql(depth + 1));
        }
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ArrayType)) {
            return false;
        }
        ArrayType otherArrayType = (ArrayType) other;
        return otherArrayType.itemType.equals(itemType);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(itemType);
    }

    @Override
    public boolean isFullyCompatible(Type other) {
        if (!other.isArrayType()) {
            return false;
        }

        if (equals(other)) {
            return true;
        }

        ArrayType t = (ArrayType) other;
        return itemType.isFullyCompatible(t.getItemType());
    }

    @Override
    protected String prettyPrint(int lpad) {
        String leftPadding = Strings.repeat(" ", lpad);
        if (!itemType.isStructType()) {
            return leftPadding + toSql();
        }
        // Pass in the padding to make sure nested fields are aligned properly,
        // even if we then strip the top-level padding.
        String structStr = itemType.prettyPrint(lpad);
        structStr = structStr.substring(lpad);
        return String.format("%sARRAY<%s>", leftPadding, structStr);
    }

    @Override
    public String toString() {
        return String.format("ARRAY<%s>", itemType.toString());
    }

    @Override
    public ArrayType clone() {
        ArrayType clone = (ArrayType) super.clone();
        clone.itemType = this.itemType.clone();
        return clone;
    }

    @Override
    public void selectAllFields() {
        if (itemType.isComplexType()) {
            itemType.selectAllFields();
        }
    }

    public void pruneUnusedSubfields() {
        if (itemType.isComplexType()) {
            itemType.pruneUnusedSubfields();
        }
    }

    public boolean hasNumericItem() {
        return itemType.isNumericType();
    }

    public boolean isBooleanType() {
        return itemType.isBoolean();
    }

    public boolean isNullTypeItem() {
        return itemType.isNull();
    }

    public String toMysqlDataTypeString() {
        return "array";
    }

    // This implementation is the same as BE schema_columns_scanner.cpp type_to_string
    public String toMysqlColumnTypeString() {
        return toSql();
    }

    @Override
    protected String toTypeString(int depth) {
        if (depth >= MAX_NESTING_DEPTH) {
            return "array<...>";
        }
        return String.format("array<%s>", itemType.toTypeString(depth + 1));
    }

    @Override
    public int getMaxUniqueId() {
        return itemType.getMaxUniqueId();
    }
}


