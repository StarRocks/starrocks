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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.thrift.TColumnType;
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNode;
import com.starrocks.thrift.TTypeNodeType;

/**
 * Describes an ARRAY type.
 */
public class ArrayType extends Type {
    @SerializedName(value = "itemType")
    private Type itemType;

    public ArrayType(Type itemType) {
        if (itemType != null && itemType.isDecimalV3()) {
            throw new InternalError("Decimal32/64/128 is not supported in current version");
        }
        Preconditions.checkState(!Type.NULL.equals(itemType), "array's item type cannot be NULL_TYPE");
        this.itemType = itemType;
    }

    public ArrayType(Type itemType, boolean fromDlaEnableDecimalV3) {
        if (!fromDlaEnableDecimalV3 && itemType.isDecimalV3()) {
            this.itemType = Type.UNKNOWN_TYPE;
        } else {
            this.itemType = itemType;
        }
    }

    public Type getItemType() {
        return itemType;
    }

    @Override
    public TColumnType toColumnTypeThrift() {
        Preconditions.checkArgument(false, "ArrayType.toColumnTypeThrift not implemented");
        return null;
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
            return "ARRAY<...>";
        }
        return String.format("ARRAY<%s>", itemType.toSql(depth + 1));
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
    public void toThrift(TTypeDesc container) {
        TTypeNode node = new TTypeNode();
        container.types.add(node);
        Preconditions.checkNotNull(itemType);
        node.setType(TTypeNodeType.ARRAY);
        itemType.toThrift(container);
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

    /**
     * @return 33 (utf8_general_ci) if type is array
     * https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition
     * character_set (2) -- is the column character set and is defined in Protocol::CharacterSet.
     */
    @Override
    public int getMysqlResultSetFieldCharsetIndex() {
        return CHARSET_UTF8;
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
}


