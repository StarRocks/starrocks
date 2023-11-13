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
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.annotations.SerializedName;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNode;
import com.starrocks.thrift.TTypeNodeType;

import java.util.Arrays;

/**
 * Describes a MAP type. MAP types have a scalar key and an arbitrarily-typed value.
 */
public class MapType extends Type {
    @SerializedName(value = "keyType")
    private Type keyType;
    @SerializedName(value = "valueType")
    private Type valueType;

    public MapType(Type keyType, Type valueType) {
        Preconditions.checkNotNull(keyType);
        Preconditions.checkNotNull(valueType);
        selectedFields = new Boolean[] {false, false};
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public Type getKeyType() {
        return keyType;
    }

    public Type getValueType() {
        return valueType;
    }

    @Override
    public void setSelectedField(ComplexTypeAccessPath accessPath, boolean needSetChildren) {
        ComplexTypeAccessPathType accessPathType = accessPath.getAccessPathType();
        switch (accessPathType) {
            case ALL_SUBFIELDS:
                Arrays.fill(selectedFields, true);
                break;
            case MAP_KEY:
                selectedFields[0] = true;
                break;
            case MAP_VALUE:
                selectedFields[1] = true;
                break;
            default:
                Preconditions.checkArgument(false, "Unreachable!");
        }

        if (needSetChildren &&
                (accessPathType == ComplexTypeAccessPathType.ALL_SUBFIELDS ||
                        accessPathType == ComplexTypeAccessPathType.MAP_VALUE) && valueType.isComplexType()) {
            valueType.selectAllFields();
        }
    }

    public void pruneUnusedSubfields() {
        // We set pruned subfield to NULL in map
        if (!selectedFields[0]) {
            keyType = ScalarType.UNKNOWN_TYPE;
        }
        if (!selectedFields[1]) {
            valueType = ScalarType.UNKNOWN_TYPE;
        }
        Preconditions.checkArgument(!keyType.isUnknown() || !valueType.isUnknown());
    }

    @Override
    public void selectAllFields() {
        Arrays.fill(selectedFields, true);
        if (valueType.isComplexType()) {
            valueType.selectAllFields();
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

    @Override
    public int hashCode() {
        return Objects.hashCode(keyType, valueType);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof MapType)) {
            return false;
        }
        MapType otherMapType = (MapType) other;
        return otherMapType.keyType.equals(keyType)
                && otherMapType.valueType.equals(valueType);
    }

    @Override
    public boolean matchesType(Type t) {
        if (t.isPseudoType()) {
            return t.matchesType(this);
        }
        return t.isMapType()
                && keyType.matchesType(((MapType) t).keyType) && valueType.matchesType(((MapType) t).getValueType());
    }

    @Override
    public String toSql(int depth) {
        if (depth >= MAX_NESTING_DEPTH) {
            return "map<...>";
        }
        return String.format("map<%s,%s>",
                keyType.toSql(depth + 1), valueType.toSql(depth + 1));
    }

    @Override
    public String toString() {
        return String.format("MAP<%s,%s>",
                keyType.toString(), valueType.toString());
    }

    @Override
    protected String prettyPrint(int lpad) {
        String leftPadding = Strings.repeat(" ", lpad);
        if (!valueType.isStructType()) {
            return leftPadding + toSql();
        }
        // Pass in the padding to make sure nested fields are aligned properly,
        // even if we then strip the top-level padding.
        String structStr = valueType.prettyPrint(lpad);
        structStr = structStr.substring(lpad);
        return String.format("%sMAP<%s,%s>", leftPadding, keyType.toSql(), structStr);
    }

    @Override
    public void toThrift(TTypeDesc container) {
        TTypeNode node = new TTypeNode();
        container.types.add(node);
        node.setType(TTypeNodeType.MAP);
        keyType.toThrift(container);
        valueType.toThrift(container);
    }

    @Override
    public boolean isFullyCompatible(Type other) {
        if (!other.isMapType()) {
            return false;
        }

        if (equals(other)) {
            return true;
        }

        MapType t = (MapType) other;
        return keyType.isFullyCompatible(t.getKeyType()) && valueType.isFullyCompatible(t.getValueType());
    }

    @Override
    public MapType clone() {
        MapType clone = (MapType) super.clone();
        clone.keyType = this.keyType.clone();
        clone.valueType = this.valueType.clone();
        if (this.selectedFields != null) {
            clone.selectedFields = this.selectedFields.clone();
        }
        return clone;
    }

    // Todo: remove it after remove selectedFields
    public static class MapTypeDeserializer implements JsonDeserializer<MapType> {
        @Override
        public MapType deserialize(JsonElement jsonElement, java.lang.reflect.Type type,
                                   JsonDeserializationContext jsonDeserializationContext)
                throws JsonParseException {
            JsonObject dumpJsonObject = jsonElement.getAsJsonObject();
            JsonObject key = dumpJsonObject.getAsJsonObject("keyType");
            Type keyType = GsonUtils.GSON.fromJson(key, Type.class);
            JsonObject value = dumpJsonObject.getAsJsonObject("valueType");
            Type valueType = GsonUtils.GSON.fromJson(value, Type.class);
            return new MapType(keyType, valueType);
        }
    }

    public String toMysqlDataTypeString() {
        return "map";
    }

    // This implementation is the same as BE schema_columns_scanner.cpp type_to_string
    public String toMysqlColumnTypeString() {
        return toSql();
    }
}

