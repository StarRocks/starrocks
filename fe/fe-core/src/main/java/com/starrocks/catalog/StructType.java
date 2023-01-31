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

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.annotations.SerializedName;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TStructField;
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNode;
import com.starrocks.thrift.TTypeNodeType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Describes a STRUCT type. STRUCT types have a list of named struct fields.
 */
public class StructType extends Type {

    private static final Logger LOG = LogManager.getLogger(StructType.class);

    private final HashMap<String, StructField> fieldMap = Maps.newHashMap();
    @SerializedName(value = "fields")
    private final ArrayList<StructField> fields;

    public StructType(ArrayList<StructField> structFields) {
        Preconditions.checkNotNull(structFields);
        Preconditions.checkArgument(structFields.size() > 0);
        this.fields = new ArrayList<>();
        for (StructField field : structFields) {
            Preconditions.checkState(!Type.NULL.equals(field.getType()), "struct's field type cannot be NULL_TYPE");
            String lowerFieldName = field.getName().toLowerCase();
            if (fieldMap.containsKey(lowerFieldName)) {
                LOG.warn(String.format("Contains the same struct subfield name: %s, ignore it", lowerFieldName));
            } else {
                field.setPosition(fields.size());
                fields.add(field);
                // Store lowercase field name in fieldMap
                fieldMap.put(lowerFieldName, field);
            }
        }
        selectedFields = new Boolean[fields.size()];
        Arrays.fill(selectedFields, false);
    }

    // Used to construct an unnamed struct type, for example, to create a struct type
    // row(1, 'b') to create an unnamed struct type struct<int, string>
    public StructType(List<Type> fieldTypes) {
        ArrayList<StructField> newFields = new ArrayList<>();
        for (Type fieldType : fieldTypes) {
            Preconditions.checkState(!Type.NULL.equals(fieldType), "struct's field type cannot be NULL_TYPE");
            newFields.add(new StructField(fieldType));
        }
        this.fields = newFields;
        selectedFields = new Boolean[fields.size()];
        Arrays.fill(selectedFields, false);
    }

    @Override
    public String toString() {
        // TODO(SmithCruise): Lazy here, any difference from toSql()?
        return toSql();
    }

    @Override
    public int getTypeSize() {
        int size = 0;
        for (StructField structField : fields) {
            size += structField.getType().getTypeSize();
        }
        return size;
    }

    @Override
    public boolean matchesType(Type t) {
        if (t.isPseudoType()) {
            return t.matchesType(this);
        }
        if (!t.isStructType()) {
            return false;
        }

        StructType rhsType = (StructType) t;
        if (fields.size() != rhsType.fields.size()) {
            return false;
        }
        for (int i = 0; i < fields.size(); ++i) {
            if (!fields.get(i).getType().matchesType(rhsType.fields.get(i).getType())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toSql(int depth) {
        if (depth >= MAX_NESTING_DEPTH) {
            return "STRUCT<...>";
        }
        ArrayList<String> fieldsSql = Lists.newArrayList();
        for (StructField f : fields) {
            fieldsSql.add(f.toSql(depth + 1));
        }
        return String.format("STRUCT<%s>", Joiner.on(", ").join(fieldsSql));
    }

    @Override
    protected String prettyPrint(int lpad) {
        String leftPadding = Strings.repeat(" ", lpad);
        ArrayList<String> fieldsSql = Lists.newArrayList();
        for (StructField f : fields) {
            fieldsSql.add(f.prettyPrint(lpad + 2));
        }
        return String.format("%sSTRUCT<\n%s\n%s>",
                leftPadding, Joiner.on(",\n").join(fieldsSql), leftPadding);
    }

    public ArrayList<StructField> getFields() {
        return fields;
    }

    public StructField getField(String fieldName) {
        return fieldMap.get(fieldName.toLowerCase());
    }

    public int getFieldPos(String fieldName) {
        return fieldMap.get(fieldName).getPosition();
    }

    public StructField getField(int pos) {
        return fields.get(pos);
    }

    @Override
    public void setSelectedField(int pos, boolean needSetChildren) {
        selectedFields[pos] = true;
        if (needSetChildren) {
            StructField structField = fields.get(pos);
            if (structField.getType().isComplexType()) {
                structField.getType().selectAllFields();
            }
        }
    }

    public void pruneUnusedSubfields() {
        for (int pos = selectedFields.length - 1; pos >= 0; pos--) {
            StructField structField = fields.get(pos);
            if (!selectedFields[pos]) {
                fields.remove(pos);
                fieldMap.remove(structField.getName());
            }
        }

        for (StructField structField : fields) {
            Type type = structField.getType();
            if (type.isComplexType()) {
                type.pruneUnusedSubfields();
            }
        }
    }

    @Override
    public void selectAllFields() {
        Arrays.fill(selectedFields, true);
        for (StructField structField : fields) {
            if (structField.getType().isComplexType()) {
                structField.getType().selectAllFields();
            }
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
        return Objects.hashCode(fields);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StructType)) {
            return false;
        }
        StructType otherStructType = (StructType) other;
        return otherStructType.getFields().equals(fields);
    }

    @Override
    public void toThrift(TTypeDesc container) {
        TTypeNode node = new TTypeNode();
        container.types.add(node);
        Preconditions.checkNotNull(fields);
        Preconditions.checkState(!fields.isEmpty(), "StructType must contains at least one StructField.");
        node.setType(TTypeNodeType.STRUCT);
        node.setStruct_fields(new ArrayList<TStructField>());
        for (StructField field : fields) {
            field.toThrift(container, node);
        }
    }

    @Override
    public StructType clone() {
        ArrayList<StructField> structFields = new ArrayList<>(fields.size());
        for (StructField field : fields) {
            structFields.add(field.clone());
        }
        return new StructType(structFields);
    }

    public static class StructTypeDeserializer implements JsonDeserializer<StructType> {
        @Override
        public StructType deserialize(JsonElement jsonElement, java.lang.reflect.Type type,
                                   JsonDeserializationContext jsonDeserializationContext)
                throws JsonParseException {
            JsonObject dumpJsonObject = jsonElement.getAsJsonObject();
            JsonArray fields = dumpJsonObject.getAsJsonArray("fields");
            ArrayList<StructField> structFields = new ArrayList<>(fields.size());
            for (JsonElement field : fields) {
                structFields.add(GsonUtils.GSON.fromJson(field, StructField.class));
            }
            return new StructType(structFields);
        }
    }
}

