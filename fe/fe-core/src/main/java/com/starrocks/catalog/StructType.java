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
import com.google.gson.annotations.SerializedName;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.commons.lang3.StringUtils;
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

    @SerializedName(value = "named")
    private final boolean isNamed;

    public StructType(ArrayList<StructField> structFields) {
        this(structFields, true);
    }

    public StructType(List<StructField> structFields, boolean isNamed) {
        Preconditions.checkNotNull(structFields);
        Preconditions.checkArgument(structFields.size() > 0);
        this.fields = new ArrayList<>();
        for (StructField field : structFields) {
            String lowerFieldName = field.getName().toLowerCase();
            if (fieldMap.containsKey(lowerFieldName)) {
                throw new SemanticException("struct contains duplicate subfield name: " + lowerFieldName);
            } else {
                field.setPosition(fields.size());
                fields.add(field);
                // Store lowercase field name in fieldMap
                fieldMap.put(lowerFieldName, field);
            }
        }
        selectedFields = new Boolean[fields.size()];
        this.isNamed = isNamed;
        Arrays.fill(selectedFields, false);
    }

    // Used to construct an unnamed struct type, for example, to create a struct type
    // row(1, 'b') to create an unnamed struct type struct<int, string>
    public StructType(List<Type> fieldTypes) {
        Preconditions.checkNotNull(fieldTypes);
        Preconditions.checkArgument(fieldTypes.size() > 0);
        isNamed = false;
        this.fields = new ArrayList<>();
        for (int i = 0; i < fieldTypes.size(); i++) {
            Type fieldType = fieldTypes.get(i);
            // unnamed struct, default column name is col1, ...
            StructField field = new StructField("col" + (i + 1), fieldType);
            this.fields.add(field);
            field.setPosition(i);
            fieldMap.put(field.getName(), field);
        }
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
            if (!StringUtils.equalsIgnoreCase(fields.get(i).getName(), rhsType.fields.get(i).getName())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toSql(int depth) {
        if (depth >= MAX_NESTING_DEPTH) {
            return "struct<...>";
        }
        ArrayList<String> fieldsSql = Lists.newArrayList();
        for (StructField f : fields) {
            fieldsSql.add(f.toSql(depth + 1, true));
        }
        return String.format("struct<%s>", Joiner.on(", ").join(fieldsSql));
    }

    @Override
    protected String prettyPrint(int lpad) {
        String leftPadding = Strings.repeat(" ", lpad);
        ArrayList<String> fieldsSql = Lists.newArrayList();
        for (StructField f : fields) {
            fieldsSql.add(f.prettyPrint(lpad + 2, true));
        }
        return String.format("%sSTRUCT<\n%s\n%s>",
                leftPadding, Joiner.on(",\n").join(fieldsSql), leftPadding);
    }

    public boolean isNamed() {
        return isNamed;
    }

    public ArrayList<StructField> getFields() {
        return fields;
    }

    public StructField getField(String fieldName) {
        return fieldMap.get(StringUtils.lowerCase(fieldName));
    }

    public boolean containsField(String fieldName) {
        return fieldMap.containsKey(StringUtils.lowerCase(fieldName));
    }

    public int getFieldPos(String fieldName) {
        return fieldMap.get(StringUtils.lowerCase(fieldName)).getPosition();
    }

    public StructField getField(int pos) {
        return fields.get(pos);
    }

    public void updateFields(List<StructField> structFields) {
        Preconditions.checkNotNull(structFields);
        Preconditions.checkArgument(structFields.size() > 0);
        fields.clear();
        fieldMap.clear();
        for (StructField field : structFields) {
            String lowerFieldName = field.getName().toLowerCase();
            if (fieldMap.containsKey(lowerFieldName)) {
                throw new SemanticException("struct contains duplicate subfield name: " + lowerFieldName);
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

    @Override
    public void setSelectedField(ComplexTypeAccessPath accessPath, boolean needSetChildren) {
        if (accessPath.getAccessPathType() == ComplexTypeAccessPathType.ALL_SUBFIELDS) {
            //  ALL_SUBFIELDS access path must be the last access path in access paths,
            //  so it's must need to set needSetChildren.
            Preconditions.checkArgument(needSetChildren);
            selectAllFields();
            return;
        }

        Preconditions.checkArgument(accessPath.getAccessPathType() == ComplexTypeAccessPathType.STRUCT_SUBFIELD);
        Preconditions.checkArgument(accessPath.getStructSubfieldName() != null);
        int pos = getFieldPos(accessPath.getStructSubfieldName());
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
                fieldMap.remove(StringUtils.lowerCase(structField.getName()));
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
    public boolean isFullyCompatible(Type other) {
        if (!other.isStructType()) {
            return false;
        }

        if (equals(other)) {
            return true;
        }

        StructType t = (StructType) other;
        if (fields.size() != t.fields.size()) {
            return false;
        }
        for (int i = 0; i < fields.size(); i++) {
            if (!fields.get(i).getType().isFullyCompatible(t.fields.get(i).getType())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public StructType clone() {
        ArrayList<StructField> structFields = new ArrayList<>(fields.size());
        for (StructField field : fields) {
            structFields.add(field.clone());
        }
        return new StructType(structFields);
    }

    public String toMysqlDataTypeString() {
        return "struct";
    }

    // This implementation is the same as BE schema_columns_scanner.cpp type_to_string
    public String toMysqlColumnTypeString() {
        return toSql();
    }

    @Override
    protected String toTypeString(int depth) {
        if (depth >= MAX_NESTING_DEPTH) {
            return "struct<...>";
        }
        ArrayList<String> fieldsSql = Lists.newArrayList();
        for (StructField f : fields) {
            fieldsSql.add(f.toTypeString(depth + 1));
        }
        return String.format("struct<%s>", Joiner.on(", ").join(fieldsSql));
    }

    @Override
    public int getMaxUniqueId() {
        int maxUniqueId = -1;
        for (StructField f : fields) {
            maxUniqueId = Math.max(maxUniqueId, f.getMaxUniqueId());
        }
        return maxUniqueId;
    }
}

