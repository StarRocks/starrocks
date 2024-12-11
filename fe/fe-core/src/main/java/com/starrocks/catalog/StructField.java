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
import com.starrocks.thrift.TStructField;
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNode;
import org.apache.commons.lang3.StringUtils;

<<<<<<< HEAD
=======
import java.util.StringJoiner;

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
/**
 * TODO: Support comments for struct fields. The Metastore does not properly store
 * comments of struct fields. We set comment to null to avoid compatibility issues.
 */
public class StructField {
    @SerializedName(value = "name")
<<<<<<< HEAD
    private final String name;
    @SerializedName(value = "type")
    private final Type type;

    // comment is not used now, it's always null.
    @SerializedName(value = "comment")
    private final String comment;
    private int position;  // in struct

    public StructField(String name, Type type, String comment) {
        this.name = name;
        this.type = type;
        this.comment = comment;
=======
    private String name;
    @SerializedName(value = "type")
    private Type type;

    // comment is not used now, it's always null.
    @SerializedName(value = "comment")
    private String comment;
    private int position;  // in struct

    @SerializedName(value = "fieldId")
    private int fieldId = -1;

    // fieldPhysicalName is used to store the physical name of the field in the storage layer.
    // for example, the physical name of a struct field in a parquet file.
    // used in delta lake column mapping name mode
    @SerializedName(value = "fieldPhysicalName")
    private String fieldPhysicalName = "";

    public StructField() {}

    public StructField(String name, int fieldId, Type type, String comment) {
        this(name, fieldId, "", type, comment);
    }

    public StructField(String name, int fieldId, String fieldPhysicalName, Type type, String comment) {
        this.name = name;
        this.type = type;
        this.comment = comment;
        this.fieldId = fieldId;
        this.fieldPhysicalName = fieldPhysicalName;
    }

    public StructField(String name, Type type, String comment) {
        this(name, -1, type, comment);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    public StructField(String name, Type type) {
        this(name, type, null);
    }

    public String getComment() {
        return comment;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public int getPosition() {
        return position;
    }

<<<<<<< HEAD
=======
    public int getFieldId() {
        return fieldId;
    }

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    public void setPosition(int position) {
        this.position = position;
    }

    public String toSql(int depth, boolean printName) {
        String typeSql = (depth < Type.MAX_NESTING_DEPTH) ? type.toSql(depth) : "...";
        StringBuilder sb = new StringBuilder();
        if (printName) {
            sb.append(name).append(' ');
        }
        sb.append(typeSql);
        if (comment != null) {
            sb.append(String.format(" COMMENT '%s'", comment));
        }
        return sb.toString();
    }

<<<<<<< HEAD
=======
    public String toTypeString(int depth) {
        String typeSql = (depth < Type.MAX_NESTING_DEPTH) ? type.toTypeString(depth) : "...";
        StringBuilder sb = new StringBuilder();
        sb.append(name).append(' ');
        sb.append(typeSql);
        return sb.toString();
    }

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    /**
     * Pretty prints this field with lpad number of leading spaces.
     * Calls prettyPrint(lpad) on this field's type.
     */
    public String prettyPrint(int lpad, boolean printName) {
        String leftPadding = Strings.repeat(" ", lpad);
        StringBuilder sb = new StringBuilder(leftPadding);
        if (printName) {
            sb.append(name).append(' ');
        }

        // Pass in the padding to make sure nested fields are aligned properly,
        // even if we then strip the top-level padding.
        String typeStr = type.prettyPrint(lpad);
        typeStr = typeStr.substring(lpad);
        sb.append(typeStr);

        if (comment != null) {
            sb.append(String.format(" COMMENT '%s'", comment));
        }
        return sb.toString();
    }

    public void toThrift(TTypeDesc container, TTypeNode node) {
        TStructField field = new TStructField();
        field.setName(name);
        field.setComment(comment);
<<<<<<< HEAD
=======
        field.setId(fieldId);
        field.setPhysical_name(fieldPhysicalName);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        node.struct_fields.add(field);
        type.toThrift(container);
    }

    @Override
    public int hashCode() {
<<<<<<< HEAD
        return Objects.hashCode(name.toLowerCase(), type);
=======
        return Objects.hashCode(name.toLowerCase(), type, fieldId, fieldPhysicalName);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StructField)) {
            return false;
        }
        StructField otherStructField = (StructField) other;
        // Both are named struct field
<<<<<<< HEAD
        return StringUtils.equalsIgnoreCase(name, otherStructField.name) && Objects.equal(type, otherStructField.type);
=======
        return StringUtils.equalsIgnoreCase(name, otherStructField.name) && Objects.equal(type, otherStructField.type) &&
                    (fieldId == otherStructField.fieldId) && Objects.equal(fieldPhysicalName, otherStructField.fieldPhysicalName);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", StructField.class.getSimpleName() + "[", "]")
                .add("name='" + (Strings.isNullOrEmpty(name) ? "" : name) + "'")
                .add("type=" + type)
                .add("position=" + position)
                .add("fieldId=" + fieldId)
                .add("fieldPhysicalName='" + (Strings.isNullOrEmpty(fieldPhysicalName) ? "" : fieldPhysicalName) + "'")
                .toString();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public StructField clone() {
<<<<<<< HEAD
        return new StructField(name, type.clone(), comment);
=======
        return new StructField(name, fieldId, fieldPhysicalName, type.clone(), comment);
    }

    public int getMaxUniqueId() {
        return Math.max(fieldId, type.getMaxUniqueId());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }
}


