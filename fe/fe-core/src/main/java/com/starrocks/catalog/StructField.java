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

/**
 * TODO: Support comments for struct fields. The Metastore does not properly store
 * comments of struct fields. We set comment to null to avoid compatibility issues.
 */
public class StructField {
    // If name is null, that means this StructFiled is unnamed.
    @SerializedName(value = "name")
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
    }

    public StructField(String name, Type type) {
        this(name, type, null);
    }

    // Unnamed struct field
    public StructField(Type type) {
        this(null, type, null);
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

    public void setPosition(int position) {
        this.position = position;
    }

    public String toSql(int depth) {
        String typeSql = (depth < Type.MAX_NESTING_DEPTH) ? type.toSql(depth) : "...";
        StringBuilder sb = new StringBuilder();
        if (name != null) {
            sb.append(name).append(' ');
        }
        sb.append(typeSql);
        if (comment != null) {
            sb.append(String.format(" COMMENT '%s'", comment));
        }
        return sb.toString();
    }

    /**
     * Pretty prints this field with lpad number of leading spaces.
     * Calls prettyPrint(lpad) on this field's type.
     */
    public String prettyPrint(int lpad) {
        String leftPadding = Strings.repeat(" ", lpad);
        StringBuilder sb = new StringBuilder(leftPadding);
        if (name != null) {
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
        node.struct_fields.add(field);
        type.toThrift(container);
    }

    @Override
    public int hashCode() {
        if (name != null) {
            return Objects.hashCode(name, type);
        } else {
            return Objects.hashCode(type);
        }
    }

    // [Named vs Named] struct<a: INT, b: STRING> is equal to struct<a: INT, b: STRING>
    // [Unnamed vs Unnamed] struct<INT, STRING> is equal to struct<INT, STRING>
    // [Named vs Unnamed][Always false] struct<a: INT, b: STRING> is not equal to struct<INT, STRING>
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StructField)) {
            return false;
        }
        StructField otherStructField = (StructField) other;
        if (name == null) {
            // If this() is unnamed struct field, other struct field must also be an unnamed struct field.
            return otherStructField.name == null && type.equals(otherStructField.type);
        }

        if (otherStructField.name == null) {
            // If other struct field is not named struct field, return false directly.
            return false;
        }
        // Both are named struct field
        return otherStructField.name.equals(name) && otherStructField.type.equals(type);
    }

    @Override
    public StructField clone() {
        return new StructField(name, type.clone(), comment);
    }
}


