// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/MapType.java

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
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNode;
import com.starrocks.thrift.TTypeNodeType;

import java.util.Arrays;

/**
 * Describes a MAP type. MAP types have a scalar key and an arbitrarily-typed value.
 */
public class MapType extends Type {
    private Type keyType;
    private Type valueType;

    public MapType(Type keyType, Type valueType) {
        Preconditions.checkNotNull(keyType);
        Preconditions.checkNotNull(valueType);
        selectedFields = new Boolean[] { false, false };
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
    public void setSelectedField(int pos, boolean needSetChildren) {
        if (pos == -1) {
            Arrays.fill(selectedFields, true);
        } else {
            selectedFields[pos] = true;
        }
        if (needSetChildren && (pos == 1 || pos == -1) && valueType.isComplexType()) {
            valueType.selectAllFields();
        }
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
            return "MAP<...>";
        }
        return String.format("MAP<%s,%s>",
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
        Preconditions.checkNotNull(keyType);
        Preconditions.checkNotNull(valueType);
        node.setType(TTypeNodeType.MAP);
        node.setSelected_fields(Arrays.asList(selectedFields));
        keyType.toThrift(container);
        valueType.toThrift(container);
    }

    @Override
    public MapType clone() {
        MapType clone = (MapType) super.clone();
        clone.keyType = this.keyType.clone();
        clone.valueType = this.valueType.clone();
        clone.selectedFields = this.selectedFields.clone();
        return clone;
    }
}

