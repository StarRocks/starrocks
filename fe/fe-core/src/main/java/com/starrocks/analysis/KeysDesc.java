// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/KeysDesc.java

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

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.Type;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.commons.lang3.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class KeysDesc implements Writable {
    private KeysType type;
    private final List<String> keysColumnNames;

    public KeysDesc() {
        this.type = KeysType.AGG_KEYS;
        this.keysColumnNames = Lists.newArrayList();
    }

    public KeysDesc(KeysType type, List<String> keysColumnNames) {
        this.type = type;
        this.keysColumnNames = keysColumnNames;
    }

    public KeysType getKeysType() {
        return type;
    }

    public int keysColumnSize() {
        return keysColumnNames.size();
    }

    public boolean containsCol(String colName) {
        return keysColumnNames.stream().anyMatch(e -> StringUtils.equalsIgnoreCase(e, colName));
    }

    // new planner framework use SemanticException instead of AnalysisException, this code will remove in future
    @Deprecated
    public void analyze(List<ColumnDef> cols) throws SemanticException {
        if (type == null) {
            throw new SemanticException("Keys type is null.");
        }

        if (keysColumnNames == null || keysColumnNames.size() == 0) {
            throw new SemanticException("The number of key columns is 0.");
        }

        if (keysColumnNames.size() > cols.size()) {
            throw new SemanticException("The number of key columns should be less than the number of columns.");
        }

        for (int i = 0; i < keysColumnNames.size(); ++i) {
            String name = cols.get(i).getName();
            if (!keysColumnNames.get(i).equalsIgnoreCase(name)) {
                String keyName = keysColumnNames.get(i);
                if (!cols.stream().anyMatch(col->col.getName().equalsIgnoreCase(keyName))) {
                    throw new SemanticException("Key column(%s) doesn't exist.", keysColumnNames.get(i));
                }
                throw new SemanticException("Key columns should be a ordered prefix of the schema.");
            }

            if (cols.get(i).getAggregateType() != null) {
                throw new SemanticException("Key column[" + name + "] should not specify aggregate type.");
            }
            if (type == KeysType.PRIMARY_KEYS) {
                ColumnDef cd = cols.get(i);
                cd.setPrimaryKeyNonNullable();
                if (cd.isAllowNull()) {
                    throw new SemanticException("primary key column[" + name + "] cannot be nullable");
                }
                Type t = cd.getType();
                if (!(t.isBoolean() || t.isIntegerType() || t.isLargeint() || t.isVarchar() || t.isDate() ||
                        t.isDatetime())) {
                    throw new SemanticException("primary key column[" + name + "] type not supported: " + t.toSql());
                }
            }
        }

        // for olap table
        for (int i = keysColumnNames.size(); i < cols.size(); ++i) {
            if (type == KeysType.AGG_KEYS) {
                if (cols.get(i).getAggregateType() == null) {
                    throw new SemanticException(type.name() + " table should specify aggregate type for "
                            + "non-key column[" + cols.get(i).getName() + "]");
                }
            } else {
                if (cols.get(i).getAggregateType() != null && cols.get(i).getAggregateType() != AggregateType.REPLACE) {
                    throw new SemanticException(type.name() + " table should not specify aggregate type for "
                            + "non-key column[" + cols.get(i).getName() + "]");
                }
            }
        }
    }

    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(type.name()).append("(");
        int i = 0;
        for (String columnName : keysColumnNames) {
            if (i != 0) {
                stringBuilder.append(", ");
            }
            stringBuilder.append("`").append(columnName).append("`");
            i++;
        }
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    public static KeysDesc read(DataInput in) throws IOException {
        KeysDesc desc = new KeysDesc();
        desc.readFields(in);
        return desc;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());

        int count = keysColumnNames.size();
        out.writeInt(count);
        for (String colName : keysColumnNames) {
            Text.writeString(out, colName);
        }
    }

    public void readFields(DataInput in) throws IOException {
        type = KeysType.valueOf(Text.readString(in));

        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            keysColumnNames.add(Text.readString(in));
        }
    }

    public void checkColumnDefs(List<ColumnDef> cols, List<Integer> sortKeyIdxes) {
        if (type == null) {
            throw new SemanticException("Keys type is null.");
        }

        if (keysColumnNames == null || keysColumnNames.size() == 0) {
            throw new SemanticException("The number of key columns is 0.");
        }

        if (keysColumnNames.size() > cols.size()) {
            throw new SemanticException("The number of key columns should be less than the number of columns.");
        }

        for (int i = 0; i < keysColumnNames.size(); ++i) {
            String name = cols.get(i).getName();
            if (!keysColumnNames.get(i).equalsIgnoreCase(name)) {
                throw new SemanticException("Key columns should be a ordered prefix of the schema.");
            }

            if (cols.get(i).getAggregateType() != null) {
                throw new SemanticException("Key column[%s] should not specify aggregate type.", name);
            }
            if (type == KeysType.PRIMARY_KEYS) {
                ColumnDef cd = cols.get(i);
                if (cd.isAllowNull()) {
                    throw new SemanticException("primary key column[%s] cannot be nullable", name);
                }
                Type t = cd.getType();
                if (!(t.isBoolean() || t.isIntegerType() || t.isLargeint() || t.isVarchar() || t.isDate() ||
                        t.isDatetime())) {
                    throw new SemanticException("primary key column[%s] type not supported: ", t.toSql());
                }
            }
        }

        // for olap table
        for (int i = keysColumnNames.size(); i < cols.size(); ++i) {
            if (type == KeysType.AGG_KEYS) {
                if (cols.get(i).getAggregateType() == null) {
                    throw new SemanticException(type.name() + " table should specify aggregate type for "
                            + "non-key column[%s]", cols.get(i).getName());
                }
            } else {
                if (cols.get(i).getAggregateType() != null && cols.get(i).getAggregateType() != AggregateType.REPLACE) {
                    throw new SemanticException(type.name() + " table should not specify aggregate type for "
                            + "non-key column[%s]", cols.get(i).getName());
                }
            }
        }

        for (int i = 0; i < sortKeyIdxes.size(); i++) {
            String name = cols.get(sortKeyIdxes.get(i)).getName();
            ColumnDef cd = cols.get(sortKeyIdxes.get(i));
            Type t = cd.getType();
            if (!(t.isBoolean() || t.isIntegerType() || t.isLargeint() || t.isVarchar() || t.isDate() ||
                    t.isDatetime())) {
                throw new SemanticException("sort key column[" + name + "] type not supported: " + t.toSql());
            }
        }
    }
}

