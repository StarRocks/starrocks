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

package com.starrocks.catalog.system;

import com.google.common.collect.Lists;
import com.starrocks.analysis.DescriptorTable.ReferencedPartitionInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.thrift.TSchemaTable;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * representation of MySQL information schema table metadata,
 */
public class SystemTable extends Table {
    public static final int FN_REFLEN = 512;
    public static final int NAME_CHAR_LEN = 2048;
    public static final int MAX_FIELD_VARCHAR_LENGTH = 65535;

    private final TSchemaTableType schemaTableType;

    public SystemTable(long id, String name, TableType type, List<Column> baseSchema, TSchemaTableType schemaTableType) {
        super(id, name, type, baseSchema);
        this.schemaTableType = schemaTableType;
    }

    public static boolean isBeSchemaTable(String name) {
        return name.startsWith("be_");
    }

    public static boolean isFeSchemaTable(String name) {
        // currently, it only stands for single FE leader, because only FE leader has related info
        return name.startsWith("fe_");
    }

    public boolean requireOperatePrivilege() {
        return SystemTable.isBeSchemaTable(getName()) || SystemTable.isFeSchemaTable(getName());
    }

    @Override
    public boolean supportsUpdate() {
        return name.equals("be_configs");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException("Do not allow to write SchemaTable to image.");
    }

    public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException("Do not allow read SchemaTable from image.");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        List<Column> columns;

        public Builder() {
            columns = Lists.newArrayList();
        }

        public Builder column(String name, ScalarType type) {
            return column(name, type, true);
        }

        public Builder column(String name, ScalarType type, boolean nullable) {
            columns.add(new Column(name, type, false, null, nullable, null, ""));
            return this;
        }

        public List<Column> build() {
            return columns;
        }
    }

    @Override
    public TTableDescriptor toThrift(List<ReferencedPartitionInfo> partitions) {
        TSchemaTable tSchemaTable = new TSchemaTable(schemaTableType);
        TTableDescriptor tTableDescriptor =
                new TTableDescriptor(getId(), TTableType.SCHEMA_TABLE, getBaseSchema().size(), 0, this.name, "");
        tTableDescriptor.setSchemaTable(tSchemaTable);
        return tTableDescriptor;
    }

    @Override
    public boolean isSupported() {
        return true;
    }
}
