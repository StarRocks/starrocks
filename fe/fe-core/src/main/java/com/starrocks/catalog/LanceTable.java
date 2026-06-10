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
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.thrift.TLanceTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;

import java.util.List;
import java.util.Map;

public class LanceTable extends Table {

    private static final String DATASET_URI = "dataset.uri";

    @SerializedName(value = "prop")
    private Map<String, String> lanceProperties;

    public LanceTable() {
        super(TableType.LANCE);
    }

    public LanceTable(long id, String srTableName, String comment,
                   List<Column> schema, Map<String, String> lanceProperties) {
        super(id, srTableName, TableType.LANCE, schema);
        this.comment = comment;
        this.lanceProperties = lanceProperties;
    }

    public List<Column> getPartitionColumns() {
        return List.of();
    }

    public String getDatasetURI() {
        return lanceProperties.get(DATASET_URI);
    }

    public Map<String, String> getProperties() {
        return lanceProperties;
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public int hashCode() {
        return com.google.common.base.Objects.hashCode(getCatalogName(), getTableIdentifier());
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LanceTable)) {
            return false;
        }

        LanceTable otherTable = (LanceTable) other;
        String catalogName = getCatalogName();
        String tableIdentifier = getTableIdentifier();
        return Objects.equal(catalogName, otherTable.getCatalogName()) &&
                Objects.equal(tableIdentifier, otherTable.getTableIdentifier());
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        // not used
        TLanceTable lanceTable = new TLanceTable();

        // only TTableType.LANCE_TABLE is used, the other field is useless now
        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.LANCE_TABLE,
                fullSchema.size(), 0, name, "");
        tTableDescriptor.setLanceTable(lanceTable);
        return tTableDescriptor;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private long id;
        private String srTableName;
        private String comment;
        private List<Column> columns;
        private Map<String, String> lanceProperties;

        public Builder() {
        }

        public Builder setId(long id) {
            this.id = id;
            return this;
        }

        public Builder setSrTableName(String srTableName) {
            this.srTableName = srTableName;
            return this;
        }

        public Builder setComment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder setColumns(List<Column> columns) {
            this.columns = columns;
            return this;
        }

        public Builder setLanceProperties(Map<String, String> lanceProperties) {
            this.lanceProperties = lanceProperties;
            return this;
        }

        public LanceTable build() {
            return new LanceTable(id, srTableName, comment, columns, lanceProperties);
        }
    }
}
