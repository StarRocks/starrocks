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

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.thrift.TTableDescriptor;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ADBCTable extends Table {

    private String catalogName;
    private String dbName;
    private Map<String, String> properties;

    public ADBCTable() {
        super(TableType.ADBC);
    }

    public ADBCTable(long id, String name, List<Column> fullSchema, String dbName,
                     String catalogName, Map<String, String> properties) {
        super(id, name, TableType.ADBC, fullSchema);
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.properties = properties;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getCatalogDBName() {
        return catalogName + "." + dbName;
    }

    public String getDbName() {
        return dbName;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String getUUID() {
        if (!Strings.isNullOrEmpty(catalogName)) {
            return String.join(".", catalogName, dbName, name);
        } else {
            return Long.toString(id);
        }
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        // TODO: Implement when TTableType.ADBC_TABLE is added to the Thrift IDL (Plan 04+)
        return null;
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public Set<TableOperation> getSupportedOperations() {
        return Sets.newHashSet(TableOperation.READ);
    }
}
