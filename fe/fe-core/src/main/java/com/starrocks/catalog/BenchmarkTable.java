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

import com.starrocks.connector.benchmark.BenchmarkCatalogConfig;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;

import java.util.Collections;
import java.util.List;

public class BenchmarkTable extends Table {
    private final String catalogName;
    private final String databaseName;
    private final String tableName;
    private final double scaleFactor;

    public BenchmarkTable(long id, String catalogName, String dbName, String tableName, List<Column> schema,
                          BenchmarkCatalogConfig config) {
        super(id, tableName, TableType.TPCDS, schema);
        this.catalogName = catalogName;
        this.databaseName = dbName;
        this.tableName = tableName;
        this.scaleFactor = config.getScaleFactor();
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getCatalogDBName() {
        return databaseName;
    }

    @Override
    public String getCatalogTableName() {
        return tableName;
    }

    public double getScaleFactor() {
        return scaleFactor;
    }

    @Override
    public List<Column> getPartitionColumns() {
        return Collections.emptyList();
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return Collections.emptyList();
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        return new TTableDescriptor(getId(), TTableType.BENCHMARK_TABLE, fullSchema.size(), 0, name, databaseName);
    }

    @Override
    public boolean isSupported() {
        return true;
    }
}
