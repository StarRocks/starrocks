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

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.starrocks.planner.DescriptorTable.ReferencedPartitionInfo;
import com.starrocks.thrift.TLanceTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;

import java.util.List;
import java.util.Map;

public class LanceTable extends Table {

    @SerializedName(value = "uri")
    private final String uri;

    @SerializedName(value = "catalogName")
    private final String catalogName;

    @SerializedName(value = "dbName")
    private final String dbName;

    @SerializedName(value = "restCatalog")
    private Map<String, Object> restCatalog;

    public void setRestCatalog(String uri, String tokenFile, List<String> tableId, long version) {
        restCatalog = Map.of("catalog_uri", uri, "token_file", tokenFile,
                "table_id", List.copyOf(tableId), "dataset_version", version);
    }

    public String getRestCatalogInfo() {
        return restCatalog == null ? null : new Gson().toJson(restCatalog);
    }

    public LanceTable(long id, String name, List<Column> schema, String uri) {
        this(id, name, schema, uri, null);
    }

    public LanceTable(long id, String name, List<Column> schema, String uri, String catalogName) {
        this(id, name, schema, uri, catalogName, "");
    }

    public LanceTable(long id, String name, List<Column> schema, String uri, String catalogName, String dbName) {
        super(id, name, TableType.LANCE, schema);
        this.uri = uri;
        this.catalogName = catalogName;
        this.dbName = dbName;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getCatalogDBName() {
        return dbName == null ? "" : dbName;
    }

    public String getUri() {
        return uri;
    }

    @Override
    public String getTableLocation() {
        return uri;
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public TTableDescriptor toThrift(List<ReferencedPartitionInfo> partitions) {
        TLanceTable tLanceTable = new TLanceTable();
        tLanceTable.setLance_dataset_uri(uri);

        TTableDescriptor tTableDescriptor =
                new TTableDescriptor(id, TTableType.LANCE_TABLE, fullSchema.size(), 0, name, getCatalogDBName());
        tTableDescriptor.setLanceTable(tLanceTable);
        return tTableDescriptor;
    }
}
