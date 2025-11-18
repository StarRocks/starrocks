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

import com.starrocks.planner.DescriptorTable;
import com.starrocks.thrift.TRedisTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisTable extends Table {

    private String catalogName = null;
    private String dbName = null;
    private Map<String, String> properties;
    private String valueDataFormat = null;
    public static final String URI = "redis_uri";
    public static final String USER = "user";
    public static final String PASSWORD = "password";

    public RedisTable() {
        super(TableType.REDIS);
    }

    public RedisTable(long id, String catalogName, String dbName, String name, List<Column> schema,
                      Map<String, String> properties, String valueDataFormat) {
        super(id, name, TableType.REDIS, schema);
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.properties = properties;
        this.valueDataFormat = valueDataFormat;
    }

    @Override
    public Map<String, String> getProperties() {
        if (properties == null) {
            this.properties = new HashMap<>();
        }
        return properties;
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {

        TRedisTable tRedisTable = new TRedisTable();
        tRedisTable.setRedis_url(properties.get(URI));
        tRedisTable.setRedis_user(properties.get(USER));
        tRedisTable.setRedis_passwd(properties.get(PASSWORD));
        tRedisTable.setValue_data_format(valueDataFormat);
        
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.REDIS_TABLE,
                fullSchema.size(), 0, getName(), dbName);
        tTableDescriptor.setRedisTable(tRedisTable);
        return tTableDescriptor;
    }
}
