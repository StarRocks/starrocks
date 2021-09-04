// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/OdbcTable.java

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

import com.google.common.collect.Maps;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.common.io.Text;
import com.starrocks.thrift.TTableDescriptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class OdbcTable extends Table {
    private static final Logger LOG = LogManager.getLogger(OlapTable.class);

    private static final String ODBC_CATALOG_RESOURCE = "odbc_catalog_resource";
    private static final String ODBC_HOST = "host";
    private static final String ODBC_PORT = "port";
    private static final String ODBC_USER = "user";
    private static final String ODBC_PASSWORD = "password";
    private static final String ODBC_DATABASE = "database";
    private static final String ODBC_TABLE = "table";
    private static final String ODBC_DRIVER = "driver";
    private static final String ODBC_TYPE = "odbc_type";

    private String odbcCatalogResourceName;
    private String host;
    private String port;
    private String userName;
    private String passwd;
    private String odbcDatabaseName;
    private String odbcTableName;
    private String driver;
    private String odbcTableTypeName;

    public OdbcTable() {
        super(TableType.ODBC);
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        throw new RuntimeException("odbc table not support");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        Map<String, String> serializeMap = Maps.newHashMap();

        serializeMap.put(ODBC_CATALOG_RESOURCE, odbcCatalogResourceName);
        serializeMap.put(ODBC_HOST, host);
        serializeMap.put(ODBC_PORT, port);
        serializeMap.put(ODBC_USER, userName);
        serializeMap.put(ODBC_PASSWORD, passwd);
        serializeMap.put(ODBC_DATABASE, odbcDatabaseName);
        serializeMap.put(ODBC_TABLE, odbcTableName);
        serializeMap.put(ODBC_DRIVER, driver);
        serializeMap.put(ODBC_TYPE, odbcTableTypeName);

        int size = (int) serializeMap.values().stream().filter(Objects::nonNull).count();
        out.writeInt(size);

        for (Map.Entry<String, String> kv : serializeMap.entrySet()) {
            if (kv.getValue() != null) {
                Text.writeString(out, kv.getKey());
                Text.writeString(out, kv.getValue());
            }
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        // Read Odbc meta
        int size = in.readInt();
        Map<String, String> serializeMap = Maps.newHashMap();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            serializeMap.put(key, value);
        }

        odbcCatalogResourceName = serializeMap.get(ODBC_CATALOG_RESOURCE);
        host = serializeMap.get(ODBC_HOST);
        port = serializeMap.get(ODBC_PORT);
        userName = serializeMap.get(ODBC_USER);
        passwd = serializeMap.get(ODBC_PASSWORD);
        odbcDatabaseName = serializeMap.get(ODBC_DATABASE);
        odbcTableName = serializeMap.get(ODBC_TABLE);
        driver = serializeMap.get(ODBC_DRIVER);
        odbcTableTypeName = serializeMap.get(ODBC_TYPE);
    }
}