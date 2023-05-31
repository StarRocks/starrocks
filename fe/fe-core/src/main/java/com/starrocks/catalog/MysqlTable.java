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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/MysqlTable.java

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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable.ReferencedPartitionInfo;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TMySQLTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.zip.Adler32;

public class MysqlTable extends Table {
    private static final Logger LOG = LogManager.getLogger(OlapTable.class);

    private static final String ODBC_CATALOG_RESOURCE = "odbc_catalog_resource";
    private static final String MYSQL_HOST = "host";
    private static final String MYSQL_PORT = "port";
    private static final String MYSQL_USER = "user";
    private static final String MYSQL_PASSWORD = "password";
    private static final String MYSQL_DATABASE = "database";
    private static final String MYSQL_TABLE = "table";

    // For starrocks, mysql table can be created by specifying odbc resource,
    // but we do not support odbc resource, so we just read this property for meta compatible
    @SerializedName(value = "rn")
    private String odbcCatalogResourceName;
    @SerializedName(value = "host")
    private String host;
    @SerializedName(value = "port")
    private String port;
    @SerializedName(value = "userName")
    private String userName;
    @SerializedName(value = "passwd")
    private String passwd;
    @SerializedName(value = "dn")
    private String mysqlDatabaseName;
    @SerializedName(value = "tn")
    private String mysqlTableName;

    public MysqlTable() {
        super(TableType.MYSQL);
    }

    public MysqlTable(long id, String name, List<Column> schema, Map<String, String> properties)
            throws DdlException {
        super(id, name, TableType.MYSQL, schema);
        validate(properties);
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of mysql table, "
                    + "they are: host, port, user, password, database and table");
        }

        // Set up
        host = properties.get(MYSQL_HOST);
        if (Strings.isNullOrEmpty(host)) {
            throw new DdlException("Host of MySQL table is null. "
                    + "Please add properties('host'='xxx.xxx.xxx.xxx') when create table");
        }

        port = properties.get(MYSQL_PORT);
        if (Strings.isNullOrEmpty(port)) {
            // Maybe null pointer or number convert
            throw new DdlException("Port of MySQL table is null. "
                    + "Please add properties('port'='3306') when create table");
        } else {
            try {
                Integer.valueOf(port);
            } catch (Exception e) {
                throw new DdlException("Port of MySQL table must be a number."
                        + "Please add properties('port'='3306') when create table");

            }
        }

        userName = properties.get(MYSQL_USER);
        if (Strings.isNullOrEmpty(userName)) {
            throw new DdlException("User of MySQL table is null. "
                    + "Please add properties('user'='root') when create table");
        }

        passwd = properties.get(MYSQL_PASSWORD);
        if (passwd == null) {
            throw new DdlException("Password of MySQL table is null. "
                    + "Please add properties('password'='xxxx') when create table");
        }

        mysqlDatabaseName = properties.get(MYSQL_DATABASE);
        if (Strings.isNullOrEmpty(mysqlDatabaseName)) {
            throw new DdlException("Database of MySQL table is null. "
                    + "Please add properties('database'='xxxx') when create table");
        }

        mysqlTableName = properties.get(MYSQL_TABLE);
        if (Strings.isNullOrEmpty(mysqlTableName)) {
            throw new DdlException("Database of MySQL table is null. "
                    + "Please add properties('table'='xxxx') when create table");
        }
    }

    private String getPropertyFromResource(String propertyName) {
        OdbcCatalogResource odbcCatalogResource = (OdbcCatalogResource)
                (GlobalStateMgr.getCurrentState().getResourceMgr().getResource(odbcCatalogResourceName));
        if (odbcCatalogResource == null) {
            throw new RuntimeException("Resource does not exist. name: " + odbcCatalogResourceName);
        }

        String property = odbcCatalogResource.getProperties(propertyName);
        if (property == null) {
            throw new RuntimeException(
                    "The property:" + propertyName + " do not set in resource " + odbcCatalogResourceName);
        }
        return property;
    }

    public String getHost() {
        if (host != null) {
            return host;
        }
        return getPropertyFromResource(MYSQL_HOST);
    }

    public String getPort() {
        if (port != null) {
            return port;
        }
        return getPropertyFromResource(MYSQL_PORT);
    }

    public String getUserName() {
        if (userName != null) {
            return userName;
        }
        return getPropertyFromResource(MYSQL_USER);
    }

    public String getPasswd() {
        if (passwd != null) {
            return passwd;
        }
        return getPropertyFromResource(MYSQL_PASSWORD);
    }

    public String getMysqlDatabaseName() {
        return mysqlDatabaseName;
    }

    public String getMysqlTableName() {
        return mysqlTableName;
    }

    @Override
    public TTableDescriptor toThrift(List<ReferencedPartitionInfo> partitions) {
        TMySQLTable tMySQLTable =
                new TMySQLTable(getHost(), getPort(), getUserName(), getPasswd(), mysqlDatabaseName, mysqlTableName);
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.MYSQL_TABLE,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setMysqlTable(tMySQLTable);
        return tTableDescriptor;
    }

    @Override
    public int getSignature(int signatureVersion) {
        Adler32 adler32 = new Adler32();
        adler32.update(signatureVersion);
        // name
        adler32.update(name.getBytes(StandardCharsets.UTF_8));
        // type
        adler32.update(type.name().getBytes(StandardCharsets.UTF_8));
        // host
        adler32.update(getHost().getBytes(StandardCharsets.UTF_8));
        // port
        adler32.update(getPort().getBytes(StandardCharsets.UTF_8));
        // username
        adler32.update(getUserName().getBytes(StandardCharsets.UTF_8));
        // passwd
        adler32.update(getPasswd().getBytes(StandardCharsets.UTF_8));
        // mysql db
        adler32.update(mysqlDatabaseName.getBytes(StandardCharsets.UTF_8));
        // mysql table
        adler32.update(mysqlTableName.getBytes(StandardCharsets.UTF_8));

        return Math.abs((int) adler32.getValue());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        Map<String, String> serializeMap = Maps.newHashMap();
        serializeMap.put(ODBC_CATALOG_RESOURCE, odbcCatalogResourceName);
        serializeMap.put(MYSQL_HOST, host);
        serializeMap.put(MYSQL_PORT, port);
        serializeMap.put(MYSQL_USER, userName);
        serializeMap.put(MYSQL_PASSWORD, passwd);
        serializeMap.put(MYSQL_DATABASE, mysqlDatabaseName);
        serializeMap.put(MYSQL_TABLE, mysqlTableName);

        int size = (int) serializeMap.values().stream().filter(v -> v != null).count();
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
        // Read MySQL meta
        int size = in.readInt();
        Map<String, String> serializeMap = Maps.newHashMap();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            serializeMap.put(key, value);
        }

        odbcCatalogResourceName = serializeMap.get(ODBC_CATALOG_RESOURCE);
        host = serializeMap.get(MYSQL_HOST);
        port = serializeMap.get(MYSQL_PORT);
        userName = serializeMap.get(MYSQL_USER);
        passwd = serializeMap.get(MYSQL_PASSWORD);
        mysqlDatabaseName = serializeMap.get(MYSQL_DATABASE);
        mysqlTableName = serializeMap.get(MYSQL_TABLE);
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public boolean supportInsert() {
        return true;
    }
}
