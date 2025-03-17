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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.catalog.Resource.ResourceType;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TJDBCTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCTable extends Table {
    private static final Logger LOG = LogManager.getLogger(JDBCTable.class);

    private static final String TABLE = "table";
    private static final String RESOURCE = "resource";
    public static final String PARTITION_NULL_VALUE = "null";

    public static final String JDBC_TABLENAME = "jdbc_tablename";

    @SerializedName(value = "tn")
    private String jdbcTable;
    @SerializedName(value = "rn")
    private String resourceName;

    private Map<String, String> connectInfo;
    private String catalogName;
    private String dbName;
    private List<Column> partitionColumns;

    public JDBCTable() {
        super(TableType.JDBC);
    }

    public JDBCTable(long id, String name, List<Column> schema, Map<String, String> properties) throws DdlException {
        super(id, name, TableType.JDBC, schema);
        validate(properties);
    }

    public JDBCTable(long id, String name, List<Column> schema, String dbName,
                     String catalogName, Map<String, String> properties) throws DdlException {
        super(id, name, TableType.JDBC, schema);
        this.catalogName = catalogName;
        this.dbName = dbName;
        validate(properties);
    }

    public JDBCTable(long id, String name, List<Column> schema, List<Column> partitionColumns, String dbName,
                     String catalogName, Map<String, String> properties) throws DdlException {
        super(id, name, TableType.JDBC, schema);
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.partitionColumns = partitionColumns;
        validate(properties);
    }

    @Override
    public String getResourceName() {
        return resourceName;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getCatalogDBName() {
        return dbName;
    }

    @Override
    public String getCatalogTableName() {
        return jdbcTable;
    }

    @Override
    public List<Column> getPartitionColumns() {
        return partitionColumns;
    }

    public Map<String, String> getConnectInfo() {
        if (connectInfo == null) {
            this.connectInfo = new HashMap<>();
        }
        return connectInfo;
    }

    @Override
    public boolean isUnPartitioned() {
        return partitionColumns == null || partitionColumns.size() == 0;
    }

    public String getConnectInfo(String connectInfoKey) {
        return connectInfo.get(connectInfoKey);
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of jdbc table, they are: table and resource");
        }

        resourceName = properties.get(RESOURCE);
        if (Strings.isNullOrEmpty(resourceName)) {
            if (properties.get(JDBCResource.USER) == null ||
                    properties.get(JDBCResource.PASSWORD) == null) {
                throw new DdlException("all catalog properties must be set");
            }
            if (Strings.isNullOrEmpty(properties.get(JDBCResource.URI)) ||
                    Strings.isNullOrEmpty(properties.get(JDBCResource.DRIVER_URL)) ||
                    Strings.isNullOrEmpty(properties.get(JDBCResource.CHECK_SUM)) ||
                    Strings.isNullOrEmpty(properties.get(JDBCResource.DRIVER_CLASS))) {
                throw new DdlException("all catalog properties must be set");
            }
            if (properties.get(JDBCTable.JDBC_TABLENAME) == null) {
                jdbcTable = name;
            } else {
                jdbcTable = properties.get(JDBCTable.JDBC_TABLENAME);
            }
            this.connectInfo = properties;
            return;
        }

        jdbcTable = properties.get(TABLE);
        if (Strings.isNullOrEmpty(jdbcTable)) {
            throw new DdlException("property " + TABLE + " must be set");
        }

        Resource resource = GlobalStateMgr.getCurrentState().getResourceMgr().getResource(resourceName);
        if (resource == null) {
            throw new DdlException("jdbc resource [" + resourceName + "] not exists");
        }
        if (resource.getType() != ResourceType.JDBC) {
            throw new DdlException("resource [" + resourceName + "] is not jdbc resource");
        }
    }

    // TODO, identify the remote table that created after deleted
    @Override
    public String getUUID() {
        if (!Strings.isNullOrEmpty(catalogName)) {
            return String.join(".", catalogName, dbName, name);
        } else {
            return Long.toString(id);
        }
    }

    private static String buildCatalogDriveName(String uri) {
        // jdbc:postgresql://172.26.194.237:5432/db_pg_select
        // -> jdbc_postgresql_172.26.194.237_5432_db_pg_select
        // requirement: it should be used as local path.
        // and there is no ':' in it to avoid be parsed into non-local filesystem.
        String ans = uri.replaceAll("[^0-9a-zA-Z]", "_");

        // currently we use this uri as part of name of download file.
        // so if this uri is too long, we might fail to write file on BE side.
        // so here we have to shorten it to reduce fail probability because of long file name.

        final String prefix = "jdbc_";
        try {
            // 256bits = 32bytes = 64hex chars.
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(ans.getBytes());
            byte[] hashBytes = digest.digest();
            StringBuilder sb = new StringBuilder();
            // it's for be side parsing: expect a _ in name.
            sb.append(prefix);
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            ans = sb.toString();
        } catch (NoSuchAlgorithmException e) {
            // don't update `ans`.
        }
        return ans;
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TJDBCTable tJDBCTable = new TJDBCTable();
        if (!Strings.isNullOrEmpty(resourceName)) {
            JDBCResource resource =
                    (JDBCResource) (GlobalStateMgr.getCurrentState().getResourceMgr().getResource(resourceName));
            tJDBCTable.setJdbc_driver_name(resource.getName());
            tJDBCTable.setJdbc_driver_url(resource.getProperty(JDBCResource.DRIVER_URL));
            tJDBCTable.setJdbc_driver_checksum(resource.getProperty(JDBCResource.CHECK_SUM));
            tJDBCTable.setJdbc_driver_class(resource.getProperty(JDBCResource.DRIVER_CLASS));

            tJDBCTable.setJdbc_url(resource.getProperty(JDBCResource.URI));
            tJDBCTable.setJdbc_table(jdbcTable);
            tJDBCTable.setJdbc_user(resource.getProperty(JDBCResource.USER));
            tJDBCTable.setJdbc_passwd(resource.getProperty(JDBCResource.PASSWORD));
        } else {
            String uri = connectInfo.get(JDBCResource.URI);
            String driverName = buildCatalogDriveName(uri);
            tJDBCTable.setJdbc_driver_name(driverName);
            tJDBCTable.setJdbc_driver_url(connectInfo.get(JDBCResource.DRIVER_URL));
            tJDBCTable.setJdbc_driver_checksum(connectInfo.get(JDBCResource.CHECK_SUM));
            tJDBCTable.setJdbc_driver_class(connectInfo.get(JDBCResource.DRIVER_CLASS));

            if (connectInfo.get(JDBC_TABLENAME) != null) {
                tJDBCTable.setJdbc_url(uri);
            } else {
                int delimiterIndex = uri.indexOf("?");
                if (delimiterIndex > 0) {
                    String urlPrefix = uri.substring(0, delimiterIndex);
                    String urlSuffix = uri.substring(delimiterIndex + 1);
                    tJDBCTable.setJdbc_url(urlPrefix + "/" + dbName + "?" + urlSuffix);
                } else {
                    tJDBCTable.setJdbc_url(uri + "/" + dbName);
                }
            }
            tJDBCTable.setJdbc_table(jdbcTable);
            tJDBCTable.setJdbc_user(connectInfo.get(JDBCResource.USER));
            tJDBCTable.setJdbc_passwd(connectInfo.get(JDBCResource.PASSWORD));
        }

        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.JDBC_TABLE,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setJdbcTable(tJDBCTable);
        return tTableDescriptor;
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    public ProtocolType getProtocolType() {
        String uri = connectInfo.get(JDBCResource.URI);
        if (StringUtils.isEmpty(uri)) {
            return ProtocolType.UNKNOWN;
        }
        URI u = URI.create(uri);
        String protocol = u.getSchemeSpecificPart();
        List<String> slices = Splitter.on(":").splitToList(protocol);
        if (CollectionUtils.isEmpty(slices) || slices.size() <= 1) {
            throw new IllegalArgumentException("illegal jdbc uri: " + uri);
        }
        protocol = slices.get(0);

        ProtocolType res = EnumUtils.getEnumIgnoreCase(ProtocolType.class, protocol);
        if (res == null) {
            return ProtocolType.UNKNOWN;
        }
        return res;
    }

    public enum ProtocolType {
        UNKNOWN,
        MYSQL,
        POSTGRES,
        ORACLE,
        MARIADB,

        CLICKHOUSE
    }
}
