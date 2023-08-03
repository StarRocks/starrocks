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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.catalog.Resource.ResourceType;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TJDBCTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JDBCTable extends Table {
    private static final Logger LOG = LogManager.getLogger(JDBCTable.class);

    private static final String TABLE = "table";
    private static final String RESOURCE = "resource";

    @SerializedName(value = "tn")
    private String jdbcTable;
    @SerializedName(value = "rn")
    private String resourceName;

    private Map<String, String> properties;
    private String dbName;
    private String catalogName;

    public JDBCTable() {
        super(TableType.JDBC);
    }

    public JDBCTable(long id, String name, List<Column> schema, Map<String, String> properties) throws DdlException {
        super(id, name, TableType.JDBC, schema);
        validate(properties);
    }

    public JDBCTable(long id, String name, List<Column> schema, String dbName, String catalogName,
                     Map<String, String> properties) throws DdlException {
        super(id, name, TableType.JDBC, schema);
        this.dbName = dbName;
        this.catalogName = catalogName;
        validate(properties);
    }

    public String getResourceName() {
        return resourceName;
    }

    public String getJdbcTable() {
        return jdbcTable;
    }

    public String getProperty(String propertyKey) {
        return properties.get(propertyKey);
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of jdbc table, they are: table and resource");
        }

        resourceName = properties.get(RESOURCE);
        if (Strings.isNullOrEmpty(resourceName)) {
            if (Strings.isNullOrEmpty(properties.get(JDBCResource.URI)) ||
                    Strings.isNullOrEmpty(properties.get(JDBCResource.USER)) ||
                    Strings.isNullOrEmpty(properties.get(JDBCResource.PASSWORD)) ||
                    Strings.isNullOrEmpty(properties.get(JDBCResource.DRIVER_URL)) ||
                    Strings.isNullOrEmpty(properties.get(JDBCResource.CHECK_SUM)) ||
                    Strings.isNullOrEmpty(properties.get(JDBCResource.DRIVER_CLASS))) {
                throw new DdlException("all catalog properties must be set");
            }
            jdbcTable = name;
            this.properties = properties;
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
            String uri = properties.get(JDBCResource.URI);
            String driverName = uri.replace("//", "").replace("/", "_");
            tJDBCTable.setJdbc_driver_name(driverName);
            tJDBCTable.setJdbc_driver_url(properties.get(JDBCResource.DRIVER_URL));
            tJDBCTable.setJdbc_driver_checksum(properties.get(JDBCResource.CHECK_SUM));
            tJDBCTable.setJdbc_driver_class(properties.get(JDBCResource.DRIVER_CLASS));

            if (dbName.isEmpty()) {
                tJDBCTable.setJdbc_url(properties.get(JDBCResource.URI));
            } else {
                tJDBCTable.setJdbc_url(properties.get(JDBCResource.URI) + "/" + dbName);
            }
            tJDBCTable.setJdbc_table(jdbcTable);
            tJDBCTable.setJdbc_user(properties.get(JDBCResource.USER));
            tJDBCTable.setJdbc_passwd(properties.get(JDBCResource.PASSWORD));
        }

        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.JDBC_TABLE,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setJdbcTable(tJDBCTable);
        return tTableDescriptor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        JsonObject obj = new JsonObject();
        obj.addProperty(TABLE, jdbcTable);
        obj.addProperty(RESOURCE, resourceName);
        Text.writeString(out, obj.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        String jsonStr = Text.readString(in);
        JsonObject obj = JsonParser.parseString(jsonStr).getAsJsonObject();
        jdbcTable = obj.getAsJsonPrimitive(TABLE).getAsString();
        resourceName = obj.getAsJsonPrimitive(RESOURCE).getAsString();
    }

    @Override
    public boolean isSupported() {
        return true;
    }
}
