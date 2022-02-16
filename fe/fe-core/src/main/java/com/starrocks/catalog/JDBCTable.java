// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.catalog.Resource.ResourceType;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
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

    private static final String PROPERTY_MISSING_MSG =
            "JDBC %s is null. Please add properties('%s'='xxx') when create table";

    private static final String DB = "database";
    private static final String TABLE = "table";
    private static final String RESOURCE = "resource";


    private String resourceName;
    private String jdbcDatabase;
    private String jdbcTable;
    private Map<String, String> jdbcProperties = Maps.newHashMap();

    public JDBCTable() {
        super(TableType.JDBC);
    }

    public JDBCTable(long id, String name, List<Column> schema, Map<String, String> properties) throws DdlException {
        super(id, name, TableType.JDBC, schema);
        validate(properties);
    }

    public String getResourceName() {
        return resourceName;
    }

    public String getJdbcDatabase() {
        return jdbcDatabase;
    }

    public String getJdbcTable() {
        return jdbcTable;
    }

    public Map<String, String> getJdbcProperties() {
        return jdbcProperties;
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of jdbc table, they are: jdbc.database, jdbc.table and jdbc.resource");
        }

        Map<String, String> copiedProps = Maps.newHashMap(properties);
        jdbcDatabase = copiedProps.get(DB);
        if (Strings.isNullOrEmpty(jdbcDatabase)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, DB, DB));
        }
        copiedProps.remove(DB);

        jdbcTable = copiedProps.get(TABLE);
        if (Strings.isNullOrEmpty(jdbcTable)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, TABLE, TABLE));
        }
        copiedProps.remove(TABLE);

        resourceName = copiedProps.get(RESOURCE);
        if (Strings.isNullOrEmpty(resourceName)) {
            throw new DdlException("property " + RESOURCE + " must be set");
        }
        copiedProps.remove(RESOURCE);

        Resource resource = Catalog.getCurrentCatalog().getResourceMgr().getResource(resourceName);
        if (resource == null) {
            throw new DdlException("jdbc resource [" + resourceName + "] not exists");
        }
        if (resource.getType() != ResourceType.JDBC) {
            throw new DdlException("resource [" + resourceName + "] is not jdbc resource");
        }
        jdbcProperties = copiedProps;

    }

    // @TODO implement toThrift function

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        JDBCResource resource = (JDBCResource) (Catalog.getCurrentCatalog().getResourceMgr().getResource(resourceName));
        TJDBCTable tJDBCTable = new TJDBCTable();
        tJDBCTable.setJdbc_driver(resource.getProperty(JDBCResource.DRIVER));
        tJDBCTable.setJdbc_url(resource.getProperty(JDBCResource.URI));
        tJDBCTable.setJdbc_database(jdbcDatabase);
        tJDBCTable.setJdbc_table(jdbcTable);
        tJDBCTable.setJdbc_properties(jdbcProperties);
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.JDBC_TABLE,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setJdbcTable(tJDBCTable);
        return tTableDescriptor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        JsonObject obj = new JsonObject();
        obj.addProperty(DB, jdbcDatabase);
        obj.addProperty(TABLE, jdbcTable);
        obj.addProperty(RESOURCE, resourceName);
        for (Map.Entry<String, String> entry : jdbcProperties.entrySet()) {
            obj.addProperty(entry.getKey(), entry.getValue());
        }
        Text.writeString(out, obj.toString());
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        String jsonStr = Text.readString(in);
        JsonObject obj = JsonParser.parseString(jsonStr).getAsJsonObject();
        jdbcDatabase = obj.getAsJsonPrimitive(DB).getAsString();
        obj.remove(DB);
        jdbcTable = obj.getAsJsonPrimitive(TABLE).getAsString();
        obj.remove(TABLE);
        resourceName = obj.getAsJsonPrimitive(RESOURCE).getAsString();
        obj.remove(RESOURCE);

        jdbcProperties.clear();
        for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
            jdbcProperties.put(entry.getKey(), entry.getValue().getAsString());
        }
    }
}
