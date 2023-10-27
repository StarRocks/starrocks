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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemotePathKey;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveRemoteFileIO;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.connector.hive.TextFileFormatDesc;
import com.starrocks.credential.azure.AzureCloudConfigurationProvider;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TFileTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.hadoop.conf.Configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class FileTable extends Table {
    public static final String JSON_KEY_FILE_PATH = "path";
    public static final String JSON_KEY_FORMAT = "format";
    private static final String JSON_RECURSIVE_DIRECTORIES = "enable_recursive_listing";
    private static final String JSON_KEY_FILE_PROPERTIES = "fileProperties";

    public static final String JSON_KEY_COLUMN_SEPARATOR = "column_separator";
    public static final String JSON_KEY_ROW_DELIMITER = "row_delimiter";
    public static final String JSON_KEY_COLLECTION_DELIMITER = "collection_delimiter";
    public static final String JSON_KEY_MAP_DELIMITER = "map_delimiter";

    @SerializedName(value = "fp")
    private Map<String, String> fileProperties = Maps.newHashMap();

    public FileTable() {
        super(TableType.FILE);
    }

    public FileTable(long id, String name, List<Column> fullSchema, Map<String, String> properties) throws DdlException {
        super(id, name, TableType.FILE, fullSchema);
        this.fileProperties = properties;
        validate(properties);
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of file table, " + "they are path and format");
        }

        String path = properties.get(JSON_KEY_FILE_PATH);
        if (Strings.isNullOrEmpty(path)) {
            throw new DdlException("path is null. Please add properties(path='xxx') when create table");
        }

        String format = properties.get(JSON_KEY_FORMAT);
        if (Strings.isNullOrEmpty(format)) {
            throw new DdlException("format is null. Please add properties(format='xxx') when create table");
        }
        if (!format.equalsIgnoreCase("parquet") && !format.equalsIgnoreCase("orc") &&
                !format.equalsIgnoreCase("text") && !format.equalsIgnoreCase("avro") &&
                !format.equalsIgnoreCase("rctext") && !format.equalsIgnoreCase("rcbinary") &&
                !format.equalsIgnoreCase("sequence")) {
            throw new DdlException("not supported format: " + format);
        }
        // Put path into fileProperties, so that we can get storage account in AzureStorageCloudConfiguration
        fileProperties.put(AzureCloudConfigurationProvider.AZURE_PATH_KEY, path);
    }

    public String getTableLocation() {
        return fileProperties.get(JSON_KEY_FILE_PATH);
    }

    public RemoteFileInputFormat getFileFormat() {
        if (fileProperties.get(JSON_KEY_FORMAT).equalsIgnoreCase("parquet")) {
            return RemoteFileInputFormat.PARQUET;
        } else if (fileProperties.get(JSON_KEY_FORMAT).equalsIgnoreCase("orc")) {
            return RemoteFileInputFormat.ORC;
        } else if (fileProperties.get(JSON_KEY_FORMAT).equalsIgnoreCase("text")) {
            return RemoteFileInputFormat.TEXT;
        } else if (fileProperties.get(JSON_KEY_FORMAT).equalsIgnoreCase("avro")) {
            return RemoteFileInputFormat.AVRO;
        } else if (fileProperties.get(JSON_KEY_FORMAT).equalsIgnoreCase("rctext")) {
            return RemoteFileInputFormat.RCTEXT;
        } else if (fileProperties.get(JSON_KEY_FORMAT).equalsIgnoreCase("rcbinary")) {
            return RemoteFileInputFormat.RCBINARY;
        } else if (fileProperties.get(JSON_KEY_FORMAT).equalsIgnoreCase("sequence")) {
            return RemoteFileInputFormat.SEQUENCE;
        } else {
            return RemoteFileInputFormat.UNKNOWN;
        }
    }

    public Map<String, String> getFileProperties() {
        return fileProperties;
    }

    public List<RemoteFileDesc> getFileDescsFromHdfs() throws DdlException {
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(fileProperties);
        Configuration configuration = hdfsEnvironment.getConfiguration();
        HiveRemoteFileIO remoteFileIO = new HiveRemoteFileIO(configuration);
        boolean recursive = Boolean.parseBoolean(fileProperties.getOrDefault(JSON_RECURSIVE_DIRECTORIES, "false"));
        RemotePathKey pathKey = new RemotePathKey(getTableLocation(), recursive, Optional.empty());

        try {
            Map<RemotePathKey, List<RemoteFileDesc>> result = remoteFileIO.getRemoteFiles(pathKey);
            if (result.isEmpty()) {
                throw new DdlException("No file exists for FileTable: " + this.getName());
            }
            List<RemoteFileDesc> remoteFileDescs = result.get(pathKey);
            if (remoteFileDescs.isEmpty()) {
                throw new DdlException("No file exists for FileTable: " + this.getName());
            }
            for (RemoteFileDesc file : remoteFileDescs) {
                if (!getTableLocation().endsWith("/") && !checkFileName(file.getFileName())) {
                    throw new DdlException("the path is a directory but didn't end with '/'");
                }
            }
            return remoteFileDescs;
        } catch (StarRocksConnectorException e) {
            throw new DdlException("doesn't get file with path: " + getTableLocation(), e);
        }
    }

    public List<RemoteFileDesc> getFileDescs() throws DdlException {
        List<RemoteFileDesc> fileDescs = getFileDescsFromHdfs();

        RemoteFileInputFormat format = getFileFormat();
        TextFileFormatDesc textFileFormatDesc = null;
        if (format.equals(RemoteFileInputFormat.TEXT)) {
            textFileFormatDesc = new TextFileFormatDesc(
                    fileProperties.getOrDefault(JSON_KEY_COLUMN_SEPARATOR, "\t"),
                    fileProperties.getOrDefault(JSON_KEY_ROW_DELIMITER, "\n"),
                    fileProperties.getOrDefault(JSON_KEY_COLLECTION_DELIMITER, ","),
                    fileProperties.getOrDefault(JSON_KEY_MAP_DELIMITER, ":")
            );
        }
        if (textFileFormatDesc != null) {
            for (RemoteFileDesc f : fileDescs) {
                f.setTextFileFormatDesc(textFileFormatDesc);
            }
        }
        return fileDescs;
    }

    private boolean checkFileName(String fileDescName) {
        return getTableLocation().endsWith(fileDescName);
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TFileTable tFileTable = new TFileTable();
        tFileTable.setLocation(getTableLocation());

        List<TColumn> tColumns = Lists.newArrayList();

        for (Column column : getBaseSchema()) {
            tColumns.add(column.toThrift());
        }
        tFileTable.setColumns(tColumns);

        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.FILE_TABLE, fullSchema.size(),
                0, "", "");
        tTableDescriptor.setFileTable(tFileTable);

        HiveStorageFormat storageFormat = HiveStorageFormat.get(fileProperties.get(JSON_KEY_FORMAT));
        tFileTable.setSerde_lib(storageFormat.getSerde());
        tFileTable.setInput_format(storageFormat.getInputFormat());

        String columnNames = fullSchema.stream().map(Column::getName).collect(Collectors.joining(","));
        String columnTypes = fullSchema.stream().map(Column::getType).map(ColumnTypeConverter::toHiveType)
                .collect(Collectors.joining("#"));
        tFileTable.setHive_column_names(columnNames);
        tFileTable.setHive_column_types(columnTypes);

        return tTableDescriptor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        JsonObject jsonObject = new JsonObject();
        if (!fileProperties.isEmpty()) {
            JsonObject jfileProperties = new JsonObject();
            for (Map.Entry<String, String> entry : fileProperties.entrySet()) {
                jfileProperties.addProperty(entry.getKey(), entry.getValue());
            }
            jsonObject.add(JSON_KEY_FILE_PROPERTIES, jfileProperties);
        }
        Text.writeString(out, jsonObject.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        String json = Text.readString(in);
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();

        if (jsonObject.has(JSON_KEY_FILE_PROPERTIES)) {
            JsonObject jHiveProperties = jsonObject.getAsJsonObject(JSON_KEY_FILE_PROPERTIES);
            for (Map.Entry<String, JsonElement> entry : jHiveProperties.entrySet()) {
                fileProperties.put(entry.getKey(), entry.getValue().getAsString());
            }
        }
    }

    @Override
    public void onReload() {
    }

    @Override
    public void onDrop(Database db, boolean force, boolean replay) {
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    public static FileTable.Builder builder() {
        return new FileTable.Builder();
    }

    public static class Builder {
        private long id;
        private String tableName;
        private List<Column> fullSchema;
        private Map<String, String> properties = Maps.newHashMap();

        public Builder() {
        }

        public FileTable.Builder setId(long id) {
            this.id = id;
            return this;
        }

        public FileTable.Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public FileTable.Builder setFullSchema(List<Column> fullSchema) {
            this.fullSchema = fullSchema;
            return this;
        }

        public FileTable.Builder setProperties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public FileTable build() throws DdlException {
            return new FileTable(id, tableName, fullSchema, properties);
        }
    }
}
