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

package com.starrocks.connector.iceberg;

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ConnectorScanner;
import com.starrocks.utils.loader.ThreadContextClassLoader;
import org.apache.iceberg.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static org.apache.iceberg.util.SerializationUtil.deserializeFromBase64;

public abstract class AbstractIcebergMetadataScanner extends ConnectorScanner {
    private static final Logger LOG = LogManager.getLogger(AbstractIcebergMetadataScanner.class);
    protected final String serializedTable;
    protected final String[] requiredFields;
    private final String[] metadataColumnNames;
    private final String[] metadataColumnTypes;
    private ColumnType[] requiredTypes;
    private final int fetchSize;
    protected final ClassLoader classLoader;
    protected Table table;
    protected String timezone;

    public AbstractIcebergMetadataScanner(int fetchSize, Map<String, String> params) {
        this.fetchSize = fetchSize;
        this.requiredFields = params.get("required_fields").split(",");
        this.metadataColumnNames = params.get("metadata_column_names").split(",");
        this.metadataColumnTypes = params.get("metadata_column_types").split(",");
        this.serializedTable = params.get("serialized_table");
        this.timezone = params.getOrDefault("time_zone", TimeZone.getDefault().getID());
        this.classLoader = this.getClass().getClassLoader();
    }

    public void open() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            this.table = deserializeFromBase64(serializedTable);
            parseRequiredTypes();
            initOffHeapTableWriter(requiredTypes, requiredFields, fetchSize);
            doOpen();
            initReader();
        } catch (Exception e) {
            this.close();
            String msg = "Failed to open the " + this.getClass().getSimpleName();
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    protected abstract void doOpen();

    protected abstract void initReader() throws IOException;

    private void parseRequiredTypes() {
        HashMap<String, String> columnNameToType = new HashMap<>();
        for (int i = 0; i < metadataColumnNames.length; i++) {
            columnNameToType.put(metadataColumnNames[i], metadataColumnTypes[i]);
        }

        requiredTypes = new ColumnType[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++) {
            String type = columnNameToType.get(requiredFields[i]);
            requiredTypes[i] = new ColumnType(requiredFields[i], type);
        }
    }
}
