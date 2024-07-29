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
import com.starrocks.jni.connector.ColumnValue;
import com.starrocks.jni.connector.ConnectorScanner;
import com.starrocks.utils.loader.ThreadContextClassLoader;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.iceberg.util.SerializationUtil.deserializeFromBase64;

public class IcebergRefsTableScanner extends ConnectorScanner {
    private static final Logger LOG = LogManager.getLogger(IcebergMetadataScanner.class);

    private final String serializedTable;
    private final String[] requiredFields;
    private final String[] metadataColumnNames;
    private final String[] metadataColumnTypes;
    private ColumnType[] requiredTypes;
    private final int fetchSize;
    private final ClassLoader classLoader;
    private Table table;
    private Iterator<Map.Entry<String, SnapshotRef>> reader;

    public IcebergRefsTableScanner(int fetchSize, Map<String, String> params) {
        this.fetchSize = fetchSize;
        this.requiredFields = params.get("required_fields").split(",");
        this.metadataColumnNames = params.get("metadata_column_names").split(",");
        this.metadataColumnTypes = params.get("metadata_column_types").split(",");
        this.serializedTable = params.get("serialized_table");
        this.classLoader = this.getClass().getClassLoader();
    }

    @Override
    public void open() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            this.table = deserializeFromBase64(serializedTable);
            parseRequiredTypes();
            initOffHeapTableWriter(requiredTypes, requiredFields, fetchSize);
            initReader();
        } catch (Exception e) {
            close();
            String msg = "Failed to open the iceberg refs table reader.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader = null;
        }
    }

    @Override
    public int getNext() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            int numRows = 0;
            for (; numRows < getTableSize(); numRows++) {
                if (!reader.hasNext()) {
                    break;
                }
                Map.Entry<String, SnapshotRef> refEntry = reader.next();
                String refName = refEntry.getKey();
                SnapshotRef ref = refEntry.getValue();
                for (int i = 0; i < requiredFields.length; i++) {
                    Object fieldData = get(requiredFields[i], refName, ref);
                    if (fieldData == null) {
                        appendData(i, null);
                    } else {
                        ColumnValue fieldValue = new IcebergMetadataColumnValue(fieldData);
                        appendData(i, fieldValue);
                    }
                }
            }
            return numRows;
        } catch (Exception e) {
            close();
            LOG.error("Failed to get the next off-heap table chunk of iceberg refs table.", e);
            throw new IOException("Failed to get the next off-heap table chunk of iceberg refs table.", e);
        }
    }

    private void initReader() {
        reader = table.refs().entrySet().iterator();
    }

    private Object get(String columnName, String refName, SnapshotRef ref) {
        switch (columnName) {
            case "name":
                return refName;
            case "type":
                return ref.isBranch() ? "BRANCH" : "TAG";
            case "snapshot_id":
                return ref.snapshotId();
            case "max_reference_age_in_ms":
                return ref.maxRefAgeMs();
            case "min_snapshots_to_keep":
                return ref.minSnapshotsToKeep();
            case "max_snapshot_age_in_ms":
                return ref.maxSnapshotAgeMs();
            default:
                throw new IllegalArgumentException("Unrecognized column name " + columnName);
        }
    }

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
