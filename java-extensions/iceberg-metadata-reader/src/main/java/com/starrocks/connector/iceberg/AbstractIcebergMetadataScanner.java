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
import com.starrocks.jni.connector.ScannerHelper;
import com.starrocks.jni.connector.SelectedFields;
import com.starrocks.utils.loader.ThreadContextClassLoader;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public abstract class AbstractIcebergMetadataScanner extends ConnectorScanner {
    private static final Logger LOG = LogManager.getLogger(AbstractIcebergMetadataScanner.class);
    protected String serializedTable;
    protected final String[] requiredFields;
    private final String[] metadataColumnNames;
    private final String[] metadataColumnTypes;
    private ColumnType[] requiredTypes;
    private final String[] nestedFields;
    private final int fetchSize;
    private final int tableCacheMaxEntries;
    protected final ClassLoader classLoader;
    protected Table table;
    protected FileIO fileIO;
    protected String timezone;

    // expire after 600 seconds long enough for most credential expiration
    // max 1000 entries large enough for most use cases
    private static final TableFileIOCache TABLE_FILE_IO_CACHE = new TableFileIOCache(600, 1000);

    // Shares the deserialized table across scan tasks of the same payload so heap is
    // O(distinct_concurrent_tables * metadata_size) instead of O(scan_pool_size * metadata_size).
    // Same 600s TTL as the FileIO cache since the table carries credential-bound FileIO. Built lazily
    // on first scan (its capacity comes from the BE config via params, which a static initializer
    // cannot see) and rebuilt when that mutable capacity changes at runtime.
    private static final long TABLE_CACHE_TTL_SECONDS = 600;
    private static final int DEFAULT_TABLE_CACHE_MAX_ENTRIES = 128;
    private static volatile DeserializedTableCache tableCache;
    private static volatile int tableCacheCapacity = -1;

    public AbstractIcebergMetadataScanner(int fetchSize, Map<String, String> params) {
        this.fetchSize = fetchSize;
        this.requiredFields = params.get("required_fields").split(",");
        this.nestedFields = ScannerHelper.splitAndOmitEmptyStrings(params.getOrDefault("nested_fields", ""), ",");
        this.metadataColumnNames = params.get("metadata_column_names").split(",");
        this.metadataColumnTypes = params.get("metadata_column_types").split("#");
        this.serializedTable = params.get("serialized_table");
        this.timezone = params.getOrDefault("time_zone", TimeZone.getDefault().getID());
        this.tableCacheMaxEntries = Integer.parseInt(
                params.getOrDefault("table_cache_max_entries", String.valueOf(DEFAULT_TABLE_CACHE_MAX_ENTRIES)));
        this.classLoader = this.getClass().getClassLoader();
    }

    // BE passes the current iceberg_metadata_table_cache_capacity on every scanner, so a runtime
    // change to that mutable config takes effect on the next scan: the cache is rebuilt with the new
    // capacity. Rebuilding drops cached entries, but capacity changes are rare.
    static DeserializedTableCache tableCache(int maxEntries) {
        DeserializedTableCache cache = tableCache;
        if (cache != null && tableCacheCapacity == maxEntries) {
            return cache;
        }
        synchronized (AbstractIcebergMetadataScanner.class) {
            if (tableCache == null || tableCacheCapacity != maxEntries) {
                tableCache = new DeserializedTableCache(TABLE_CACHE_TTL_SECONDS, maxEntries);
                tableCacheCapacity = maxEntries;
            }
            return tableCache;
        }
    }

    @Override
    public void open() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            this.table = tableCache(tableCacheMaxEntries).get(serializedTable);
            // the base64 payload is tens of MB; drop the reference once the shared table is resolved
            this.serializedTable = null;
            this.fileIO = TABLE_FILE_IO_CACHE.get(table);
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

    public int getNext() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return doGetNext();
        } catch (Exception e) {
            close();
            LOG.error("Failed to get the next off-heap table chunk of {}.", this.getClass().getSimpleName(), e);
            throw new IOException(String.format("Failed to get the next off-heap table chunk of %s.",
                    this.getClass().getSimpleName()), e);
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            doClose();
        } catch (IOException e) {
            LOG.error("Failed to close the {}.", this.getClass().getSimpleName(), e);
            throw new IOException(String.format("Failed to close the %s.", this.getClass().getSimpleName()), e);
        }
    }

    protected abstract void doOpen();

    protected abstract int doGetNext();

    protected abstract void doClose() throws IOException;

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

        SelectedFields ssf = new SelectedFields();
        for (String nestField : nestedFields) {
            ssf.addNestedPath(nestField);
        }
        for (int i = 0; i < requiredFields.length; i++) {
            ColumnType type = requiredTypes[i];
            String name = requiredFields[i];
            type.pruneOnField(ssf, name);
        }
    }
}
