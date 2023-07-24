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

package com.starrocks.paimon.reader;

import com.google.common.base.Strings;
import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnValue;
import com.starrocks.jni.connector.ConnectorScanner;
import com.starrocks.utils.loader.ThreadContextClassLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.options.CatalogOptions.METASTORE;
import static org.apache.paimon.options.CatalogOptions.URI;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;

public class PaimonSplitScanner extends ConnectorScanner {

    private static final Logger LOG = LogManager.getLogger(PaimonSplitScanner.class);

    private final String catalogType;
    private final String metastoreUri;
    private final String warehousePath;
    private final String databaseName;
    private final String tableName;
    private final String splitInfo;
    private final String predicateInfo;
    private final String[] requiredFields;
    private ColumnType[] requiredTypes;
    private DataType[] logicalTypes;
    private Table table;
    private RecordReader<InternalRow> reader;
    private RecordReader.RecordIterator<InternalRow> batch;
    private boolean continueCurrentBatch;
    private final int fetchSize;
    private final ClassLoader classLoader;

    public PaimonSplitScanner(int fetchSize, Map<String, String> params) {
        this.fetchSize = fetchSize;
        this.catalogType = params.get("catalog_type");
        this.metastoreUri = params.get("metastore_uri");
        this.warehousePath = params.get("warehouse_path");
        this.databaseName = params.get("database_name");
        this.tableName = params.get("table_name");
        this.requiredFields = params.get("required_fields").split(",");
        this.splitInfo = params.get("split_info");
        this.predicateInfo = params.get("predicate_info");
        this.classLoader = this.getClass().getClassLoader();
        for (Map.Entry<String, String> kv : params.entrySet()) {
            LOG.debug("key = " + kv.getKey() + ", value = " + kv.getValue());
        }
    }

    private void initTable() throws IOException {
        Options options = new Options();
        options.setString(METASTORE.key(), catalogType);
        options.setString(WAREHOUSE.key(), warehousePath);
        if (!Strings.isNullOrEmpty(metastoreUri)) {
            options.setString(URI.key(), metastoreUri);
        }
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        Identifier identifier = new Identifier(databaseName, tableName);
        try {
            this.table = catalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            String msg = "Failed to init the paimon table.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    private void parseRequiredTypes() {
        List<String> fieldNames = PaimonScannerUtils.fieldNames(table.rowType());
        requiredTypes = new ColumnType[requiredFields.length];
        logicalTypes = new DataType[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++) {
            int index = fieldNames.indexOf(requiredFields[i]);
            if (index == -1) {
                throw new RuntimeException(String.format("Cannot find field %s in schema %s",
                        requiredFields[i], fieldNames));
            }
            DataType dataType = table.rowType().getTypeAt(index);
            String type = PaimonTypeUtils.fromPaimonType(dataType);
            requiredTypes[i] = new ColumnType(type);
            logicalTypes[i] = dataType;
        }
    }

    private void initReader() throws IOException {

        ReadBuilder readBuilder = table.newReadBuilder();
        RowType rowType = table.rowType();
        List<String> fieldNames = PaimonScannerUtils.fieldNames(rowType);
        if (requiredFields.length < fieldNames.size()) {
            int[] projected = Arrays.stream(requiredFields).mapToInt(fieldNames::indexOf).toArray();
            readBuilder.withProjection(projected);
        }
        List<Predicate> predicates = PaimonScannerUtils.decodeStringToObject(predicateInfo);
        readBuilder.withFilter(predicates);
        Split split = PaimonScannerUtils.decodeStringToObject(splitInfo);
        reader = readBuilder.newRead().createReader(split);
    }

    @Override
    public void open() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            initTable();
            parseRequiredTypes();
            initOffHeapTableWriter(requiredTypes, requiredFields, fetchSize);
            initReader();
        } catch (Exception e) {
            close();
            String msg = "Failed to open the paimon reader.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            String msg = "Failed to close the paimon reader.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public int getNext() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (batch == null || !continueCurrentBatch) {
                if (batch != null) {
                    this.batch.releaseBatch();
                }
                this.batch = reader.readBatch();
            }
            int numRows = 0;
            if (batch != null) {
                InternalRow row = null;
                while (numRows < fetchSize) {
                    row = batch.next();
                    if (row == null) {
                        break;
                    }
                    for (int i = 0; i < requiredFields.length; i++) {
                        Object fieldData = InternalRowUtils.get(row, i, logicalTypes[i]);
                        if (fieldData == null) {
                            appendData(i, null);
                        } else {
                            ColumnValue fieldValue = new PaimonColumnValue(fieldData);
                            appendData(i, fieldValue);
                        }
                    }
                    numRows++;
                }
                this.continueCurrentBatch = row != null;
            }
            return numRows;
        } catch (Exception e) {
            close();
            String msg = "Failed to get the next off-heap table chunk of paimon.";
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }

    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("catalogType: ");
        sb.append(catalogType);
        sb.append("\n");
        sb.append("metastoreUri: ");
        sb.append(metastoreUri);
        sb.append("\n");
        sb.append("warehousePath: ");
        sb.append(warehousePath);
        sb.append("\n");
        sb.append("databaseName: ");
        sb.append(databaseName);
        sb.append("\n");
        sb.append("tableName: ");
        sb.append(tableName);
        sb.append("\n");
        sb.append("splitInfo: ");
        sb.append(splitInfo);
        sb.append("\n");
        sb.append("requiredFields: ");
        sb.append(Arrays.toString(requiredFields));
        sb.append("\n");
        sb.append("fetchSize: ");
        sb.append(fetchSize);
        sb.append("\n");
        return sb.toString();
    }
}
