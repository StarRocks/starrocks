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

package com.starrocks.connector.lance;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.ReadOptions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LanceTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.planner.lance.LanceConfig;
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.ArrayType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarbinaryType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class LanceMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(LanceMetadata.class);

    private final String catalogName;
    private final Map<String, String> properties;
    private final String warehousePath;
    private final Map<String, Database> databases = new ConcurrentHashMap<>();
    private final Map<String, Table> tables = new ConcurrentHashMap<>();

    public LanceMetadata(String catalogName, Map<String, String> properties) {
        this.catalogName = catalogName;
        this.properties = properties;
        this.warehousePath = stripTrailingSlash(properties.get(LanceConnector.LANCE_CATALOG_WAREHOUSE));
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.LANCE;
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        return databases.computeIfAbsent(dbName,
                name -> new Database(CONNECTOR_ID_GENERATOR.getNextId().asInt(), name));
    }

    @Override
    public boolean dbExists(ConnectContext context, String dbName) {
        return true;
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        String key = dbName + "." + tblName;
        Table cached = tables.get(key);
        if (cached != null) {
            return cached;
        }
        String datasetUri = buildDatasetUri(dbName, tblName);
        List<Column> schema = inferSchema(datasetUri);
        LanceTable table = new LanceTable(catalogName, dbName, tblName, schema, buildTableProperties(datasetUri));
        tables.put(key, table);
        return table;
    }

    @Override
    public boolean tableExists(ConnectContext context, String dbName, String tblName) {
        try {
            return getTable(context, dbName, tblName) != null;
        } catch (Exception e) {
            return false;
        }
    }

    private String buildDatasetUri(String dbName, String tblName) {
        return warehousePath + "/" + dbName + "/" + tblName + LanceConfig.LANCE_FILE_SUFFIX;
    }

    // Carry the dataset uri and the storage credentials/endpoint into the table properties so that
    // LanceScanNode / LanceConfig.buildStorageOptions can read them on the scan path.
    private Map<String, String> buildTableProperties(String datasetUri) {
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put(LanceTable.DATASET_URI, datasetUri);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String k = entry.getKey();
            if (k.startsWith("aws.s3.") || k.startsWith(LanceConfig.PROP_RAW_OPTION_PREFIX)) {
                tableProperties.put(k, entry.getValue());
            }
        }
        return tableProperties;
    }

    private List<Column> inferSchema(String datasetUri) {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        ReadOptions.Builder builder = new ReadOptions.Builder();
        Map<String, String> storageOptions = LanceConfig.buildStorageOptions(properties);
        if (!storageOptions.isEmpty()) {
            builder.setStorageOptions(storageOptions);
        }
        try (Dataset dataset = Dataset.open(allocator, datasetUri, builder.build())) {
            Schema arrowSchema = dataset.getSchema();
            List<Column> columns = new ArrayList<>(arrowSchema.getFields().size());
            for (Field field : arrowSchema.getFields()) {
                columns.add(new Column(field.getName(), arrowToStarRocksType(field), true));
            }
            return columns;
        } catch (Exception e) {
            LOG.error("Failed to infer schema for lance dataset {}", datasetUri, e);
            throw new StarRocksConnectorException("Failed to open lance dataset %s: %s", datasetUri, e.getMessage());
        }
    }

    private static Type arrowToStarRocksType(Field field) {
        ArrowType arrowType = field.getType();
        switch (arrowType.getTypeID()) {
            case Int:
                int bitWidth = ((ArrowType.Int) arrowType).getBitWidth();
                switch (bitWidth) {
                    case 8:
                        return TypeFactory.createType(PrimitiveType.TINYINT);
                    case 16:
                        return TypeFactory.createType(PrimitiveType.SMALLINT);
                    case 32:
                        return TypeFactory.createType(PrimitiveType.INT);
                    default:
                        return TypeFactory.createType(PrimitiveType.BIGINT);
                }
            case FloatingPoint:
                ArrowType.FloatingPoint fp = (ArrowType.FloatingPoint) arrowType;
                return fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE
                        ? TypeFactory.createType(PrimitiveType.FLOAT)
                        : TypeFactory.createType(PrimitiveType.DOUBLE);
            case Bool:
                return TypeFactory.createType(PrimitiveType.BOOLEAN);
            case Utf8:
            case LargeUtf8:
                return TypeFactory.createDefaultCatalogString();
            case Date:
                return TypeFactory.createType(PrimitiveType.DATE);
            case Timestamp:
                return TypeFactory.createType(PrimitiveType.DATETIME);
            case Decimal:
                ArrowType.Decimal decimal = (ArrowType.Decimal) arrowType;
                return TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128,
                        decimal.getPrecision(), decimal.getScale());
            case Binary:
            case LargeBinary:
            case FixedSizeBinary:
                return VarbinaryType.VARBINARY;
            case List:
            case LargeList:
            case FixedSizeList:
                // e.g. a vector column FixedSizeList<float> maps to ARRAY<FLOAT>
                return new ArrayType(arrowToStarRocksType(field.getChildren().get(0)));
            default:
                throw new StarRocksConnectorException("Unsupported lance/arrow type: %s for column %s",
                        arrowType, field.getName());
        }
    }

    private static String stripTrailingSlash(String path) {
        if (path != null && path.endsWith("/")) {
            return path.substring(0, path.length() - 1);
        }
        return path;
    }
}
