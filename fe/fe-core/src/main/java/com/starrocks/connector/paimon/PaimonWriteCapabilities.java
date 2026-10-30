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

package com.starrocks.connector.paimon;

import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import java.util.Map;

/** Capabilities of the community Paimon C++ 0.3 writer, not of Paimon DDL in general. */
public final class PaimonWriteCapabilities {
    private PaimonWriteCapabilities() {
    }

    /** Validate only the already-resolved native table; this method performs no catalog or filesystem IO. */
    public static void validateWrite(Table nativeTable) {
        if (!(nativeTable instanceof FileStoreTable)) {
            throw new StarRocksConnectorException("Paimon writes require a file-store table");
        }
        try {
            validateFileStoreWrite((FileStoreTable) nativeTable);
        } catch (StarRocksConnectorException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new StarRocksConnectorException("Invalid or unsupported Paimon write configuration");
        }
    }

    private static void validateFileStoreWrite(FileStoreTable table) {
        TableSchema schema = table.schema();
        Map<String, String> properties = table.options();
        Options typed = Options.fromMap(properties);
        for (ConfigOption<?> option : CoreOptions.getOptions()) {
            if (typed.contains(option)) {
                try {
                    typed.get(option);
                } catch (RuntimeException e) {
                    throw new StarRocksConnectorException("Invalid value for Paimon table property: %s", option.key());
                }
            }
        }
        CoreOptions options = new CoreOptions(properties);
        int bucket = options.bucket();
        if (schema.primaryKeys().isEmpty() ? bucket != -1 && bucket <= 0 : bucket <= 0) {
            throw new StarRocksConnectorException("Paimon C++ writes require bucket > 0 for primary-key tables, " +
                    "or bucket = -1 / bucket > 0 for append tables (found bucket=%s)", bucket);
        }
        if (!"parquet".equals(options.fileFormatString())
                || options.fileFormatPerLevel().values().stream().anyMatch(format -> !"parquet".equals(format))) {
            throw new StarRocksConnectorException("Paimon writes currently support only Parquet data files");
        }
        if (schema.crossPartitionUpdate()) {
            throw new StarRocksConnectorException("Paimon writes require all partition columns " +
                    "to be included in the primary key");
        }
        if (!schema.primaryKeys().isEmpty() && options.mergeEngine() != CoreOptions.MergeEngine.DEDUPLICATE) {
            throw new StarRocksConnectorException("Paimon writes currently support only the deduplicate merge engine");
        }
        if (options.dataEvolutionEnabled() || options.rowTrackingEnabled()) {
            throw new StarRocksConnectorException("Paimon writes do not support data evolution or row tracking");
        }
        if (options.dataFileExternalPaths() != null && !options.dataFileExternalPaths().isBlank()) {
            throw new StarRocksConnectorException("Paimon native writes do not support external data paths");
        }
        if (options.dataFilePathDirectory() != null) {
            throw new StarRocksConnectorException("Paimon C++ 0.3 writes do not support data-file.path-directory");
        }
        for (Map.Entry<String, String> property : properties.entrySet()) {
            if (property.getKey().startsWith("global-index.") && !property.getValue().isBlank()
                    && !"false".equalsIgnoreCase(property.getValue())) {
                throw new StarRocksConnectorException("Paimon writes do not support global indexes");
            }
        }
        for (org.apache.paimon.types.DataField field : schema.fields()) {
            validateType(field.type());
        }
        Map<String, org.apache.paimon.types.DataField> fields = schema.nameToFieldMap();
        for (String partitionKey : schema.partitionKeys()) {
            org.apache.paimon.types.DataField field = fields.get(partitionKey);
            if (field == null) {
                throw new StarRocksConnectorException("Paimon partition column does not exist: %s", partitionKey);
            }
            validatePartitionType(partitionKey, field.type());
        }
    }

    private static void validatePartitionType(String name, DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case CHAR:
            case VARCHAR:
            case DATE:
                return;
            default:
                throw new StarRocksConnectorException("Unsupported Paimon write partition type for column '%s': %s",
                        name, type.getTypeRoot());
        }
    }

    private static void validateTimestampPrecision(int precision) {
        if (precision != 0 && precision != 3 && precision != 6 && precision != 9) {
            throw new StarRocksConnectorException("Unsupported Paimon write timestamp precision: %s; " +
                    "supported precisions are 0, 3, 6 and 9", precision);
        }
    }

    private static void validateType(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case DATE:
            case VARIANT:
            case ARRAY:
            case MAP:
            case ROW:
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                validateTimestampPrecision(((TimestampType) type).getPrecision());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                validateTimestampPrecision(((LocalZonedTimestampType) type).getPrecision());
                break;
            default:
                throw new StarRocksConnectorException("Unsupported Paimon write type: %s", type.getTypeRoot());
        }
        if (type instanceof ArrayType) {
            validateType(((ArrayType) type).getElementType());
        } else if (type instanceof MapType) {
            validateType(((MapType) type).getKeyType());
            validateType(((MapType) type).getValueType());
        } else if (type instanceof RowType) {
            ((RowType) type).getFields().forEach(field -> validateType(field.type()));
        }
    }
}
