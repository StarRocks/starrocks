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

package com.starrocks.connector.iceberg.alter;

import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

import java.util.Locale;
import java.util.Map;

import static com.starrocks.analysis.OutFileClause.PARQUET_COMPRESSION_TYPE_MAP;
import static com.starrocks.connector.iceberg.IcebergMetadata.COMPRESSION_CODEC;
import static com.starrocks.connector.iceberg.IcebergMetadata.FILE_FORMAT;
import static com.starrocks.connector.iceberg.IcebergMetadata.LOCATION_PROPERTY;

public class ModifyPropertiesAction implements AlterTableAction {

    @Override
    public void execute(Transaction transaction, AlterClause clause) {

        Preconditions.checkArgument(clause instanceof ModifyTablePropertiesClause, "alter clause instance is not correct.");
        ModifyTablePropertiesClause propertiesClause = (ModifyTablePropertiesClause) clause;
        Map<String, String> modifiedProperties = propertiesClause.getProperties();
        Preconditions.checkArgument(modifiedProperties.size() > 0, "Modified property is empty");

        modifyProperties(transaction, modifiedProperties);
    }

    private static void modifyProperties(Transaction transaction, Map<String, String> pendingUpdate) {
        UpdateProperties updateProperties = transaction.updateProperties();
        for (Map.Entry<String, String> entry : pendingUpdate.entrySet()) {
            Preconditions.checkNotNull(entry.getValue(), new StarRocksConnectorException("property value cannot be null"));
            switch (entry.getKey().toLowerCase()) {
                case FILE_FORMAT:
                    updateProperties.defaultFormat(FileFormat.fromString(entry.getValue()));
                    break;
                case LOCATION_PROPERTY:
                    updateProperties.commit();
                    transaction.updateLocation().setLocation(entry.getValue()).commit();
                    break;
                case COMPRESSION_CODEC:
                    Preconditions.checkArgument(
                            PARQUET_COMPRESSION_TYPE_MAP.containsKey(entry.getValue().toLowerCase(Locale.ROOT)),
                            "Unsupported compression codec for iceberg connector: " + entry.getValue());

                    String fileFormat = pendingUpdate.get(FILE_FORMAT);
                    // only modify compression_codec or modify both file_format and compression_codec.
                    String currentFileFormat = fileFormat != null ? fileFormat : transaction.table().properties()
                            .getOrDefault(TableProperties.DEFAULT_FILE_FORMAT,
                                    TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);

                    updateCodeCompr(updateProperties, FileFormat.fromString(currentFileFormat), entry.getValue());
                    break;
                default:
                    updateProperties.set(entry.getKey(), entry.getValue());
            }
        }

        updateProperties.commit();
    }

    private static void updateCodeCompr(UpdateProperties updateProperties, FileFormat fileFormat, String codeCompression) {
        switch (fileFormat) {
            case PARQUET:
                updateProperties.set(TableProperties.PARQUET_COMPRESSION, codeCompression);
                break;
            case ORC:
                updateProperties.set(TableProperties.ORC_COMPRESSION, codeCompression);
                break;
            case AVRO:
                updateProperties.set(TableProperties.AVRO_COMPRESSION, codeCompression);
                break;
            default:
                throw new StarRocksConnectorException(
                        "Unsupported file format for iceberg connector");
        }
    }
}
