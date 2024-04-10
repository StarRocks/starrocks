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

import com.starrocks.analysis.ColumnPosition;
import com.starrocks.catalog.Column;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorAlterTableExecutor;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AlterTableCommentClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.TableRenameClause;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.analysis.OutFileClause.PARQUET_COMPRESSION_TYPE_MAP;
import static com.starrocks.connector.iceberg.IcebergApiConverter.toIcebergColumnType;
import static com.starrocks.connector.iceberg.IcebergMetadata.COMMENT;
import static com.starrocks.connector.iceberg.IcebergMetadata.COMPRESSION_CODEC;
import static com.starrocks.connector.iceberg.IcebergMetadata.FILE_FORMAT;
import static com.starrocks.connector.iceberg.IcebergMetadata.LOCATION_PROPERTY;

public class IcebergAlterTableExecutor extends ConnectorAlterTableExecutor {
    private org.apache.iceberg.Table table;
    private IcebergCatalog icebergCatalog;
    private Transaction transaction;

    public IcebergAlterTableExecutor(AlterTableStmt stmt, org.apache.iceberg.Table table, IcebergCatalog icebergCatalog) {
        super(stmt);
        this.table = table;
        this.icebergCatalog = icebergCatalog;
    }

    @Override
    public void checkConflict() throws DdlException {
    }

    @Override
    public void applyClauses() throws DdlException {
        transaction = table.newTransaction();
        super.applyClauses();
        transaction.commitTransaction();
    }

    @Override
    public Void visitAddColumnClause(AddColumnClause clause, ConnectContext context) {
        actions.add(() -> {
            UpdateSchema updateSchema = this.transaction.updateSchema();
            ColumnPosition pos = clause.getColPos();
            Column column = clause.getColumnDef().toColumn();

            // All non-partition columns must use NULL as the default value.
            if (!column.isAllowNull()) {
                throw new StarRocksConnectorException("column in iceberg table must be nullable.");
            }
            updateSchema.addColumn(
                    column.getName(),
                    toIcebergColumnType(column.getType()),
                    column.getComment());

            // AFTER column / FIRST
            if (pos != null) {
                if (pos.isFirst()) {
                    updateSchema.moveFirst(column.getName());
                } else if (pos.getLastCol() != null) {
                    updateSchema.moveAfter(column.getName(), pos.getLastCol());
                } else {
                    throw new StarRocksConnectorException("Unsupported position: " + pos);
                }
            }

            updateSchema.commit();
        });
        return null;
    }

    @Override
    public Void visitAddColumnsClause(AddColumnsClause clause, ConnectContext context) {
        actions.add(() -> {
            UpdateSchema updateSchema = this.transaction.updateSchema();
            List<Column> columns = clause
                    .getColumnDefs()
                    .stream()
                    .map(ColumnDef::toColumn)
                    .collect(Collectors.toList());

            for (Column column : columns) {
                if (!column.isAllowNull()) {
                    throw new StarRocksConnectorException("column in iceberg table must be nullable.");
                }
                updateSchema.addColumn(
                        column.getName(),
                        toIcebergColumnType(column.getType()),
                        column.getComment());
            }
            updateSchema.commit();
        });
        return null;
    }

    @Override
    public Void visitDropColumnClause(DropColumnClause clause, ConnectContext context) {
        actions.add(() -> {
            UpdateSchema updateSchema = this.transaction.updateSchema();
            String columnName = clause.getColName();
            updateSchema.deleteColumn(columnName).commit();
        });
        return null;
    }

    @Override
    public Void visitColumnRenameClause(ColumnRenameClause clause, ConnectContext context) {
        actions.add(() -> {
            ColumnRenameClause columnRenameClause = (ColumnRenameClause) clause;
            UpdateSchema updateSchema = this.transaction.updateSchema();
            updateSchema.renameColumn(columnRenameClause.getColName(), columnRenameClause.getNewColName()).commit();
        });
        return null;
    }

    @Override
    public Void visitModifyColumnClause(ModifyColumnClause clause, ConnectContext context) {
        actions.add(() -> {
            UpdateSchema updateSchema = this.transaction.updateSchema();
            ColumnPosition colPos = clause.getColPos();
            Column column = clause.getColumnDef().toColumn();
            org.apache.iceberg.types.Type colType = toIcebergColumnType(column.getType());

            // UPDATE column type
            if (!colType.isPrimitiveType()) {
                throw new StarRocksConnectorException(
                        "Cannot modify " + column.getName() + ", not a primitive type");
            }
            updateSchema.updateColumn(column.getName(), colType.asPrimitiveType());

            // UPDATE comment
            if (column.getComment() != null) {
                updateSchema.updateColumnDoc(column.getName(), column.getComment());
            }

            // NOT NULL / NULL
            if (column.isAllowNull()) {
                updateSchema.makeColumnOptional(column.getName());
            } else {
                throw new StarRocksConnectorException(
                        "column in iceberg table must be nullable.");
            }

            // AFTER column / FIRST
            if (colPos != null) {
                if (colPos.isFirst()) {
                    updateSchema.moveFirst(column.getName());
                } else if (colPos.getLastCol() != null) {
                    updateSchema.moveAfter(column.getName(), colPos.getLastCol());
                } else {
                    throw new StarRocksConnectorException("Unsupported position: " + colPos);
                }
            }

            updateSchema.commit();
        });
        return null;
    }

    @Override
    public Void visitModifyTablePropertiesClause(ModifyTablePropertiesClause clause, ConnectContext context) {
        actions.add(() -> {
            UpdateProperties updateProperties = this.transaction.updateProperties();
            Map<String, String> pendingUpdate = clause.getProperties();
            if (pendingUpdate.isEmpty()) {
                throw new StarRocksConnectorException("Modified property is empty");
            }

            for (Map.Entry<String, String> entry : pendingUpdate.entrySet()) {
                Preconditions.checkNotNull(entry.getValue(), "property value cannot be null");
                switch (entry.getKey().toLowerCase()) {
                    case FILE_FORMAT:
                        updateProperties.defaultFormat(FileFormat.fromString(entry.getValue()));
                        break;
                    case LOCATION_PROPERTY:
                        updateProperties.commit();
                        transaction.updateLocation().setLocation(entry.getValue()).commit();
                        break;
                    case COMPRESSION_CODEC:
                        if (!PARQUET_COMPRESSION_TYPE_MAP.containsKey(entry.getValue().toLowerCase(Locale.ROOT))) {
                            throw new StarRocksConnectorException(
                                    "Unsupported compression codec for iceberg connector: " + entry.getValue());
                        }

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
        });
        return null;
    }

    private void updateCodeCompr(UpdateProperties updateProperties, FileFormat fileFormat, String codeCompression) {
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

    @Override
    public Void visitAlterTableCommentClause(AlterTableCommentClause clause, ConnectContext context) {
        AlterTableCommentClause alterTableCommentClause = (AlterTableCommentClause) clause;
        UpdateProperties updateProperties = this.transaction.updateProperties();
        updateProperties.set(COMMENT, alterTableCommentClause.getNewComment()).commit();
        return null;
    }

    @Override
    public Void visitTableRenameClause(TableRenameClause clause, ConnectContext context) {
        icebergCatalog.renameTable(tableName.getDb(), tableName.getTbl(), clause.getNewTableName());
        return null;
    }
}
