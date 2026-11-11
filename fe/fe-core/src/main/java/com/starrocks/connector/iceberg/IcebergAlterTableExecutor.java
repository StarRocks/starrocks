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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnBuilder;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorAlterTableExecutor;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.procedure.IcebergTableProcedure;
import com.starrocks.connector.iceberg.procedure.IcebergTableProcedureContext;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AddFieldClause;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AddPartitionColumnClause;
import com.starrocks.sql.ast.AlterTableCommentClause;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.BranchOptions;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ColumnPosition;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.CreateOrReplaceBranchClause;
import com.starrocks.sql.ast.CreateOrReplaceTagClause;
import com.starrocks.sql.ast.DropBranchClause;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.DropFieldClause;
import com.starrocks.sql.ast.DropPartitionColumnClause;
import com.starrocks.sql.ast.DropTagClause;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.ReplacePartitionColumnClause;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.TagOptions;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.connector.iceberg.IcebergApiConverter.toIcebergColumnType;
import static com.starrocks.connector.iceberg.IcebergMetadata.COMMENT;
import static com.starrocks.connector.iceberg.IcebergMetadata.COMPRESSION_CODEC;
import static com.starrocks.connector.iceberg.IcebergMetadata.FILE_FORMAT;
import static com.starrocks.connector.iceberg.IcebergMetadata.LOCATION_PROPERTY;
import static com.starrocks.sql.ast.OutFileClause.PARQUET_COMPRESSION_TYPE_MAP;

public class IcebergAlterTableExecutor extends ConnectorAlterTableExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergAlterTableExecutor.class);
    private Table table;
    private IcebergCatalog icebergCatalog;
    private Transaction transaction;
    private HdfsEnvironment hdfsEnvironment;
    private ConnectContext context;

    public IcebergAlterTableExecutor(AlterTableStmt stmt,
                                     Table table,
                                     IcebergCatalog icebergCatalog,
                                     ConnectContext context,
                                     HdfsEnvironment hdfsEnvironment) {
        super(stmt);
        this.table = table;
        this.icebergCatalog = icebergCatalog;
        this.context = context;
        this.hdfsEnvironment = hdfsEnvironment;
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
            ColumnDef columnDef = clause.getColumnDef();

            addColumn(updateSchema, columnDef);

            // AFTER column / FIRST
            if (pos != null) {
                if (pos.isFirst()) {
                    updateSchema.moveFirst(columnDef.getName());
                } else if (pos.getLastCol() != null) {
                    updateSchema.moveAfter(columnDef.getName(), pos.getLastCol());
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
            List<ColumnDef> columns = clause.getColumnDefs();

            for (ColumnDef columnDef : columns) {
                addColumn(updateSchema, columnDef);
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
            ColumnDef columnDef = clause.getColumnDef();
            org.apache.iceberg.types.Type colType = toIcebergColumnType(columnDef.getType());

            // UPDATE column type
            if (!colType.isPrimitiveType()) {
                throw new StarRocksConnectorException(
                        "Cannot modify " + columnDef.getName() + ", not a primitive type");
            }
            updateSchema.updateColumn(columnDef.getName(), colType.asPrimitiveType());

            // UPDATE comment
            if (columnDef.getComment() != null) {
                updateSchema.updateColumnDoc(columnDef.getName(), columnDef.getComment());
            }

            // NOT NULL / NULL
            if (columnDef.isAllowNull()) {
                updateSchema.makeColumnOptional(columnDef.getName());
            } else {
                throw new StarRocksConnectorException(
                        "column in iceberg table must be nullable.");
            }

            if (columnDef.getDefaultValueDef().isSet) {
                Literal<?> defaultLiteral = buildIcebergDefaultLiteral(columnDef, colType);
                updateSchema.updateColumnDefault(columnDef.getName(), defaultLiteral);
            }

            // AFTER column / FIRST
            if (colPos != null) {
                if (colPos.isFirst()) {
                    updateSchema.moveFirst(columnDef.getName());
                } else if (colPos.getLastCol() != null) {
                    updateSchema.moveAfter(columnDef.getName(), colPos.getLastCol());
                } else {
                    throw new StarRocksConnectorException("Unsupported position: " + colPos);
                }
            }

            updateSchema.commit();
        });
        return null;
    }

    @Override
    public Void visitAddFieldClause(AddFieldClause clause, ConnectContext context) {
        actions.add(() -> {
            UpdateSchema updateSchema = this.transaction.updateSchema();
            Types.NestedField parent = findNestedStruct(transaction.table().schema(), clause.getColName(),
                    clause.getNestedParentFieldNames());
            String parentName = transaction.table().schema().findColumnName(parent.fieldId());
            String fieldName = clause.getFieldName();
            updateSchema.addColumn(parentName, fieldName, toIcebergColumnType(clause.getType()));

            ColumnPosition pos = clause.getFieldPos();
            if (pos != null) {
                String fullName = parentName + "." + fieldName;
                if (pos.isFirst()) {
                    updateSchema.moveFirst(fullName);
                } else {
                    updateSchema.moveAfter(fullName, parentName + "." + findChild(parent, pos.getLastCol()).name());
                }
            }
            updateSchema.commit();
        });
        return null;
    }

    @Override
    public Void visitDropFieldClause(DropFieldClause clause, ConnectContext context) {
        actions.add(() -> {
            Schema schema = transaction.table().schema();
            Types.NestedField parent = findNestedStruct(schema, clause.getColName(), clause.getNestedParentFieldNames());
            Types.NestedField field = findChild(parent, clause.getFieldName());
            // StarRocks has no empty struct type, so the column would become unreadable
            if (parent.type().asStructType().fields().size() == 1) {
                throw new StarRocksConnectorException("Cannot drop %s, the only field of %s", field.name(),
                        parent.name());
            }
            transaction.updateSchema().deleteColumn(schema.findColumnName(field.fieldId())).commit();
        });
        return null;
    }

    // Walks a column down a nested field path: a name steps into a struct field and [*] into an array element.
    // Returns the struct the path ends at.
    private static Types.NestedField findNestedStruct(Schema schema, String columnName, List<String> path) {
        Types.NestedField field = schema.caseInsensitiveFindField(columnName);
        if (field == null) {
            throw new StarRocksConnectorException("Column %s does not exist", columnName);
        }
        for (String name : path) {
            org.apache.iceberg.types.Type type = field.type();
            if (type.isListType()) {
                field = type.asListType().fields().get(0);
            } else if (type.isStructType()) {
                field = findChild(field, name);
            } else {
                throw new StarRocksConnectorException("Field %s of column %s is not a struct or an array", name,
                        columnName);
            }
        }
        if (!field.type().isStructType()) {
            throw new StarRocksConnectorException("Field %s is not a struct", field.name());
        }
        return field;
    }

    private static Types.NestedField findChild(Types.NestedField struct, String name) {
        Types.NestedField child = struct.type().asStructType().caseInsensitiveField(name);
        if (child == null) {
            throw new StarRocksConnectorException("Field %s does not exist in %s", name, struct.name());
        }
        return child;
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
        ConnectContext ctx = this.context != null ? this.context : ConnectContext.get();
        icebergCatalog.renameTable(ctx, tableName.getDb(), tableName.getTbl(), clause.getNewTableName());
        return null;
    }

    @Override
    public Void visitCreateOrReplaceBranchClause(CreateOrReplaceBranchClause clause, ConnectContext context) {
        actions.add(() -> {
            String branchName = clause.getBranchName();
            BranchOptions branchOptions = clause.getBranchOptions();
            boolean create = clause.isCreate();
            boolean replace = clause.isReplace();
            boolean ifNotExists = clause.isIfNotExists();

            Long snapshotId = branchOptions.getSnapshotId().orElse(
                    Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId).orElse(null));
            ManageSnapshots manageSnapshots = transaction.manageSnapshots();

            Runnable safeCreateBranch = () -> {
                if (snapshotId == null) {
                    manageSnapshots.createBranch(branchName);
                } else {
                    manageSnapshots.createBranch(branchName, snapshotId);
                }
            };

            boolean refExists = table.refs().get(branchName) != null;
            if (create && replace && !refExists) {
                safeCreateBranch.run();
            } else if (replace) {
                Preconditions.checkArgument(snapshotId != null,
                        "Cannot complete replace branch operation on %s, main has no snapshot", table.name());
                manageSnapshots.replaceBranch(branchName, snapshotId);
            } else {
                if (refExists && ifNotExists) {
                    return;
                }
                safeCreateBranch.run();
            }

            if (branchOptions.getNumSnapshots().isPresent()) {
                manageSnapshots.setMinSnapshotsToKeep(branchName, branchOptions.getNumSnapshots().get());
            }

            if (branchOptions.getSnapshotRetain().isPresent()) {
                manageSnapshots.setMaxSnapshotAgeMs(branchName, branchOptions.getSnapshotRetain().get());
            }

            if (branchOptions.getSnapshotRefRetain().isPresent()) {
                manageSnapshots.setMaxRefAgeMs(branchName, branchOptions.getSnapshotRefRetain().get());
            }

            manageSnapshots.commit();
        });
        return null;
    }

    @Override
    public Void visitCreateOrReplaceTagClause(CreateOrReplaceTagClause clause, ConnectContext context) {
        actions.add(() -> {
            String tagName = clause.getTagName();
            TagOptions tagOptions = clause.getTagOptions();
            boolean create = clause.isCreate();
            boolean replace = clause.isReplace();
            boolean ifNotExists = clause.isIfNotExists();

            Long snapshotId = tagOptions.getSnapshotId().orElse(
                    Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId).orElse(null));

            Preconditions.checkArgument(snapshotId != null,
                    "Cannot complete create or replace tag operation on %s, main has no snapshot", table.name());
            ManageSnapshots manageSnapshots = transaction.manageSnapshots();

            boolean refExists = table.refs().get(tagName) != null;

            if (create && replace && !refExists) {
                manageSnapshots.createTag(tagName, snapshotId);
            } else if (replace) {
                manageSnapshots.replaceTag(tagName, snapshotId);
            } else {
                if (refExists && ifNotExists) {
                    return;
                }
                manageSnapshots.createTag(tagName, snapshotId);
            }

            if (tagOptions.getSnapshotRefRetain().isPresent()) {
                manageSnapshots.setMaxRefAgeMs(tagName, tagOptions.getSnapshotRefRetain().get());
            }

            manageSnapshots.commit();
        });
        return null;
    }

    @Override
    public Void visitDropBranchClause(DropBranchClause clause, ConnectContext context) {
        actions.add(() -> {
            String branchName = clause.getBranch();
            boolean ifExists = clause.isIfExists();
            SnapshotRef snapshotRef = table.refs().get(branchName);

            if (snapshotRef != null || !ifExists) {
                transaction.manageSnapshots().removeBranch(branchName).commit();
            }
        });

        return null;
    }

    @Override
    public Void visitAddPartitionClause(AddPartitionClause clause, ConnectContext context) {
        return null;
    }

    @Override
    public Void visitDropTagClause(DropTagClause clause, ConnectContext context) {
        actions.add(() -> {
            String tagName = clause.getTag();
            boolean ifExists = clause.isIfExists();
            SnapshotRef snapshotRef = table.refs().get(tagName);

            if (snapshotRef != null || !ifExists) {
                transaction.manageSnapshots().removeTag(tagName).commit();
            }
        });

        return null;
    }

    @Override
    public Void visitAlterTableOperationClause(AlterTableOperationClause clause, ConnectContext context) {
        IcebergTableProcedure tableProcedure = clause.getTableProcedure();
        if (tableProcedure == null) {
            throw new StarRocksConnectorException("Table operation clause must have a table procedure");
        }
        Map<String, ConstantOperator> args = clause.getAnalyzedArgs();
        IcebergTableProcedureContext tableProcedureContext =
                new IcebergTableProcedureContext(icebergCatalog, table, context != null ? context : ConnectContext.get(),
                        transaction, hdfsEnvironment, stmt, clause);
        actions.add(() -> {
            super.resultSet = tableProcedure.execute(tableProcedureContext, args);
        });
        return null;
    }

    // A NOT NULL column can only be added to a v3 table, and only with a default: rows in the existing data
    // files read the default as the column's initial-default.
    private void addColumn(UpdateSchema updateSchema, ColumnDef columnDef) {
        org.apache.iceberg.types.Type icebergType = toIcebergColumnType(columnDef.getType());
        Literal<?> defaultLiteral = buildIcebergDefaultLiteral(columnDef, icebergType);
        if (!columnDef.isAllowNull()) {
            if (defaultLiteral == null || !(table instanceof BaseTable baseTable)
                    || baseTable.operations().current().formatVersion() < 3) {
                throw new StarRocksConnectorException(
                        "column in iceberg table must be nullable, unless it has a default value on a v3 table.");
            }
            updateSchema.addRequiredColumn(columnDef.getName(), icebergType, columnDef.getComment(), defaultLiteral);
        } else if (defaultLiteral != null) {
            updateSchema.addColumn(columnDef.getName(), icebergType, columnDef.getComment(), defaultLiteral);
        } else {
            updateSchema.addColumn(columnDef.getName(), icebergType, columnDef.getComment());
        }
    }

    private Literal<?> buildIcebergDefaultLiteral(ColumnDef columnDef, org.apache.iceberg.types.Type icebergType) {
        if (!columnDef.getDefaultValueDef().isSet) {
            return null;
        }
        Column column = ColumnBuilder.buildColumn(columnDef);
        return IcebergApiConverter.toIcebergDefaultLiteral(column, icebergType);
    }

    @Override
    public Void visitAddPartitionColumnClause(AddPartitionColumnClause clause, ConnectContext context) {
        actions.add(() -> {
            UpdatePartitionSpec spec = this.transaction.updateSpec();
            List<Term> terms = clause.getPartitionExprList().stream()
                    .map(IcebergPartitionUtils::convertPartitionExprToTerm).toList();
            try {
                for (Term term : terms) {
                    spec.addField(term);
                }
                spec.commit();
            } catch (Exception e) {
                throw new StarRocksConnectorException("Failed to add partition column: " + e.getMessage(), e);
            }
        });
        return null;
    }

    @Override
    public Void visitDropPartitionColumnClause(DropPartitionColumnClause clause, ConnectContext context) {
        actions.add(() -> {
            UpdatePartitionSpec spec = this.transaction.updateSpec();
            List<Term> terms = clause.getPartitionExprList().stream()
                    .map(IcebergPartitionUtils::convertPartitionExprToTerm).toList();
            try {
                for (Term term : terms) {
                    spec.removeField(term);
                }
                spec.commit();
            } catch (Exception e) {
                throw new StarRocksConnectorException("Failed to drop partition column: " + e.getMessage(), e);
            }
        });
        return null;
    }

    @Override
    public Void visitReplacePartitionColumnClause(ReplacePartitionColumnClause clause, ConnectContext context) {
        actions.add(() -> {
            if (table instanceof BaseTable baseTable) {
                int formatVersion = baseTable.operations().current().formatVersion();
                if (formatVersion < 2) {
                    throw new StarRocksConnectorException(
                            "REPLACE PARTITION COLUMN is only supported for Iceberg v2 tables, " +
                                    "but table is v%d format", formatVersion);
                }
            }

            UpdatePartitionSpec spec = this.transaction.updateSpec();
            Expr oldPartitionExpr = clause.getOldPartitionExpr();
            Expr newPartitionExpr = clause.getNewPartitionExpr();

            // Resolve old partition: by field name (e.g. "dt_day") or by transform expression (e.g. "day(dt)")
            String oldFieldName = resolvePartitionFieldName(oldPartitionExpr);

            String newExprStr = IcebergPartitionUtils.normalizePartitionExpr(newPartitionExpr);
            Set<String> currentPartitionExprs = table.spec().fields().stream()
                    .map(field -> IcebergApiConverter.toPartitionField(table.spec(), field, false)
                            .toLowerCase(Locale.ROOT))
                    .collect(Collectors.toSet());

            if (oldFieldName == null) {
                String oldExprStr = IcebergPartitionUtils.normalizePartitionExpr(oldPartitionExpr);
                if (oldExprStr.equalsIgnoreCase(newExprStr)) {
                    throw new StarRocksConnectorException(
                            "Failed to replace partition column: old and new partition column are the same: "
                                    + oldExprStr);
                }
                if (!currentPartitionExprs.contains(oldExprStr.toLowerCase(Locale.ROOT))) {
                    throw new StarRocksConnectorException(
                            "Failed to replace partition column: old partition column does not exist: " + oldExprStr);
                }
            }

            if (currentPartitionExprs.contains(newExprStr.toLowerCase(Locale.ROOT))) {
                throw new StarRocksConnectorException(
                        "Failed to replace partition column: new partition column already exists: " + newExprStr);
            }

            Term newTerm = IcebergPartitionUtils.convertPartitionExprToTerm(newPartitionExpr);
            try {
                if (oldFieldName != null) {
                    spec.removeField(oldFieldName);
                } else {
                    Term oldTerm = IcebergPartitionUtils.convertPartitionExprToTerm(oldPartitionExpr);
                    spec.removeField(oldTerm);
                }
                spec.addField(newTerm);
                spec.commit();
            } catch (Exception e) {
                throw new StarRocksConnectorException("Failed to replace partition column: " + e.getMessage(), e);
            }
        });
        return null;
    }

    /**
     * Check if the old partition expression is a partition field name reference (e.g., "dt_day")
     * rather than a transform expression (e.g., "day(dt)").
     * Returns the field name if resolved by name, null otherwise.
     */
    private String resolvePartitionFieldName(Expr expr) {
        if (!(expr instanceof SlotRef slotRef)) {
            return null;
        }
        String name = slotRef.getColumnName();
        // If the name matches a schema column, treat as identity transform, not a field name
        if (table.schema().findField(name) != null) {
            return null;
        }
        // Check if it matches a partition field name
        return table.spec().fields().stream()
                .filter(f -> f.name().equalsIgnoreCase(name))
                .map(f -> f.name())
                .findFirst()
                .orElse(null);
    }
}
