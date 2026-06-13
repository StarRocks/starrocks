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

package com.starrocks.load.routineload;

import com.google.common.collect.Sets;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.SchemaChangeTypeCompatibility;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.AutoHeavySchemaChangeForbiddenException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddFieldClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.StructFieldDesc;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TColumn;
import com.starrocks.type.ArrayType;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StringType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeDeserializer;
import com.starrocks.type.TypeFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static com.starrocks.catalog.InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;

// Turns the writer schema the BE shipped (the full record, not a diff) into the ALTER clauses needed to make
// the table accommodate it. The BE escalated because something did not fit; the FE re-diffs the whole schema
// against the live table and decides, per field:
//   - name with no column                        -> ADD COLUMN (the writer type verbatim, nullable, no default)
//   - existing struct column with a new subfield -> ADD FIELD
//   - existing column whose type does not fit but the change is legal -> MODIFY (safe widening, possibly a
//     heavy rewrite if the table is not fast-schema-evolution)
//   - unresolvable -> a pause reason: a metadata-column clash, a case-only name difference, a key column, or
//     a type change the engine cannot perform (e.g. scalar to struct)
//
// Most fields in the shipped schema already fit (the BE sends everything, not just the offender), so the
// "does the column already hold the writer type" predicate is load-bearing: it must agree with the BE's
// avro_scalar_fits/avro_field_covered, or detection would loop (FE skips a field the BE keeps escalating) or
// over-alter. The writer types here are already StarRocks types the BE mapped from Avro (unions/enums
// collapsed), so this is a StarRocks-type-to-StarRocks-type comparison.
//
// Partial batch: resolvable clauses are returned to apply even when other fields are unresolvable; the caller
// applies the clauses and then pauses with the returned pause reason.
public class AvroSchemaEvolver {

    // The clauses to apply and, if any field could not be resolved, the reason to pause the job afterwards.
    public static class Plan {
        private final List<AlterClause> clauses;
        private final String pauseReason;

        Plan(List<AlterClause> clauses, String pauseReason) {
            this.clauses = clauses;
            this.pauseReason = pauseReason;
        }

        public List<AlterClause> getClauses() {
            return clauses;
        }

        public String getPauseReason() {
            return pauseReason;
        }

        public boolean shouldPause() {
            return pauseReason != null;
        }
    }

    private AvroSchemaEvolver() {
    }

    public static Plan plan(OlapTable table, PendingSchemaChange pending, Set<String> metaColumnNames) {
        List<AlterClause> clauses = new ArrayList<>();
        List<String> pauseReasons = new ArrayList<>();

        if (pending.getSchemaColumns() != null) {
            for (TColumn tcol : pending.getSchemaColumns()) {
                if (!tcol.isSetType_desc()) {
                    continue;
                }
                String name = tcol.getColumn_name();
                Type writerType = TypeDeserializer.fromThrift(tcol.getType_desc());

                if (metaColumnNames.contains(name)) {
                    pauseReasons.add("field '" + name + "' collides with a metadata column; rename one");
                    continue;
                }
                Column liveCol = table.getColumn(name);
                if (liveCol == null) {
                    clauses.add(buildAddColumn(name, writerType));
                    continue;
                }
                // getColumn is case-insensitive; StarRocks forbids two columns differing only in case, so a
                // case-only mismatch can never be resolved by ADD and must be raised to the user.
                if (!liveCol.getName().equals(name)) {
                    pauseReasons.add("field '" + name + "' differs only in case from column '"
                            + liveCol.getName() + "'; rename one");
                    continue;
                }
                // Load planning creates no source slot for generated/auto-increment columns, so the BE
                // keeps escalating a same-named writer field as uncovered no matter how well the type
                // fits; treating it as covered here would loop. Only a rename resolves the clash.
                if (liveCol.isGeneratedColumn() || liveCol.isAutoIncrement()) {
                    pauseReasons.add("field '" + name + "' collides with "
                            + (liveCol.isGeneratedColumn() ? "generated" : "auto-increment")
                            + " column '" + liveCol.getName()
                            + "', which is never loaded from the payload; rename the field");
                    continue;
                }
                Type colType = liveCol.getType();
                if (columnAccommodates(colType, writerType)) {
                    continue;
                }
                // The column would have to change to hold the writer type, but it is a key column. A key
                // type change affects sort/distribution, so it is not auto-evolved; pause for the user. (A
                // key column that already fits was skipped above -- the writer schema carries every field,
                // key columns included, and an unchanged key must not pause the job.)
                if (liveCol.isKey()) {
                    pauseReasons.add("key column '" + name + "' would need a type change to hold the new Avro "
                            + "type (" + writerType + "), which is not auto-evolved");
                    continue;
                }
                // DECIMALV2 is deprecated and the native avro reader has no V2 path, so a writer decimal
                // never fits a V2 column. Its only lossless v3 form is DECIMAL128(27, 9) (the single legal
                // V2 -> v3 conversion the engine allows); migrate to it when that form holds the writer so
                // the reader can read the field. A string writer routes through the generic path below
                // (V2 -> VARCHAR is legal and holds anything); a writer beyond V2's capacity falls through
                // to a pause.
                if (colType.isDecimalV2()) {
                    Type v3 = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 27, 9);
                    if (columnAccommodates(v3, writerType)) {
                        clauses.add(buildModify(name, v3, liveCol.isAllowNull()));
                        continue;
                    }
                }
                if (colType.isStructType() && writerType.isStructType()) {
                    diffStruct(name, new ArrayList<>(), (StructType) colType, (StructType) writerType,
                            clauses, pauseReasons);
                } else if (colType.isScalarType() && writerType.isScalarType()
                        && SchemaChangeTypeCompatibility.isSchemaChangeAllowed(colType, writerType)) {
                    clauses.add(buildModify(name, writerType, liveCol.isAllowNull()));
                } else {
                    pauseReasons.add("column '" + name + "' (" + colType + ") cannot hold the new Avro type ("
                            + writerType + ")");
                }
            }
        }

        if (pending.getWidenColumns() != null) {
            for (String name : pending.getWidenColumns()) {
                Column liveCol = table.getColumn(name);
                if (liveCol == null) {
                    continue;
                }
                Type colType = liveCol.getType();
                if (!colType.isScalarType()) {
                    continue;
                }
                // The BE only flags a length-limited varchar/varbinary (len < the max width). A column
                // already at the max -- or unbounded (len < 0) -- needs no widen; emitting the MODIFY
                // anyway would be a no-op the next daemon tick reads as "did not converge", pausing the job.
                int len = ((ScalarType) colType).getLength();
                if (len < 0 || len >= StringType.MAX_STRING_LENGTH) {
                    continue;
                }
                // Widen to the bounded max width -- the same length the BE treats as "no overflow possible"
                // (TypeDescriptor::MAX_VARCHAR_LENGTH) -- so the widened column never re-triggers detection.
                Type target = colType.isBinaryType()
                        ? TypeFactory.createVarbinary(StringType.MAX_STRING_LENGTH)
                        : TypeFactory.createVarcharType(StringType.MAX_STRING_LENGTH);
                clauses.add(buildModify(liveCol.getName(), target, liveCol.isAllowNull()));
            }
        }

        String pauseReason = pauseReasons.isEmpty() ? null : String.join("; ", pauseReasons);
        return new Plan(clauses, pauseReason);
    }

    // Applies the planned clauses under a root inner context (the caller runs this off the txn thread and
    // holding no routine-load lock). Returns the descriptions of changes that were refused because they need
    // a full table rewrite, so the caller can pause and name them.
    //
    // allowHeavyRewrite=true: all clauses go in one ALTER. A fast-schema-evolution change takes effect
    // synchronously; a heavy one runs as a background AlterJobV2. Always returns an empty list.
    //
    // allowHeavyRewrite=false: the engine is told to refuse heavy changes (forbidAutoHeavySchemaChange).
    // Clauses are applied one at a time so a refused (heavy) clause is identified exactly while the light
    // ones still take effect; the refused clauses' descriptions are returned.
    public static List<String> applyClauses(String dbName, String tableName, List<AlterClause> clauses,
                                            boolean allowHeavyRewrite) throws Exception {
        ConnectContext context = ConnectContext.buildInner();
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        try (ConnectContext.ScopeGuard guard = context.bindScope()) {
            if (allowHeavyRewrite) {
                applyOne(context, dbName, tableName, clauses);
                return new ArrayList<>();
            }
            context.setForbidAutoHeavySchemaChange(true);
            List<String> refused = new ArrayList<>();
            for (AlterClause clause : clauses) {
                try {
                    applyOne(context, dbName, tableName, Collections.singletonList(clause));
                } catch (AutoHeavySchemaChangeForbiddenException e) {
                    refused.add(describeClause(clause));
                }
            }
            return refused;
        }
    }

    private static void applyOne(ConnectContext context, String dbName, String tableName, List<AlterClause> clauses)
            throws Exception {
        QualifiedName qualifiedName =
                QualifiedName.of(Arrays.asList(DEFAULT_INTERNAL_CATALOG_NAME, dbName, tableName));
        TableRef tableRef = new TableRef(qualifiedName, null, NodePosition.ZERO);
        AlterTableStmt stmt = new AlterTableStmt(tableRef, clauses);
        Analyzer.analyze(stmt, context);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(context, stmt);
    }

    // A short, human-readable rendering of a planned clause for a pause message, so the operator sees the
    // exact change to run by hand.
    public static String describeClause(AlterClause clause) {
        if (clause instanceof AddColumnClause) {
            ColumnDef def = ((AddColumnClause) clause).getColumnDef();
            return "ADD COLUMN " + def.getName() + " " + def.getTypeDef().toSql();
        }
        if (clause instanceof ModifyColumnClause) {
            ColumnDef def = ((ModifyColumnClause) clause).getColumnDef();
            return "MODIFY COLUMN " + def.getName() + " " + def.getTypeDef().toSql();
        }
        if (clause instanceof AddFieldClause) {
            AddFieldClause add = (AddFieldClause) clause;
            return "MODIFY COLUMN " + add.getColName() + " ADD FIELD " + add.getFieldDesc().getFieldName();
        }
        return clause.toString();
    }

    // Walks an existing struct column against the writer struct: a missing subfield becomes an ADD FIELD (at
    // its nested path); a present-but-wider subfield recurses (nested struct) or, if its type changed
    // incompatibly, becomes a pause reason (a struct subfield type cannot be auto-evolved).
    private static void diffStruct(String topColName, List<String> parentPath, StructType colStruct,
                                   StructType writerStruct, List<AlterClause> clauses, List<String> pauseReasons) {
        for (StructField wf : writerStruct.getFields()) {
            StructField cf = colStruct.getField(wf.getName());
            if (cf == null) {
                StructFieldDesc desc = new StructFieldDesc(wf.getName(), new ArrayList<>(parentPath),
                        new TypeDef(wf.getType()), null);
                clauses.add(new AddFieldClause(topColName, desc, new HashMap<>()));
            } else if (!cf.getName().equals(wf.getName())) {
                // getField matched case-insensitively. Subfield names are unique ignoring case, so ADD
                // FIELD cannot resolve this; raise it to the user like the top-level case-only mismatch.
                String path = parentPath.isEmpty() ? wf.getName()
                        : String.join(".", parentPath) + "." + wf.getName();
                pauseReasons.add("struct column '" + topColName + "' subfield '" + path
                        + "' differs only in case from '" + cf.getName() + "'; rename one");
            } else if (columnAccommodates(cf.getType(), wf.getType())) {
                continue;
            } else if (cf.getType().isStructType() && wf.getType().isStructType()) {
                List<String> childPath = new ArrayList<>(parentPath);
                childPath.add(wf.getName());
                diffStruct(topColName, childPath, (StructType) cf.getType(), (StructType) wf.getType(),
                        clauses, pauseReasons);
            } else {
                String path = parentPath.isEmpty() ? wf.getName()
                        : String.join(".", parentPath) + "." + wf.getName();
                pauseReasons.add("struct column '" + topColName + "' subfield '" + path
                        + "' type change is not auto-evolved");
            }
        }
    }

    // True if column type `col` already holds writer type `writer` without an ALTER. Mirrors the BE
    // avro_field_covered/avro_scalar_fits: a string/binary column holds anything; struct/array/map recurse;
    // a scalar must be the same or a wider type in the same family.
    private static boolean columnAccommodates(Type col, Type writer) {
        if (writer.isStructType()) {
            if (!col.isStructType()) {
                return col.isStringType() || col.isBinaryType();
            }
            StructType cs = (StructType) col;
            for (StructField wf : ((StructType) writer).getFields()) {
                StructField cf = cs.getField(wf.getName());
                // getField matches case-insensitively but the BE matches subfield names exactly, so a
                // case-only match is NOT covered — or the BE would keep escalating what we skip.
                if (cf == null || !cf.getName().equals(wf.getName())
                        || !columnAccommodates(cf.getType(), wf.getType())) {
                    return false;
                }
            }
            return true;
        }
        if (writer.isArrayType()) {
            if (!col.isArrayType()) {
                return col.isStringType() || col.isBinaryType();
            }
            return columnAccommodates(((ArrayType) col).getItemType(), ((ArrayType) writer).getItemType());
        }
        if (writer.isMapType()) {
            if (!col.isMapType()) {
                return col.isStringType() || col.isBinaryType();
            }
            // Avro map keys are always strings; mirror the BE: only a string/binary key column holds
            // them (the map reader appends raw key bytes into a binary key column).
            Type colKey = ((MapType) col).getKeyType();
            if (!colKey.isStringType() && !colKey.isBinaryType()) {
                return false;
            }
            return columnAccommodates(((MapType) col).getValueType(), ((MapType) writer).getValueType());
        }
        if (col.isScalarType() && writer.isScalarType()) {
            return scalarAccommodates((ScalarType) col, (ScalarType) writer);
        }
        // Writer is a scalar but the column is complex: not representable.
        return false;
    }

    private static boolean scalarAccommodates(ScalarType col, ScalarType writer) {
        if (col.isStringType() || col.isBinaryType()) {
            return true;
        }
        // Only decimal v3 columns, mirroring the BE is_decimalv3_field_type: a writer decimal always maps
        // to v3 (get_avro_type), so a v3 column holds it when its precision/scale are wide enough. A legacy
        // DECIMALV2 column falls through to the exact-match below (v3 != v2 -> escalate), keeping FE and BE
        // in lockstep so detection neither loops nor over-alters.
        if (col.isDecimalV3()) {
            if (writer.isDecimalV3()) {
                return col.getScalarPrecision() >= writer.getScalarPrecision()
                        && col.getScalarScale() >= writer.getScalarScale();
            }
            PrimitiveType wp = writer.getPrimitiveType();
            // An integer/float into a decimal column is cast with a per-value overflow check by the reader.
            return isIntegerType(wp) || isFloatType(wp);
        }
        PrimitiveType cp = col.getPrimitiveType();
        PrimitiveType wp = writer.getPrimitiveType();
        if (isIntegerType(cp)) {
            return isIntegerType(wp) && numericRank(cp) >= numericRank(wp);
        }
        if (isFloatType(cp)) {
            if (isIntegerType(wp)) {
                return true;
            }
            return isFloatType(wp) && numericRank(cp) >= numericRank(wp);
        }
        return cp == wp;
    }

    private static boolean isIntegerType(PrimitiveType t) {
        return t == PrimitiveType.TINYINT || t == PrimitiveType.SMALLINT || t == PrimitiveType.INT
                || t == PrimitiveType.BIGINT || t == PrimitiveType.LARGEINT;
    }

    private static boolean isFloatType(PrimitiveType t) {
        return t == PrimitiveType.FLOAT || t == PrimitiveType.DOUBLE;
    }

    // Byte-width rank within the integer and float families, matching the BE numeric_rank.
    private static int numericRank(PrimitiveType t) {
        switch (t) {
            case TINYINT:
                return 1;
            case SMALLINT:
                return 2;
            case INT:
            case FLOAT:
                return 4;
            case BIGINT:
            case DOUBLE:
                return 8;
            case LARGEINT:
                return 16;
            default:
                return 0;
        }
    }

    private static AlterClause buildAddColumn(String name, Type type) {
        ColumnDef def = new ColumnDef(name, new TypeDef(type), true);
        return new AddColumnClause(def, null, null, new HashMap<>());
    }

    private static AlterClause buildModify(String name, Type type, boolean nullable) {
        ColumnDef def = new ColumnDef(name, new TypeDef(type), nullable);
        return new ModifyColumnClause(def, null, null, new HashMap<>());
    }
}
