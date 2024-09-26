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

package com.starrocks.sql.plan;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.ResolvedField;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryAttachScanPredicate;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by liujing on 2024/6/25.
 */
public final class ScanAttachPredicateContext {

    private static final Logger LOG = LogManager.getLogger(ScanAttachPredicateContext.class);

    private static final ThreadLocal<ScanAttachPredicateContext>
            SCAN_ATTACH_PREDICATE_CONTEXT = new ThreadLocal<>();

    private static final Map<Type, Integer> NUMERIC_TYPE_RANKS = new HashMap<>();

    static {
        int order = 0;
        NUMERIC_TYPE_RANKS.put(ScalarType.NULL, order++);
        NUMERIC_TYPE_RANKS.put(ScalarType.BOOLEAN, order++);
        NUMERIC_TYPE_RANKS.put(ScalarType.TINYINT, order++);
        NUMERIC_TYPE_RANKS.put(ScalarType.SMALLINT, order++);
        NUMERIC_TYPE_RANKS.put(ScalarType.INT, order++);
        NUMERIC_TYPE_RANKS.put(ScalarType.BIGINT, order++);
        NUMERIC_TYPE_RANKS.put(ScalarType.LARGEINT, order++);
        NUMERIC_TYPE_RANKS.put(ScalarType.FLOAT, order++);
        NUMERIC_TYPE_RANKS.put(ScalarType.DOUBLE, order++);
    }

    public class ScanAttachPredicate {
        String tableName;
        String columnName;
        SlotRef attachCompareExpr;
        LiteralExpr[] attachValueExprs;

        int relationFieldIndex;
        ColumnRefOperator[] fieldMappings;
        Column[] columnMappings;
        ScalarOperator[] scalarOperators;

        ScanAttachPredicate(TableName tableName, SlotRef attachCompareExpr, LiteralExpr[] attachValueExprs) {
            this.attachCompareExpr = attachCompareExpr;
            this.attachValueExprs = new LiteralExpr[attachValueExprs.length];
            System.arraycopy(attachValueExprs, 0, this.attachValueExprs, 0, attachValueExprs.length);
            this.tableName = tableName.getNoClusterString();
            this.columnName = attachCompareExpr.getColumnName();
        }

        void resolve(Scope scope,
                     List<ColumnRefOperator> fieldMappings,
                     Map<Column, ColumnRefOperator> columnMetaToColRefMap) {
            LOG.info("ScanAttachPredicate[{}]-[{}] resolve, " +
                            "fieldMappings: {}, " +
                            "columnMetaToColRefMap: {}.",
                    this.tableName,
                    this.columnName,
                    fieldMappings,
                    columnMetaToColRefMap);

            this.fieldMappings = new ColumnRefOperator[fieldMappings.size()];
            this.columnMappings = new Column[fieldMappings.size()];
            fieldMappings.toArray(this.fieldMappings);
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap =
                    columnMetaToColRefMap
                            .entrySet()
                            .stream()
                            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
            for (int i = 0; i < this.fieldMappings.length; i++) {
                this.columnMappings[i] = colRefToColumnMetaMap.get(this.fieldMappings[i]);
                if (this.fieldMappings[i].getName().equals(this.columnName)) {
                    this.relationFieldIndex = i;
                    LOG.info("ScanAttachPredicate[{}]-[{}] resolve, " +
                                    "relationFieldIndex: {}.",
                            this.tableName,
                            this.columnName,
                            this.relationFieldIndex);
                }
            }
            ResolvedField resolvedField;
            try {
                resolvedField = scope.resolveField(this.attachCompareExpr);
                this.relationFieldIndex = resolvedField.getRelationFieldIndex();
            } catch (Exception ex) {
                resolvedField = null;
                LOG.warn("ScanAttachPredicate[{}]-[{}] resolve field error,",
                        this.tableName,
                        this.columnName,
                        ex);
            }
            this.scalarOperators = new ScalarOperator[attachValueExprs.length + 1];
            this.scalarOperators[0] = this.fieldMappings[this.relationFieldIndex];
            Column column = this.columnMappings[this.relationFieldIndex];
            this.attachCompareExpr.setType(column.getType());
            for (int i = 0; i < attachValueExprs.length; i++) {
                ScalarOperator constantOperator = visitLiteral(column, attachValueExprs[i]);
                this.scalarOperators[i + 1] = constantOperator;
            }
            LOG.info("ScanAttachPredicate[{}]-[{}] resolve, " +
                            "column: {}, " +
                            "scalarOperators: {}, " +
                            "relationFieldIndex: {}.",
                    this.tableName,
                    this.columnName,
                    column,
                    scalarOperators != null ? Arrays.toString(scalarOperators) : null,
                    this.relationFieldIndex);
        }

        public Column getAttachColumn() {
            return this.columnMappings[this.relationFieldIndex];
        }

        public ColumnRefOperator getAttachColumnRefOperator() {
            return this.fieldMappings[this.relationFieldIndex];
        }

        public ScalarOperator getAttachPredicate() {
            return new InPredicateOperator(false, scalarOperators);
        }

        public String getAttachTableName() {
            return this.tableName;
        }

        ScalarOperator visitLiteral(Column column, LiteralExpr node) {
            Type columnType = column.getType();
            if (node instanceof NullLiteral) {
                return ConstantOperator.createNull(columnType);
            }

            if (columnType.matchesType(node.getType())) {
                LOG.info("ScanAttachPredicate[{}]-[{}] visit literal, " +
                                "columnType: {} matches nodeType: {}," +
                                " realObjectValue: {}.",
                        this.tableName,
                        this.columnName,
                        columnType,
                        node.getType(),
                        node.getRealObjectValue());
                return ConstantOperator.createObject(node.getRealObjectValue(), columnType);
            } else {
                String errorMsg = null;
                ScalarOperator scalarOperator = null;
                if (NUMERIC_TYPE_RANKS.containsKey(columnType)
                        && NUMERIC_TYPE_RANKS.containsKey(node.getType())) {
                    int columnTypeRank = NUMERIC_TYPE_RANKS.get(columnType);
                    int nodeTypeRank = NUMERIC_TYPE_RANKS.get(node.getType());
                    if (nodeTypeRank <= columnTypeRank) {
                        scalarOperator = ConstantOperator.createObject(
                                getNumberLiteralValue(columnType, node), columnType);
                    } else {
                        errorMsg = String.format("ScanAttachPredicate input literal value(%s)," +
                                        " does not match table[%s] column[%s] type[%s].",
                                String.valueOf(node.getRealObjectValue()),
                                this.tableName,
                                column.getName(),
                                String.valueOf(columnType));
                    }
                } else {
                    errorMsg = String.format("ScanAttachPredicate input literal type[%s]," +
                                    " does not match table[%s] column[%s] type[%s].",
                            String.valueOf(node.getType()),
                            this.tableName,
                            column.getName(),
                            String.valueOf(columnType));
                }
                if (errorMsg != null) {
                    LOG.error(errorMsg);
                    throw new SemanticException(errorMsg);
                }
                return scalarOperator;
            }
        }

        Object getNumberLiteralValue(Type columnType, LiteralExpr expr) {
            Object value = expr.getRealObjectValue();
            if (columnType.isBoolean()) {
                return value instanceof Boolean ?
                        String.valueOf(value) :
                        ((Number) value).intValue() == 1 ? true : false;
            } else if (columnType.isTinyint()) {
                return Byte.valueOf(((Number) value).byteValue());
            } else if (columnType.isSmallint()) {
                return Short.valueOf(((Number) value).shortValue());
            } else if (columnType.isInt()) {
                return Integer.valueOf(((Number) value).intValue());
            } else if (columnType.isBigint()) {
                return Long.valueOf(((Number) value).longValue());
            } else if (columnType.isLargeint()) {
                return new BigInteger(String.valueOf(value));
            } else if (columnType.isFloat()) {
                return Float.valueOf(((Number) value).floatValue());
            } else if (columnType.isDouble()) {
                return Double.valueOf(((Number) value).doubleValue());
            } else {
                return value;
            }
        }
    }

    class SlotRefMatcher implements Predicate<TableName> {

        SlotRef attachCompareExpr;

        SlotRefMatcher(SlotRef attachCompareExpr) {
            this.attachCompareExpr = attachCompareExpr;
        }

        @Override
        public boolean test(TableName tableName) {
            String dbName = tableName.getDb();
            String tblName = tableName.getTbl();
            TableName testTableName = attachCompareExpr.getTblNameWithoutAnalyzed();
            String testDbName = testTableName.getDb();
            String testTblName = testTableName.getTbl();

            LOG.info("SlotRefMatcher test, " +
                            "dbName: {}, " +
                            "tblName: {}, " +
                            "testDbName: {}, " +
                            "testTblName: {}.",
                    dbName,
                    tblName,
                    testDbName,
                    testTblName);
            if (testDbName == null) {
                return tblName.startsWith(testTblName);
            } else {
                return dbName.equals(testDbName) && tblName.startsWith(testTblName);
            }
        }
    }

    class LogicalScan {
        TableRelation tableRelation;
        LogicalScanOperator logicalScanOperator;
        List<ColumnRefOperator> fieldMappings;

        LogicalScan(TableRelation tableRelation,
                    LogicalScanOperator logicalScanOperator,
                    List<ColumnRefOperator> fieldMappings) {
            this.tableRelation = tableRelation;
            this.logicalScanOperator = logicalScanOperator;
            this.fieldMappings = fieldMappings;
        }
    }

    private final OperatorType opType;
    private LiteralExpr[] attachValueExprs;
    private SlotRefMatcher[] slotRefMatchers;

    private Map<String, LogicalScan>
            logicalScanToTableRelationMap = new ConcurrentHashMap<>();
    private Map<PhysicalOlapScanOperator, ScanAttachPredicate>
            physicalScanToAttachPredicateMap = new ConcurrentHashMap<>();

    private ScanAttachPredicateContext(OperatorType opType,
                                       SlotRef[] attachCompareExprs,
                                       LiteralExpr[] attachValueExprs) {
        this.opType = opType;
        this.slotRefMatchers = new SlotRefMatcher[attachCompareExprs.length];
        for (int i = 0; i < slotRefMatchers.length; i++) {
            SlotRef attachCompareExpr = attachCompareExprs[i];
            this.slotRefMatchers[i] = new SlotRefMatcher(attachCompareExpr);
        }
        this.attachValueExprs = attachValueExprs;
    }

    public OperatorType getOpType() {
        return opType;
    }


    public void init(LogicalScanOperator scanOperator,
                     TableRelation tableRelation,
                     List<ColumnRefOperator> fieldMappings) {
        if (scanOperator.getId() != null && !this.logicalScanToTableRelationMap.containsKey(scanOperator.getId())) {
            logicalScanToTableRelationMap.put(scanOperator.getId(), new LogicalScan(tableRelation, scanOperator, fieldMappings));
        }
    }

    public PhysicalOlapScanOperator prepare(LogicalOlapScanOperator logicalOlapScanOperator) {
        if (this.logicalScanToTableRelationMap.containsKey(logicalOlapScanOperator.getId())) {
            LogicalScan logicalScan = logicalScanToTableRelationMap.get(logicalOlapScanOperator.getId());
            TableName tableName = logicalScan.tableRelation.getName();
            Preconditions.checkNotNull(tableName);
            Scope scope = logicalScan.tableRelation.getScope();
            List<ColumnRefOperator> fieldMappings = logicalScan.fieldMappings;
            Map<Column, ColumnRefOperator> columnMetaToColRefMap = logicalScan
                    .logicalScanOperator.getColumnMetaToColRefMap();
            ScanAttachPredicate scanAttachPredicate = null;
            for (SlotRefMatcher matcher : slotRefMatchers) {
                if (matcher.test(tableName)) {
                    ScanAttachPredicate predicate = new ScanAttachPredicate(
                            tableName,
                            matcher.attachCompareExpr,
                            this.attachValueExprs);
                    predicate.resolve(scope, fieldMappings, columnMetaToColRefMap);
                    scanAttachPredicate = predicate;
                    break;
                }
            }
            if (scanAttachPredicate != null) {
                ScalarOperator newScalarOperator;
                if (logicalOlapScanOperator.getPredicate() != null) {
                    newScalarOperator = new CompoundPredicateOperator(
                            CompoundPredicateOperator.CompoundType.AND,
                            scanAttachPredicate.getAttachPredicate(),
                            logicalOlapScanOperator.getPredicate());
                } else {
                    newScalarOperator = scanAttachPredicate.getAttachPredicate();
                }
                Map<ColumnRefOperator, Column> colRefToColumnMetaMap;
                ColumnRefOperator columnRefOperator = scanAttachPredicate.getAttachColumnRefOperator();
                if (logicalOlapScanOperator.getColRefToColumnMetaMap() != null
                        && !logicalOlapScanOperator.getColRefToColumnMetaMap().isEmpty()) {
                    colRefToColumnMetaMap = new HashMap<>(logicalOlapScanOperator.getColRefToColumnMetaMap());
                    boolean exists = logicalOlapScanOperator
                            .getColRefToColumnMetaMap()
                            .keySet()
                            .stream()
                            .filter(op -> op.getId() == columnRefOperator.getId())
                            .findAny()
                            .isPresent();
                    if (!exists) {
                        colRefToColumnMetaMap.put(columnRefOperator, scanAttachPredicate.getAttachColumn());
                    }
                } else {
                    colRefToColumnMetaMap = new HashMap<>();
                    colRefToColumnMetaMap.put(columnRefOperator, scanAttachPredicate.getAttachColumn());
                }

                PhysicalOlapScanOperator physicalOlapScanOperator = new PhysicalOlapScanOperator(
                        logicalOlapScanOperator.getTable(),
                        colRefToColumnMetaMap,
                        logicalOlapScanOperator.getDistributionSpec(),
                        logicalOlapScanOperator.getLimit(),
                        newScalarOperator,
                        logicalOlapScanOperator.getSelectedIndexId(),
                        logicalOlapScanOperator.getSelectedPartitionId(),
                        logicalOlapScanOperator.getSelectedTabletId(),
                        logicalOlapScanOperator.getHintsReplicaIds(),
                        logicalOlapScanOperator.getPrunedPartitionPredicates(),
                        logicalOlapScanOperator.getProjection(),
                        logicalOlapScanOperator.isUsePkIndex()
                );
                physicalScanToAttachPredicateMap.put(physicalOlapScanOperator, scanAttachPredicate);
                return physicalOlapScanOperator;
            } else {
                return new PhysicalOlapScanOperator(logicalOlapScanOperator);
            }
        } else {
            return new PhysicalOlapScanOperator(logicalOlapScanOperator);
        }
    }

    public void destroy() {
        this.attachValueExprs = null;
        this.slotRefMatchers = null;
        this.logicalScanToTableRelationMap.clear();
        this.physicalScanToAttachPredicateMap.clear();
    }

    public static ScanAttachPredicateContext getContext() {
        return SCAN_ATTACH_PREDICATE_CONTEXT.get();
    }

    public static void beginAttachScanPredicate(
            QueryAttachScanPredicate queryAttachScanPredicate) {
        Preconditions.checkNotNull(queryAttachScanPredicate);
        ScanAttachPredicateContext context = SCAN_ATTACH_PREDICATE_CONTEXT.get();
        if (RunMode.isSharedDataMode() && context == null) {
            context = new ScanAttachPredicateContext(
                    OperatorType.IN,
                    queryAttachScanPredicate.getAttachCompareExprs(),
                    queryAttachScanPredicate.getAttachValueExprs());
            SCAN_ATTACH_PREDICATE_CONTEXT.set(context);
            LOG.info("Begin attach scan predicate, " +
                            "attachCompareExprs: {}, " +
                            "attachValueExprs: {}.",
                    Arrays.toString(queryAttachScanPredicate.getAttachCompareExprs()),
                    Arrays.toString(queryAttachScanPredicate.getAttachValueExprs()));
        }
    }

    public static boolean isAttachScanPredicate(LogicalOlapScanOperator logicalOlapScanOperator) {
        LOG.info("Is attach scan predicate, {}.", logicalOlapScanOperator.getId());
        ScanAttachPredicateContext context = getContext();
        if (context == null) {
            return false;
        } else {
            return context.logicalScanToTableRelationMap
                    .containsKey(logicalOlapScanOperator.getId());
        }
    }

    public static void initAttachScanPredicate(LogicalOlapScanOperator logicalOlapScanOperator,
                                               TableRelation tableRelation,
                                               List<ColumnRefOperator> fieldMappings) {
        ScanAttachPredicateContext context = SCAN_ATTACH_PREDICATE_CONTEXT.get();
        if (context != null) {
            context.init(logicalOlapScanOperator, tableRelation, fieldMappings);
            LOG.info("Init attach scan predicate, {}.", logicalOlapScanOperator.getId());
        }
    }

    public static PhysicalOlapScanOperator prepareAttachScanPredicate(
            LogicalOlapScanOperator logicalOlapScanOperator) {
        ScanAttachPredicateContext context = SCAN_ATTACH_PREDICATE_CONTEXT.get();
        if (context != null) {
            try {
                return context.prepare(logicalOlapScanOperator);
            } finally {
                LOG.info("Prepare attach scan predicate, {}.",
                        logicalOlapScanOperator.getId());
            }
        } else {
            return new PhysicalOlapScanOperator(logicalOlapScanOperator);
        }
    }

    public static void endAttachScanPredicate() {
        ScanAttachPredicateContext context = SCAN_ATTACH_PREDICATE_CONTEXT.get();
        if (context != null) {
            context.destroy();
            SCAN_ATTACH_PREDICATE_CONTEXT.set(null);
            LOG.info("End attach scan predicate.");
        }
    }
}
