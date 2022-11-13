// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.connector.iceberg.cost.IcebergTableStatisticCalculator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHudiScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.parser.ParsingException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Utils {
    private static final Logger LOG = LogManager.getLogger(Utils.class);

    public static List<ScalarOperator> extractConjuncts(ScalarOperator root) {
        LinkedList<ScalarOperator> list = new LinkedList<>();
        if (null == root) {
            return list;
        }
        extractConjunctsImpl(root, list);
        return list;
    }

    public static void extractConjunctsImpl(ScalarOperator root, List<ScalarOperator> result) {
        if (!OperatorType.COMPOUND.equals(root.getOpType())) {
            result.add(root);
            return;
        }

        CompoundPredicateOperator cpo = (CompoundPredicateOperator) root;
        if (!cpo.isAnd()) {
            result.add(root);
            return;
        }
        extractConjunctsImpl(cpo.getChild(0), result);
        extractConjunctsImpl(cpo.getChild(1), result);
    }

    public static List<ScalarOperator> extractDisjunctive(ScalarOperator root) {
        LinkedList<ScalarOperator> list = new LinkedList<>();
        if (null == root) {
            return list;
        }
        extractDisjunctiveImpl(root, list);
        return list;
    }

    public static void extractDisjunctiveImpl(ScalarOperator root, List<ScalarOperator> result) {
        if (!OperatorType.COMPOUND.equals(root.getOpType())) {
            result.add(root);
            return;
        }

        CompoundPredicateOperator cpo = (CompoundPredicateOperator) root;
        if (!cpo.isOr()) {
            result.add(root);
            return;
        }
        extractDisjunctiveImpl(cpo.getChild(0), result);
        extractDisjunctiveImpl(cpo.getChild(1), result);
    }

    public static List<ColumnRefOperator> extractColumnRef(ScalarOperator root) {
        if (null == root || !root.isVariable()) {
            return new LinkedList<>();
        }

        LinkedList<ColumnRefOperator> list = new LinkedList<>();
        if (OperatorType.VARIABLE.equals(root.getOpType())) {
            list.add((ColumnRefOperator) root);
            return list;
        }

        for (ScalarOperator child : root.getChildren()) {
            list.addAll(extractColumnRef(child));
        }

        return list;
    }

    public static int countColumnRef(ScalarOperator root) {
        return countColumnRef(root, 0);
    }

    private static int countColumnRef(ScalarOperator root, int count) {
        if (null == root || !root.isVariable()) {
            return 0;
        }

        if (OperatorType.VARIABLE.equals(root.getOpType())) {
            return 1;
        }

        for (ScalarOperator child : root.getChildren()) {
            count += countColumnRef(child, count);
        }

        return count;
    }

    public static void extractOlapScanOperator(GroupExpression groupExpression, List<LogicalOlapScanOperator> list) {
        extractOperator(groupExpression, list, p -> OperatorType.LOGICAL_OLAP_SCAN.equals(p.getOpType()));
    }

    private static <E extends Operator> void extractOperator(GroupExpression root, List<E> list,
                                                             Predicate<Operator> lambda) {
        if (lambda.test(root.getOp())) {
            list.add((E) root.getOp());
            return;
        }

        List<Group> groups = root.getInputs();
        for (Group group : groups) {
            GroupExpression expression = group.getFirstLogicalExpression();
            extractOperator(expression, list, lambda);
        }
    }

    public static boolean containAnyColumnRefs(List<ColumnRefOperator> refs, ScalarOperator operator) {
        if (refs.isEmpty() || null == operator) {
            return false;
        }

        if (operator.isColumnRef()) {
            return refs.contains(operator);
        }

        for (ScalarOperator so : operator.getChildren()) {
            if (containAnyColumnRefs(refs, so)) {
                return true;
            }
        }

        return false;
    }

    public static boolean containColumnRef(ScalarOperator operator, String column) {
        if (null == column || null == operator) {
            return false;
        }

        if (operator.isColumnRef()) {
            return ((ColumnRefOperator) operator).getName().equalsIgnoreCase(column);
        }

        for (ScalarOperator so : operator.getChildren()) {
            if (containColumnRef(so, column)) {
                return true;
            }
        }

        return false;
    }

    public static ScalarOperator compoundOr(Collection<ScalarOperator> nodes) {
        return createCompound(CompoundPredicateOperator.CompoundType.OR, nodes);
    }

    public static ScalarOperator compoundOr(ScalarOperator... nodes) {
        return createCompound(CompoundPredicateOperator.CompoundType.OR, Arrays.asList(nodes));
    }

    public static ScalarOperator compoundAnd(Collection<ScalarOperator> nodes) {
        return createCompound(CompoundPredicateOperator.CompoundType.AND, nodes);
    }

    public static ScalarOperator compoundAnd(ScalarOperator... nodes) {
        return createCompound(CompoundPredicateOperator.CompoundType.AND, Arrays.asList(nodes));
    }

    // Build a compound tree by bottom up
    //
    // Example: compoundType.OR
    // Initial state:
    //  a b c d e
    //
    // First iteration:
    //  or    or
    //  /\    /\   e
    // a  b  c  d
    //
    // Second iteration:
    //     or   e
    //    / \
    //  or   or
    //  /\   /\
    // a  b c  d
    //
    // Last iteration:
    //       or
    //      / \
    //     or  e
    //    / \
    //  or   or
    //  /\   /\
    // a  b c  d
    public static ScalarOperator createCompound(CompoundPredicateOperator.CompoundType type,
                                                Collection<ScalarOperator> nodes) {
        LinkedList<ScalarOperator> link =
                nodes.stream().filter(Objects::nonNull).collect(Collectors.toCollection(Lists::newLinkedList));

        if (link.size() < 1) {
            return null;
        }

        if (link.size() == 1) {
            return link.get(0);
        }

        while (link.size() > 1) {
            LinkedList<ScalarOperator> buffer = new LinkedList<>();

            // combine pairs of elements
            while (link.size() >= 2) {
                buffer.add(new CompoundPredicateOperator(type, link.poll(), link.poll()));
            }

            // if there's and odd number of elements, just append the last one
            if (!link.isEmpty()) {
                buffer.add(link.remove());
            }

            // continue processing the pairs that were just built
            link = buffer;
        }
        return link.remove();
    }

    public static boolean isInnerOrCrossJoin(Operator operator) {
        if (operator instanceof LogicalJoinOperator) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
            return joinOperator.isInnerOrCrossJoin();
        }
        return false;
    }

    public static int countInnerJoinNodeSize(OptExpression root) {
        int count = 0;
        Operator operator = root.getOp();
        for (OptExpression child : root.getInputs()) {
            if (isInnerOrCrossJoin(operator) && ((LogicalJoinOperator) operator).getJoinHint().isEmpty()) {
                count += countInnerJoinNodeSize(child);
            } else {
                count = Math.max(count, countInnerJoinNodeSize(child));
            }
        }

        if (isInnerOrCrossJoin(operator) && ((LogicalJoinOperator) operator).getJoinHint().isEmpty()) {
            count += 1;
        }
        return count;
    }

    public static boolean capableSemiReorder(OptExpression root, boolean hasSemi, int joinNum, int maxJoin) {
        Operator operator = root.getOp();

        if (operator instanceof LogicalJoinOperator) {
            if (((LogicalJoinOperator) operator).getJoinType().isSemiAntiJoin()) {
                hasSemi = true;
            } else {
                joinNum = joinNum + 1;
            }

            if (joinNum > maxJoin && hasSemi) {
                return false;
            }
        }

        for (OptExpression child : root.getInputs()) {
            if (operator instanceof LogicalJoinOperator) {
                if (!capableSemiReorder(child, hasSemi, joinNum, maxJoin)) {
                    return false;
                }
            } else {
                if (!capableSemiReorder(child, false, 0, maxJoin)) {
                    return false;
                }
            }
        }

        return true;
    }

    public static boolean hasUnknownColumnsStats(OptExpression root) {
        Operator operator = root.getOp();
        if (operator instanceof LogicalScanOperator) {
            LogicalScanOperator scanOperator = (LogicalScanOperator) operator;
            List<String> colNames =
                    scanOperator.getColRefToColumnMetaMap().values().stream().map(Column::getName).collect(
                            Collectors.toList());
            if (operator instanceof LogicalOlapScanOperator) {
                Table table = scanOperator.getTable();
                if (table instanceof OlapTable) {
                    if (KeysType.AGG_KEYS.equals(((OlapTable) table).getKeysType())) {
                        List<String> keyColumnNames =
                                scanOperator.getColRefToColumnMetaMap().values().stream().filter(Column::isKey)
                                        .map(Column::getName)
                                        .collect(Collectors.toList());
                        List<ColumnStatistic> keyColumnStatisticList =
                                GlobalStateMgr.getCurrentStatisticStorage().getColumnStatistics(table, keyColumnNames);
                        return keyColumnStatisticList.stream().anyMatch(ColumnStatistic::isUnknown);
                    }
                }
                List<ColumnStatistic> columnStatisticList =
                        GlobalStateMgr.getCurrentStatisticStorage().getColumnStatistics(table, colNames);
                return columnStatisticList.stream().anyMatch(ColumnStatistic::isUnknown);
            } else if (operator instanceof LogicalHiveScanOperator || operator instanceof LogicalHudiScanOperator) {
                if (ConnectContext.get().getSessionVariable().enableHiveColumnStats()) {
                    if (operator instanceof LogicalHiveScanOperator) {
                        return ((LogicalHiveScanOperator) operator).hasUnknownColumn();
                    } else {
                        return ((LogicalHudiScanOperator) operator).hasUnknownColumn();
                    }
                }
                return true;
            } else if (operator instanceof LogicalIcebergScanOperator) {
                IcebergTable table = (IcebergTable) scanOperator.getTable();
                try {
                    List<ColumnStatistic> columnStatisticList = IcebergTableStatisticCalculator.getColumnStatistics(
                            new ArrayList<>(), table.getIcebergTable(),
                            scanOperator.getColRefToColumnMetaMap());
                    return columnStatisticList.stream().anyMatch(ColumnStatistic::isUnknown);
                } catch (Exception e) {
                    LOG.warn("Iceberg table {} get column failed. error : {}", table.getName(), e);
                    return true;
                }
            } else {
                // For other scan operators, we do not know the column statistics.
                return true;
            }
        }

        return root.getInputs().stream().anyMatch(Utils::hasUnknownColumnsStats);
    }

    public static long getLongFromDateTime(LocalDateTime dateTime) {
        return dateTime.atZone(ZoneId.systemDefault()).toInstant().getEpochSecond();
    }

    public static LocalDateTime getDatetimeFromLong(long dateTime) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(dateTime), ZoneId.systemDefault());
    }

    public static long convertBitSetToLong(BitSet bitSet, int length) {
        long gid = 0;
        for (int b = 0; b < length; ++b) {
            gid = gid * 2 + (bitSet.get(b) ? 1 : 0);
        }
        return gid;
    }

    public static ColumnRefOperator findSmallestColumnRef(List<ColumnRefOperator> columnRefOperatorList) {
        Preconditions.checkState(!columnRefOperatorList.isEmpty());
        ColumnRefOperator smallestColumnRef = columnRefOperatorList.get(0);
        int smallestColumnLength = Integer.MAX_VALUE;
        for (ColumnRefOperator columnRefOperator : columnRefOperatorList) {
            Type columnType = columnRefOperator.getType();
            if (columnType.isScalarType()) {
                int columnLength = columnType.getTypeSize();
                if (columnLength < smallestColumnLength) {
                    smallestColumnRef = columnRefOperator;
                    smallestColumnLength = columnLength;
                }
            }
        }
        return smallestColumnRef;
    }

    public static boolean canDoReplicatedJoin(OlapTable table, long selectedIndexId,
                                              Collection<Long> selectedPartitionId,
                                              Collection<Long> selectedTabletId) {
        ConnectContext ctx = ConnectContext.get();
        int backendSize = ctx.getTotalBackendNumber();
        int aliveBackendSize = ctx.getAliveBackendNumber();
        int schemaHash = table.getSchemaHashByIndexId(selectedIndexId);
        for (Long partitionId : selectedPartitionId) {
            Partition partition = table.getPartition(partitionId);
            if (table.isLakeTable()) {
                // TODO(wyb): necessary to support?
                return false;
            }
            if (table.getPartitionInfo().getReplicationNum(partitionId) < backendSize) {
                return false;
            }
            long visibleVersion = partition.getVisibleVersion();
            MaterializedIndex materializedIndex = partition.getIndex(selectedIndexId);
            // TODO(kks): improve this for loop
            for (Long id : selectedTabletId) {
                LocalTablet tablet = (LocalTablet) materializedIndex.getTablet(id);
                if (tablet != null && tablet.getQueryableReplicasSize(visibleVersion, schemaHash)
                        != aliveBackendSize) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean isEqualBinaryPredicate(ScalarOperator predicate) {
        if (predicate instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) predicate;
            return binaryPredicate.getBinaryType().isEquivalence();
        }
        if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compoundPredicate = (CompoundPredicateOperator) predicate;
            if (compoundPredicate.isAnd()) {
                return isEqualBinaryPredicate(compoundPredicate.getChild(0)) &&
                        isEqualBinaryPredicate(compoundPredicate.getChild(1));
            }
            return false;
        }
        return false;
    }

    /**
     * Try cast op to descType, return empty if failed
     */
    public static Optional<ScalarOperator> tryCastConstant(ScalarOperator op, Type descType) {
        // Forbidden cast float, because behavior isn't same with before
        if (!op.isConstantRef() || op.getType().matchesType(descType) || Type.FLOAT.equals(op.getType())
                || descType.equals(Type.FLOAT)) {
            return Optional.empty();
        }

        try {
            if (((ConstantOperator) op).isNull()) {
                return Optional.of(ConstantOperator.createNull(descType));
            }

            ConstantOperator result = ((ConstantOperator) op).castToStrictly(descType);
            if (result.toString().equalsIgnoreCase(op.toString())) {
                return Optional.of(result);
            } else if (descType.isDate() && (op.getType().isIntegerType() || op.getType().isStringType())) {
                if (op.toString().equalsIgnoreCase(result.toString().replaceAll("-", ""))) {
                    return Optional.of(result);
                }
            }
        } catch (Exception ignored) {
        }
        return Optional.empty();
    }

    // tryDecimalCastConstant is employed by ReduceCastRule to reduce BinaryPredicateOperator involving DecimalV3
    // ReduceCastRule try to reduce 'CAST(Expr<T> as U) BINOP LITERAL<S>' to
    // 'EXPR<T> BINOP CAST(LITERAL<S> as T>', only T->U casting and S->T casting are both legal, then this
    // reduction is legal, so for numerical types, S is not wider than T and T is not wider than U. for examples:
    //     CAST(IntLiteral(100,TINYINT) as DECIMAL32(9,9)) < IntLiteral(0x7f50, SMALLINT) cannot be reduced.
    //     CAST(IntLiteral(100,SMALLINT) as DECIMAL64(13,10)) < IntLiteral(101, TINYINT) can be reduced.
    public static Optional<ScalarOperator> tryDecimalCastConstant(CastOperator lhs, ConstantOperator rhs) {
        Type lhsType = lhs.getType();
        Type rhsType = rhs.getType();
        Type childType = lhs.getChild(0).getType();

        // Only handle Integer or DecimalV3 types
        if (!lhsType.isExactNumericType() ||
                !rhsType.isExactNumericType() ||
                !childType.isExactNumericType()) {
            return Optional.empty();
        }
        // Guarantee that both childType casting to lhsType and rhsType casting to childType are
        // lossless
        if (!Type.isAssignable2Decimal((ScalarType) lhsType, (ScalarType) childType) ||
                !Type.isAssignable2Decimal((ScalarType) childType, (ScalarType) rhsType)) {
            return Optional.empty();
        }

        if (rhs.isNull()) {
            return Optional.of(ConstantOperator.createNull(childType));
        }

        try {
            ConstantOperator result = rhs.castTo(childType);
            return Optional.of(result);
        } catch (Exception ignored) {
        }
        return Optional.empty();
    }

    public static ScalarOperator transTrue2Null(ScalarOperator predicates) {
        if (ConstantOperator.TRUE.equals(predicates)) {
            return null;
        }
        return predicates;
    }

    public static <T extends ScalarOperator> List<T> collect(ScalarOperator root, Class<T> clazz) {
        List<T> output = Lists.newArrayList();
        collect(root, clazz, output);
        return output;
    }

    private static <T extends ScalarOperator> void collect(ScalarOperator root, Class<T> clazz, List<T> output) {
        if (clazz.isInstance(root)) {
            output.add(clazz.cast(root));
        }

        root.getChildren().forEach(child -> collect(child, clazz, output));
    }

    public static Set<MaterializedView> getRelatedMvs(int maxLevel, List<Table> tablesToCheck) {
        Set<MaterializedView> mvs = Sets.newHashSet();
        getRelatedMvs(maxLevel, 0, tablesToCheck, mvs);
        return mvs;
    }

    public static void getRelatedMvs(int maxLevel, int currentLevel, List<Table> tablesToCheck, Set<MaterializedView> mvs) {
        if (currentLevel >= maxLevel) {
            return;
        }
        Set<MvId> newMvIds = Sets.newHashSet();
        for (Table table : tablesToCheck) {
            Set<MvId> mvIds = table.getRelatedMaterializedViews();
            if (mvIds != null && !mvIds.isEmpty()) {
                newMvIds.addAll(mvIds);
            }
        }
        if (newMvIds.isEmpty()) {
            return;
        }
        List<Table> newMvs = Lists.newArrayList();
        for (MvId mvId : newMvIds) {
            Database db = GlobalStateMgr.getCurrentState().getDb(mvId.getDbId());
            if (db == null) {
                continue;
            }
            Table table = db.getTable(mvId.getId());
            if (table == null) {
                continue;
            }
            newMvs.add(table);
            mvs.add((MaterializedView) table);
        }
        getRelatedMvs(maxLevel, currentLevel + 1, newMvs, mvs);
    }

    // get all ref tables within and below root
    public static List<Table> getAllTables(OptExpression root) {
        List<Table> tables = Lists.newArrayList();
        getAllTables(root, tables);
        return tables;
    }

    private static void getAllTables(OptExpression root, List<Table> tables) {
        if (root.getOp() instanceof LogicalScanOperator) {
            LogicalScanOperator scanOperator = (LogicalScanOperator) root.getOp();
            tables.add(scanOperator.getTable());
        } else {
            for (OptExpression child : root.getInputs()) {
                getAllTables(child, tables);
            }
        }
    }

    public static boolean isValidMVPlan(OptExpression root) {
        if (root == null) {
            return false;
        }
        if (isLogicalSPJ(root)) {
            return true;
        }
        if (isLogicalSPJG(root)) {
            LogicalAggregationOperator agg = (LogicalAggregationOperator) root.getOp();
            // having is not supported now
            return agg.getPredicate() == null;
        }
        return false;
    }

    public static boolean isLogicalSPJG(OptExpression root) {
        if (root == null) {
            return false;
        }
        Operator operator = root.getOp();
        if (!(operator instanceof LogicalAggregationOperator)) {
            return false;
        }
        LogicalAggregationOperator agg = (LogicalAggregationOperator) operator;
        if (agg.getType() != AggType.GLOBAL) {
            return false;
        }

        OptExpression child = root.inputAt(0);
        return isLogicalSPJ(child);
    }

    public static boolean isLogicalSPJ(OptExpression root) {
        if (root == null) {
            return false;
        }
        Operator operator = root.getOp();
        if (!(operator instanceof LogicalOperator)) {
            return false;
        }
        if (!(operator instanceof LogicalScanOperator)
                && !(operator instanceof LogicalProjectOperator)
                && !(operator instanceof LogicalFilterOperator)
                && !(operator instanceof LogicalJoinOperator)) {
            return false;
        }
        for (OptExpression child : root.getInputs()) {
            if (!isLogicalSPJ(child)) {
                return false;
            }
        }
        return true;
    }

    public static Pair<OptExpression, LogicalPlan> getRuleOptimizedLogicalPlan(String sql,
                                                                               ColumnRefFactory columnRefFactory,
                                                                               ConnectContext connectContext) {
        StatementBase mvStmt;
        try {
            List<StatementBase> statementBases =
                    com.starrocks.sql.parser.SqlParser.parse(sql, connectContext.getSessionVariable());
            Preconditions.checkState(statementBases.size() == 1);
            mvStmt = statementBases.get(0);
        } catch (ParsingException parsingException) {
            LOG.warn("parse sql:{} failed", sql, parsingException);
            return null;
        }
        Preconditions.checkState(mvStmt instanceof QueryStatement);
        Analyzer.analyze(mvStmt, connectContext);
        QueryRelation query = ((QueryStatement) mvStmt).getQueryRelation();
        LogicalPlan logicalPlan =
                new RelationTransformer(columnRefFactory, connectContext).transformWithSelectLimit(query);
        // optimize the sql by rule and disable rule based materialized view rewrite
        OptimizerConfig optimizerConfig = new OptimizerConfig(OptimizerConfig.OptimizerAlgorithm.RULE_BASED);
        optimizerConfig.disableRuleSet(RuleSetType.SINGLE_TABLE_MV_REWRITE);
        Optimizer optimizer = new Optimizer(optimizerConfig);
        OptExpression optimizedPlan = optimizer.optimize(
                connectContext,
                logicalPlan.getRoot(),
                new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()),
                columnRefFactory);
        return Pair.create(optimizedPlan, logicalPlan);
    }

    public static List<OptExpression> collectScanExprs(OptExpression expression) {
        List<OptExpression> scanExprs = Lists.newArrayList();
        OptExpressionVisitor scanCollector = new OptExpressionVisitor<Void, Void>() {
            @Override
            public Void visit(OptExpression optExpression, Void context) {
                for (OptExpression input : optExpression.getInputs()) {
                    super.visit(input, context);
                }
                return null;
            }

            @Override
            public Void visitLogicalTableScan(OptExpression optExpression, Void context) {
                scanExprs.add(optExpression);
                return null;
            }
        };
        expression.getOp().accept(scanCollector, expression, null);
        return scanExprs;
    }

    // get all predicates within and below root
    public static List<ScalarOperator> getAllPredicates(OptExpression root) {
        List<ScalarOperator> predicates = Lists.newArrayList();
        getAllPredicates(root, predicates);
        return predicates;
    }

    private static void getAllPredicates(OptExpression root, List<ScalarOperator> predicates) {
        Operator operator = root.getOp();
        if (operator.getPredicate() != null) {
            predicates.add(root.getOp().getPredicate());
        }
        if (operator instanceof LogicalJoinOperator) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
            if (joinOperator.getOnPredicate() != null) {
                predicates.add(joinOperator.getOnPredicate());
            }
        }
        for (OptExpression child : root.getInputs()) {
            getAllPredicates(child, predicates);
        }
    }

    public static ScalarOperator canonizePredicate(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        ScalarOperatorRewriter rewrite = new ScalarOperatorRewriter();
        return rewrite.rewrite(predicate, ScalarOperatorRewriter.DEFAULT_REWRITE_SCAN_PREDICATE_RULES);
    }

    public static ScalarOperator canonizePredicateForRewrite(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        ScalarOperatorRewriter rewrite = new ScalarOperatorRewriter();
        return rewrite.rewrite(predicate, ScalarOperatorRewriter.MV_SCALAR_REWRITE_RULES);
    }

    public static ScalarOperator getCompensationPredicateForDisjunctive(ScalarOperator src, ScalarOperator target) {
        List<ScalarOperator> srcItems = Utils.extractDisjunctive(src);
        List<ScalarOperator> targetItems = Utils.extractDisjunctive(target);
        int srcLength = srcItems.size();
        int targetLength = targetItems.size();
        if (!targetItems.containsAll(srcItems)) {
            return null;
        }
        targetItems.removeAll(srcItems);
        if (targetItems.isEmpty() && srcLength == targetLength) {
            // it is the same, so return true constant
            return ConstantOperator.createBoolean(true);
        } else if (!targetItems.isEmpty()) {
            // the target has more or item, so return src
            return src;
        } else {
            return null;
        }
    }

    public static boolean isAllEqualInnerJoin(OptExpression root) {
        Operator operator = root.getOp();
        if (!(operator instanceof LogicalOperator)) {
            return false;
        }
        if (operator instanceof LogicalJoinOperator) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
            boolean isEqualPredicate = isColumnEqualPredicate(joinOperator.getOnPredicate());
            if (joinOperator.getJoinType() != JoinOperator.INNER_JOIN || !isEqualPredicate) {
                return false;
            }
        }
        for (OptExpression child : root.getInputs()) {
            if (!isAllEqualInnerJoin(child)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isColumnEqualPredicate(ScalarOperator predicate) {
        if (predicate == null) {
            return false;
        }

        ScalarOperatorVisitor<Boolean, Void> checkVisitor = new ScalarOperatorVisitor<Boolean, Void>() {
            @Override
            public Boolean visit(ScalarOperator scalarOperator, Void context) {
                return false;
            }

            @Override
            public Boolean visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
                if (!predicate.isAnd()) {
                    return false;
                }
                for (ScalarOperator child : predicate.getChildren()) {
                    Boolean ret = child.accept(this, null);
                    if (!Boolean.TRUE.equals(ret)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public Boolean visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
                return predicate.getBinaryType().isEqual()
                        && predicate.getChild(0).isColumnRef()
                        && predicate.getChild(1).isColumnRef();
            }
        };

        return predicate.accept(checkVisitor, null);
    }

    public static Map<ColumnRefOperator, ScalarOperator> getLineage(
            OptExpression expression, ColumnRefFactory refFactory) {
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = Maps.newHashMap();
        if (expression.getOp().getProjection() != null) {
            columnRefMap.putAll(expression.getOp().getProjection().getColumnRefMap());
            // for LogicalAggregationOperator, should include aggregation
            if (expression.getOp() instanceof LogicalAggregationOperator) {
                LogicalAggregationOperator agg = (LogicalAggregationOperator) expression.getOp();
                Map<ColumnRefOperator, ScalarOperator> keyMap = agg.getGroupingKeys().stream().collect(Collectors.toMap(
                        java.util.function.Function.identity(),
                        java.util.function.Function.identity()));
                columnRefMap.putAll(keyMap);
                columnRefMap.putAll(agg.getAggregations());
            }
        } else {
            if (expression.getOp() instanceof LogicalAggregationOperator) {
                LogicalAggregationOperator agg = (LogicalAggregationOperator) expression.getOp();
                Map<ColumnRefOperator, ScalarOperator> keyMap = agg.getGroupingKeys().stream().collect(Collectors.toMap(
                        java.util.function.Function.identity(),
                        java.util.function.Function.identity()));
                columnRefMap.putAll(keyMap);
                columnRefMap.putAll(agg.getAggregations());
            } else {
                ColumnRefSet refSet = expression.getOutputColumns();
                for (int columnId : refSet.getColumnIds()) {
                    ColumnRefOperator columnRef = refFactory.getColumnRef(columnId);
                    columnRefMap.put(columnRef, columnRef);
                }
            }
        }

        return columnRefMap;
    }

    public static Map<ColumnRefOperator, ScalarOperator> getColumnRefMap(
            OptExpression expression, ColumnRefFactory refFactory) {
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = Maps.newHashMap();
        if (expression.getOp().getProjection() != null) {
            columnRefMap.putAll(expression.getOp().getProjection().getColumnRefMap());
        } else {
            if (expression.getOp() instanceof LogicalAggregationOperator) {
                LogicalAggregationOperator agg = (LogicalAggregationOperator) expression.getOp();
                Map<ColumnRefOperator, ScalarOperator> keyMap = agg.getGroupingKeys().stream().collect(Collectors.toMap(
                        java.util.function.Function.identity(),
                        java.util.function.Function.identity()));
                columnRefMap.putAll(keyMap);
                columnRefMap.putAll(agg.getAggregations());
            } else {
                ColumnRefSet refSet = expression.getOutputColumns();
                for (int columnId : refSet.getColumnIds()) {
                    ColumnRefOperator columnRef = refFactory.getColumnRef(columnId);
                    columnRefMap.put(columnRef, columnRef);
                }
            }
        }
        return columnRefMap;
    }
}
