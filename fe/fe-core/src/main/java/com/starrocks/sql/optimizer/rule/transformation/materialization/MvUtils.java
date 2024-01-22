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


package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvPlanContextBuilder;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerConfig;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalViewScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.optimizer.transformer.TransformerContext;
import com.starrocks.sql.parser.ParsingException;
import org.apache.commons.collections4.SetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;

public class MvUtils {
    private static final Logger LOG = LogManager.getLogger(MvUtils.class);

    public static Set<MaterializedView> getRelatedMvs(ConnectContext connectContext,
                                                      int maxLevel,
                                                      Set<Table> tablesToCheck) {
        if (tablesToCheck.isEmpty()) {
            return Sets.newHashSet();
        }
        Set<MaterializedView> mvs = Sets.newHashSet();
        getRelatedMvs(connectContext, maxLevel, 0, tablesToCheck, mvs);
        return mvs;
    }

    public static void getRelatedMvs(ConnectContext connectContext,
                                     int maxLevel, int currentLevel,
                                     Set<Table> tablesToCheck, Set<MaterializedView> mvs) {
        if (currentLevel >= maxLevel) {
            logMVPrepare(connectContext, "Current level {} is greater than max level {}", currentLevel, maxLevel);
            return;
        }
        Set<MvId> newMvIds = Sets.newHashSet();
        for (Table table : tablesToCheck) {
            Set<MvId> mvIds = table.getRelatedMaterializedViews();
            if (mvIds != null && !mvIds.isEmpty()) {
                logMVPrepare(connectContext, "Table/MaterializedView {} has related materialized views: {}",
                        table.getName(), mvIds);
                newMvIds.addAll(mvIds);
            } else {
                logMVPrepare(connectContext, "Table/MaterializedView {} has no related materialized views, " +
                                "identifier:{}", table.getName(), table.getTableIdentifier());
            }
        }
        if (newMvIds.isEmpty()) {
            return;
        }
        Set<Table> newMvs = Sets.newHashSet();
        for (MvId mvId : newMvIds) {
            Database db = GlobalStateMgr.getCurrentState().getDb(mvId.getDbId());
            if (db == null) {
                logMVPrepare(connectContext, "Cannot find database from mvId:{}", mvId);
                continue;
            }
            Table table = db.getTable(mvId.getId());
            if (table == null) {
                logMVPrepare(connectContext, "Cannot find materialized view from mvId:{}", mvId);
                continue;
            }
            newMvs.add(table);
            mvs.add((MaterializedView) table);
        }
        getRelatedMvs(connectContext, maxLevel, currentLevel + 1, newMvs, mvs);
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

    public static List<MaterializedView> collectMaterializedViews(OptExpression optExpression) {
        List<MaterializedView> mvs = new ArrayList<>();
        collectMaterializedViews(optExpression, mvs);
        return mvs;
    }

    private static void collectMaterializedViews(OptExpression optExpression, List<MaterializedView> mvs) {
        if (optExpression == null) {
            return;
        }
        Operator op = optExpression.getOp();
        if (op instanceof PhysicalScanOperator) {
            PhysicalScanOperator scanOperator = (PhysicalScanOperator) op;
            if (scanOperator.getTable().isMaterializedView()) {
                mvs.add((MaterializedView) scanOperator.getTable());
            }
        }

        for (OptExpression child : optExpression.getInputs()) {
            collectMaterializedViews(child, mvs);
        }
    }

    // get all ref table scan descs within and below root
    // the operator tree must match the rule pattern we define and now we only support SPJG pattern tree rewrite.
    // so here LogicalScanOperator's children must be LogicalScanOperator or LogicalJoinOperator
    public static List<TableScanDesc> getTableScanDescs(OptExpression root) {
        TableScanContext scanContext = new TableScanContext();
        OptExpressionVisitor joinFinder = new OptExpressionVisitor<Void, TableScanContext>() {
            @Override
            public Void visit(OptExpression optExpression, TableScanContext context) {
                for (OptExpression child : optExpression.getInputs()) {
                    child.getOp().accept(this, child, context);
                }
                return null;
            }

            @Override
            public Void visitLogicalTableScan(OptExpression optExpression, TableScanContext context) {
                LogicalScanOperator scanOperator = (LogicalScanOperator) optExpression.getOp();
                Table table = scanOperator.getTable();
                Integer id = scanContext.getTableIdMap().computeIfAbsent(table, t -> 0);
                TableScanDesc tableScanDesc = new TableScanDesc(table, id, scanOperator, null, false);
                context.getTableScanDescs().add(tableScanDesc);
                scanContext.getTableIdMap().put(table, ++id);
                return null;
            }

            @Override
            public Void visitLogicalJoin(OptExpression optExpression, TableScanContext context) {
                for (int i = 0; i < optExpression.getInputs().size(); i++) {
                    OptExpression child = optExpression.inputAt(i);
                    if (child.getOp() instanceof LogicalScanOperator) {
                        LogicalScanOperator scanOperator = (LogicalScanOperator) child.getOp();
                        Table table = scanOperator.getTable();
                        Integer id = scanContext.getTableIdMap().computeIfAbsent(table, t -> 0);
                        TableScanDesc tableScanDesc = new TableScanDesc(
                                table, id, scanOperator, optExpression, i == 0);
                        context.getTableScanDescs().add(tableScanDesc);
                        scanContext.getTableIdMap().put(table, ++id);
                    } else {
                        child.getOp().accept(this, child, context);
                    }
                }
                return null;
            }
        };

        root.getOp().<Void, TableScanContext>accept(joinFinder, root, scanContext);
        return scanContext.getTableScanDescs();
    }

    public static List<JoinOperator> getAllJoinOperators(OptExpression root) {
        List<JoinOperator> joinOperators = Lists.newArrayList();
        getAllJoinOperators(root, joinOperators);
        return joinOperators;
    }

    private static void getAllJoinOperators(OptExpression root, List<JoinOperator> joinOperators) {
        if (root.getOp() instanceof LogicalJoinOperator) {
            LogicalJoinOperator join = (LogicalJoinOperator) root.getOp();
            joinOperators.add(join.getJoinType());
        }
        for (OptExpression child : root.getInputs()) {
            getAllJoinOperators(child, joinOperators);
        }
    }

    public static List<LogicalScanOperator> getScanOperator(OptExpression root) {
        List<LogicalScanOperator> scanOperators = Lists.newArrayList();
        getScanOperator(root, scanOperators);
        return scanOperators;
    }

    public static void getScanOperator(OptExpression root, List<LogicalScanOperator> scanOperators) {
        if (root.getOp() instanceof LogicalScanOperator) {
            scanOperators.add((LogicalScanOperator) root.getOp());
        } else {
            for (OptExpression child : root.getInputs()) {
                getScanOperator(child, scanOperators);
            }
        }
    }

    public static List<LogicalOlapScanOperator> getOlapScanNode(OptExpression root) {
        List<LogicalOlapScanOperator> olapScanOperators = Lists.newArrayList();
        getOlapScanNode(root, olapScanOperators);
        return olapScanOperators;
    }

    public static void getOlapScanNode(OptExpression root, List<LogicalOlapScanOperator> olapScanOperators) {
        if (root.getOp() instanceof LogicalOlapScanOperator) {
            olapScanOperators.add((LogicalOlapScanOperator) root.getOp());
        } else {
            for (OptExpression child : root.getInputs()) {
                getOlapScanNode(child, olapScanOperators);
            }
        }
    }

    public static boolean isValidMVPlan(OptExpression root) {
        if (root == null) {
            return false;
        }
        // 1. check whether is SPJ first
        if (isLogicalSPJ(root)) {
            return true;
        }
        // 2. check whether it's SPJG then
        return isLogicalSPJG(root);
    }

    public static String getInvalidReason(OptExpression expr) {
        List<Operator> operators = collectOperators(expr);
        if (operators.stream().anyMatch(op -> !isLogicalSPJGOperator(op))) {
            String nonSPJGOperators =
                    operators.stream().filter(x -> !isLogicalSPJGOperator(x))
                            .map(Operator::toString)
                            .collect(Collectors.joining(","));
            return "MV contains non-SPJG operators: " + nonSPJGOperators;
        }
        return "MV is not SPJG structure";
    }

    private static List<Operator> collectOperators(OptExpression expr) {
        List<Operator> operators = Lists.newArrayList();
        collectOperators(expr, operators);
        return operators;
    }

    private static void collectOperators(OptExpression expr, List<Operator> result) {
        result.add(expr.getOp());
        for (OptExpression child : expr.getInputs()) {
            collectOperators(child, result);
        }
    }

    public static boolean isLogicalSPJG(OptExpression root) {
        return isLogicalSPJG(root, 0);
    }

    /**
     * Whether `root` and its children are all Select/Project/Join/Group ops,
     * NOTE: This method requires `root` must be Aggregate op to check whether MV is satisfiable quickly.
     */
    public static boolean isLogicalSPJG(OptExpression root, int level) {
        if (root == null) {
            return false;
        }
        Operator operator = root.getOp();
        if (!(operator instanceof LogicalAggregationOperator)) {
            if (level == 0) {
                return false;
            } else {
                return isLogicalSPJ(root);
            }
        }
        LogicalAggregationOperator agg = (LogicalAggregationOperator) operator;
        if (agg.getType() != AggType.GLOBAL) {
            return false;
        }

        OptExpression child = root.inputAt(0);
        return isLogicalSPJG(child, level + 1);
    }

    /**
     * Whether `root` and its children are Select/Project/Join ops.
     */
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

    public static boolean isLogicalSPJGOperator(Operator operator) {
        return (operator instanceof LogicalScanOperator)
                || (operator instanceof LogicalProjectOperator)
                || (operator instanceof LogicalFilterOperator)
                || (operator instanceof LogicalJoinOperator)
                || (operator instanceof LogicalAggregationOperator);
    }

    public static Pair<OptExpression, LogicalPlan> getRuleOptimizedLogicalPlan(
            MaterializedView mv,
            String sql,
            ColumnRefFactory columnRefFactory,
            ConnectContext connectContext,
            OptimizerConfig optimizerConfig,
            boolean inlineView) {
        StatementBase mvStmt;
        try {
            List<StatementBase> statementBases =
                    com.starrocks.sql.parser.SqlParser.parse(sql, connectContext.getSessionVariable());
            Preconditions.checkState(statementBases.size() == 1);
            mvStmt = statementBases.get(0);
        } catch (ParsingException parsingException) {
            LOG.warn("parse mv{}'s sql:{} failed", mv.getName(), sql, parsingException);
            return null;
        }
        Preconditions.checkState(mvStmt instanceof QueryStatement);
        Analyzer.analyze(mvStmt, connectContext);
        QueryRelation query = ((QueryStatement) mvStmt).getQueryRelation();
        TransformerContext transformerContext =
                new TransformerContext(columnRefFactory, connectContext, inlineView);
        LogicalPlan logicalPlan = new RelationTransformer(transformerContext).transform(query);
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

    public static List<OptExpression> collectJoinExpr(OptExpression expression) {
        List<OptExpression> joinExprs = Lists.newArrayList();
        OptExpressionVisitor joinCollector = new OptExpressionVisitor<Void, Void>() {
            @Override
            public Void visit(OptExpression optExpression, Void context) {
                for (OptExpression input : optExpression.getInputs()) {
                    input.getOp().accept(this, input, null);
                }
                return null;
            }

            @Override
            public Void visitLogicalJoin(OptExpression optExpression, Void context) {
                joinExprs.add(optExpression);
                for (OptExpression input : optExpression.getInputs()) {
                    input.getOp().accept(this, input, null);
                }
                return null;
            }
        };
        expression.getOp().accept(joinCollector, expression, null);
        return joinExprs;
    }

    // get all predicates within and below root
    public static Set<ScalarOperator> getAllValidPredicates(OptExpression root) {
        Set<ScalarOperator> predicates = Sets.newHashSet();
        getAllValidPredicates(root, predicates);
        return predicates;
    }

    public static Set<ScalarOperator> getAllValidPredicates(ScalarOperator conjunct) {
        if (conjunct == null) {
            return Sets.newHashSet();
        }
        return Utils.extractConjuncts(conjunct).stream().filter(MvUtils::isValidPredicate)
                .collect(Collectors.toSet());
    }

    public static List<ColumnRefOperator> getPredicateColumns(OptExpression root) {
        List<ColumnRefOperator> res = Lists.newArrayList();
        Set<ScalarOperator> predicates = getAllValidPredicates(root);
        SetUtils.emptyIfNull(predicates).forEach(x -> x.getColumnRefs(res));
        return res;
    }

    // If join is not cross/inner join, MV Rewrite must rewrite, otherwise may cause bad results.
    // eg.
    // mv: select * from customer left outer join orders on c_custkey = o_custkey;
    //
    //query（cannot rewrite）:
    //select count(1) from customer left outer join orders
    //  on c_custkey = o_custkey and o_comment not like '%special%requests%';
    //
    //query（can rewrite）:
    //select count(1)
    //          from customer left outer join orders on c_custkey = o_custkey
    //          where o_comment not like '%special%requests%';
    public static Set<ScalarOperator> getJoinOnPredicates(OptExpression root) {
        Set<ScalarOperator> predicates = Sets.newHashSet();
        getJoinOnPredicates(root, predicates);
        return predicates;
    }

    private static void getJoinOnPredicates(OptExpression root, Set<ScalarOperator> predicates) {
        Operator operator = root.getOp();

        if (operator instanceof LogicalJoinOperator) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
            JoinOperator joinOperatorType = joinOperator.getJoinType();
            // Collect all join on predicates which join type are not inner/cross join.
            if ((joinOperatorType != JoinOperator.INNER_JOIN
                    && joinOperatorType != JoinOperator.CROSS_JOIN) && joinOperator.getOnPredicate() != null) {
                // Now join's on-predicates may be pushed down below join, so use original on-predicates
                // instead of new on-predicates.
                List<ScalarOperator> conjuncts = Utils.extractConjuncts(joinOperator.getOriginalOnPredicate());
                collectValidPredicates(conjuncts, predicates);
            }
        }
        for (OptExpression child : root.getInputs()) {
            getJoinOnPredicates(child, predicates);
        }
    }

    public static ReplaceColumnRefRewriter getReplaceColumnRefWriter(OptExpression root,
                                                                     ColumnRefFactory columnRefFactory) {
        Map<ColumnRefOperator, ScalarOperator> mvLineage = LineageFactory.getLineage(root, columnRefFactory);
        return new ReplaceColumnRefRewriter(mvLineage, true);
    }

    private static void collectPredicates(List<ScalarOperator> conjuncts,
                                          Function<ScalarOperator, Boolean> supplier,
                                          Set<ScalarOperator> predicates) {
        conjuncts.stream().filter(p -> supplier.apply(p)).forEach(predicates::add);
    }

    // push-down predicates are excluded when calculating compensate predicates,
    // because they are derived from equivalence class, the original predicates have be considered
    private static void collectValidPredicates(List<ScalarOperator> conjuncts,
                                               Set<ScalarOperator> predicates) {
        collectPredicates(conjuncts, MvUtils::isValidPredicate, predicates);
    }

    public static boolean isValidPredicate(ScalarOperator predicate) {
        return !isRedundantPredicate(predicate);
    }

    public static boolean isRedundantPredicate(ScalarOperator predicate) {
        return predicate.isPushdown() || predicate.isRedundant();
    }

    // for A inner join B A.a = B.b;
    // A.a is not null and B.b is not null can be deduced from join.
    // This function only extracts predicates from inner/semi join and scan node,
    // which scan node will exclude IsNullPredicateOperator predicates if it's not null
    // and its column ref is in the join's keys
    public static Set<ScalarOperator> getPredicateForRewrite(OptExpression root) {
        Set<ScalarOperator> result = Sets.newHashSet();
        OptExpressionVisitor predicateVisitor = new OptExpressionVisitor<Object, ColumnRefSet>() {
            @Override
            public Object visit(OptExpression optExpression, ColumnRefSet context) {
                for (OptExpression child : optExpression.getInputs()) {
                    child.getOp().accept(this, child, null);
                }
                return null;
            }

            public Object visitLogicalTableScan(OptExpression optExpression, ColumnRefSet context) {
                List<ScalarOperator> conjuncts = Utils.extractConjuncts(optExpression.getOp().getPredicate());
                for (ScalarOperator conjunct : conjuncts) {
                    if (!isValidPredicate(conjunct)) {
                        continue;
                    }
                    if (conjunct instanceof IsNullPredicateOperator) {
                        IsNullPredicateOperator isNullPredicateOperator = conjunct.cast();
                        if (isNullPredicateOperator.isNotNull() && context != null
                                && context.containsAll(isNullPredicateOperator.getUsedColumns())) {
                            // if column ref is join key and column ref is not null can be ignored for inner and semi join
                            continue;
                        }
                    }
                    result.add(conjunct);
                }
                return null;
            }

            public Object visitLogicalJoin(OptExpression optExpression, ColumnRefSet context) {
                LogicalJoinOperator joinOperator = optExpression.getOp().cast();

                ColumnRefSet joinKeyColumns = new ColumnRefSet();
                JoinOperator joinType = joinOperator.getJoinType();
                if (joinType.isInnerJoin() || joinType.isCrossJoin() || joinType.isSemiJoin()) {
                    List<ScalarOperator> onConjuncts = Utils.extractConjuncts(joinOperator.getOnPredicate());
                    ColumnRefSet leftChildColumns = optExpression.inputAt(0).getOutputColumns();
                    ColumnRefSet rightChildColumns = optExpression.inputAt(1).getOutputColumns();
                    List<BinaryPredicateOperator> eqOnPredicates = JoinHelper.getEqualsPredicate(
                            leftChildColumns, rightChildColumns, onConjuncts);
                    eqOnPredicates.forEach(predicate -> joinKeyColumns.union(predicate.getUsedColumns()));
                    if (context != null) {
                        joinKeyColumns.union(context);
                    }
                    optExpression.inputAt(0).getOp().accept(this, optExpression.inputAt(0), joinKeyColumns);
                    optExpression.inputAt(1).getOp().accept(this, optExpression.inputAt(1), joinKeyColumns);
                } else if (joinType.isLeftOuterJoin() || joinType.isLeftAntiJoin()) {
                    optExpression.inputAt(0).getOp().accept(this, optExpression.inputAt(0), joinKeyColumns);
                } else if (joinType.isRightOuterJoin() || joinType.isRightAntiJoin()) {
                    optExpression.inputAt(1).getOp().accept(this, optExpression.inputAt(1), joinKeyColumns);
                }
                List<ScalarOperator> conjuncts = Utils.extractConjuncts(
                        Utils.compoundAnd(joinOperator.getPredicate(), joinOperator.getOnPredicate()));
                for (ScalarOperator conjunct : conjuncts) {
                    if (!isValidPredicate(conjunct)) {
                        continue;
                    }
                    result.add(conjunct);
                }
                return null;
            }
        };
        root.getOp().accept(predicateVisitor, root, null);
        return result;
    }

    /**
     * Get all predicates by filtering according the input `supplier`.
     */
    private static void getAllPredicates(OptExpression root,
                                         Function<ScalarOperator, Boolean> supplier,
                                         Set<ScalarOperator> predicates) {
        Operator operator = root.getOp();

        // Ignore aggregation predicates, because aggregation predicates should be rewritten after
        // aggregation functions' rewrite and should not be pushed down into mv scan operator.
        if (operator.getPredicate() != null && !(operator instanceof LogicalAggregationOperator)) {
            List<ScalarOperator> conjuncts = Utils.extractConjuncts(operator.getPredicate());
            collectPredicates(conjuncts, supplier, predicates);
        }
        if (operator instanceof LogicalJoinOperator) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
            if (joinOperator.getOnPredicate() != null) {
                List<ScalarOperator> conjuncts = Utils.extractConjuncts(joinOperator.getOnPredicate());
                collectPredicates(conjuncts, supplier, predicates);
            }
        }
        for (OptExpression child : root.getInputs()) {
            getAllPredicates(child, supplier, predicates);
        }
    }

    /**
     * Get all valid predicates from input opt expression. `valid` predicate means this predicate is not
     * a pushed-down predicate or redundant predicate among others.
     */
    private static void getAllValidPredicates(OptExpression root, Set<ScalarOperator> predicates) {
        getAllPredicates(root, MvUtils::isValidPredicate, predicates);
    }

    /**
     * Canonize the predicate into a normalized form to deduce the redundant predicates.
     */
    public static ScalarOperator canonizePredicate(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        // do not change original predicate, clone it here
        ScalarOperator cloned = predicate.clone();
        ScalarOperatorRewriter rewrite = new ScalarOperatorRewriter();
        return rewrite.rewrite(cloned, ScalarOperatorRewriter.DEFAULT_REWRITE_SCAN_PREDICATE_RULES);
    }

    /**
     * Canonize the predicate into a more normalized form to be compared better.
     * NOTE:
     * 1. `canonizePredicateForRewrite` will do more optimizations than `canonizePredicate`.
     * 2. if you need to rewrite src predicate to target predicate, should use `canonizePredicateForRewrite`
     *  both rather than one use `canonizePredicate` or `canonizePredicateForRewrite`.
     */
    public static ScalarOperator canonizePredicateForRewrite(QueryMaterializationContext queryMaterializationContext,
                                                             ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        return queryMaterializationContext == null ?
                new ScalarOperatorRewriter().rewrite(predicate.clone(), ScalarOperatorRewriter.MV_SCALAR_REWRITE_RULES)
                : queryMaterializationContext.getCanonizedPredicate(predicate);
    }

    public static Collection<ScalarOperator> canonizePredicatesForRewrite(
            QueryMaterializationContext queryMaterializationContext,
            Collection<ScalarOperator> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return predicates;
        }
        return predicates.stream()
                .map(x -> canonizePredicateForRewrite(queryMaterializationContext, x))
                .collect(Collectors.toSet());
    }

    public static ScalarOperator getCompensationPredicateForDisjunctive(ScalarOperator src, ScalarOperator target) {
        List<ScalarOperator> srcItems = Utils.extractDisjunctive(src);
        List<ScalarOperator> targetItems = Utils.extractDisjunctive(target);
        if (!Sets.newHashSet(targetItems).containsAll(srcItems)) {
            return null;
        }
        targetItems.removeAll(srcItems);
        if (targetItems.isEmpty()) {
            // it is the same, so return true constant
            return ConstantOperator.createBoolean(true);
        } else {
            // the target has more or item, so return src
            return src;
        }
    }

    public static boolean isAllEqualInnerOrCrossJoin(OptExpression root) {
        Operator operator = root.getOp();
        if (!(operator instanceof LogicalOperator)) {
            return false;
        }

        if (operator instanceof LogicalJoinOperator) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
            if (!isEqualInnerOrCrossJoin(joinOperator)) {
                return false;
            }
        }
        for (OptExpression child : root.getInputs()) {
            if (!isAllEqualInnerOrCrossJoin(child)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isEqualInnerOrCrossJoin(LogicalJoinOperator joinOperator) {
        if (joinOperator.getJoinType() == JoinOperator.CROSS_JOIN && joinOperator.getOnPredicate() == null) {
            return true;
        }

        return joinOperator.getJoinType() == JoinOperator.INNER_JOIN &&
                isColumnEqualPredicate(joinOperator.getOnPredicate());
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

    public static List<ColumnRefOperator> collectScanColumn(OptExpression optExpression) {
        return collectScanColumn(optExpression, Predicates.alwaysTrue());
    }

    public static List<ColumnRefOperator> collectScanColumn(OptExpression optExpression,
                                                            Predicate<LogicalScanOperator> predicate) {

        List<ColumnRefOperator> columnRefOperators = Lists.newArrayList();
        OptExpressionVisitor visitor = new OptExpressionVisitor<Void, Void>() {
            @Override
            public Void visit(OptExpression optExpression, Void context) {
                for (OptExpression input : optExpression.getInputs()) {
                    input.getOp().accept(this, input, null);
                }
                return null;
            }

            @Override
            public Void visitLogicalTableScan(OptExpression optExpression, Void context) {
                LogicalScanOperator scan = (LogicalScanOperator) optExpression.getOp();
                if (predicate.test(scan)) {
                    columnRefOperators.addAll(scan.getColRefToColumnMetaMap().keySet());
                }
                return null;
            }
        };
        optExpression.getOp().accept(visitor, optExpression, null);
        return columnRefOperators;
    }

    public static ScalarOperator convertListPartitionKeys(ScalarOperator partitionColRef,
                                                          List<Range<PartitionKey>> partitionRanges) {
        List<ScalarOperator> inArgs = Lists.newArrayList();
        inArgs.add(partitionColRef);
        for (Range<PartitionKey> range : partitionRanges) {
            if (range.isEmpty()) {
                continue;
            }

            // see `convertToDateRange`
            if (range.hasLowerBound() && range.hasUpperBound()) {
                // partition range must have lower bound and upper bound
                ConstantOperator lowerBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.lowerEndpoint().getKeys().get(0));
                ConstantOperator upperBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.upperEndpoint().getKeys().get(0));
                Preconditions.checkState(lowerBound.getType().isStringType());
                Preconditions.checkState(upperBound.getType().isStringType());
                inArgs.add(lowerBound);
            } else if (range.hasUpperBound()) {
                ConstantOperator upperBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.upperEndpoint().getKeys().get(0));
                Preconditions.checkState(upperBound.getType().isStringType());
                inArgs.add(upperBound);
            } else if (range.hasLowerBound()) {
                ConstantOperator lowerBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.lowerEndpoint().getKeys().get(0));
                Preconditions.checkState(lowerBound.getType().isStringType());
                inArgs.add(lowerBound);
            } else {
                // continue
            }
        }
        if (inArgs.size() == 1) {
            return ConstantOperator.TRUE;
        } else {
            return new InPredicateOperator(false, inArgs);
        }
    }

    public static ScalarOperator convertPartitionKeysToPredicate(ScalarOperator partitionColumn,
                                                                 List<Range<PartitionKey>> partitionKeys) {
        boolean isListPartition = partitionColumn.getType().isStringType();
        // NOTE: For string type partition column, it should be list partition rather than range partition.
        if (isListPartition) {
            return convertListPartitionKeys(partitionColumn, partitionKeys);
        } else {
            List<ScalarOperator> rangePredicates = MvUtils.convertRanges(partitionColumn, partitionKeys);
            return Utils.compoundOr(rangePredicates);
        }
    }

    public static List<ScalarOperator> convertRanges(ScalarOperator partitionScalar,
                                                     List<Range<PartitionKey>> partitionRanges) {
        List<ScalarOperator> rangeParts = Lists.newArrayList();
        for (Range<PartitionKey> range : partitionRanges) {
            if (range.isEmpty()) {
                continue;
            }
            // partition range must have lower bound and upper bound
            Preconditions.checkState(range.hasLowerBound() && range.hasUpperBound());
            LiteralExpr lowerExpr = range.lowerEndpoint().getKeys().get(0);
            if (lowerExpr.isMinValue() && range.upperEndpoint().isMaxValue()) {
                continue;
            } else if (lowerExpr.isMinValue()) {
                ConstantOperator upperBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.upperEndpoint().getKeys().get(0));
                BinaryPredicateOperator upperPredicate = new BinaryPredicateOperator(
                        BinaryType.LT, partitionScalar, upperBound);
                rangeParts.add(upperPredicate);
            } else if (range.upperEndpoint().isMaxValue()) {
                ConstantOperator lowerBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.lowerEndpoint().getKeys().get(0));
                BinaryPredicateOperator lowerPredicate = new BinaryPredicateOperator(
                        BinaryType.GE, partitionScalar, lowerBound);
                rangeParts.add(lowerPredicate);
            } else {
                // close, open range
                ConstantOperator lowerBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.lowerEndpoint().getKeys().get(0));
                ConstantOperator upperBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.upperEndpoint().getKeys().get(0));
                BinaryPredicateOperator lowerPredicate = new BinaryPredicateOperator(
                        BinaryType.GE, partitionScalar, lowerBound);
                BinaryPredicateOperator upperPredicate = new BinaryPredicateOperator(
                        BinaryType.LT, partitionScalar, upperBound);
                CompoundPredicateOperator andPredicate = new CompoundPredicateOperator(
                        CompoundPredicateOperator.CompoundType.AND, lowerPredicate, upperPredicate);
                rangeParts.add(andPredicate);
            }
        }
        return rangeParts;
    }

    public static List<Expr> convertRange(Expr slotRef, List<Range<PartitionKey>> partitionRanges) {
        List<Expr> rangeParts = Lists.newArrayList();
        for (Range<PartitionKey> range : partitionRanges) {
            if (range.isEmpty()) {
                continue;
            }
            // partition range must have lower bound and upper bound
            Preconditions.checkState(range.hasLowerBound() && range.hasUpperBound());
            LiteralExpr lowerExpr = range.lowerEndpoint().getKeys().get(0);
            if (lowerExpr.isMinValue() && range.upperEndpoint().isMaxValue()) {
                continue;
            } else if (lowerExpr.isMinValue()) {
                Expr upperBound = range.upperEndpoint().getKeys().get(0);
                BinaryPredicate upperPredicate = new BinaryPredicate(BinaryType.LT, slotRef, upperBound);
                rangeParts.add(upperPredicate);
            } else if (range.upperEndpoint().isMaxValue()) {
                Expr lowerBound = range.lowerEndpoint().getKeys().get(0);
                BinaryPredicate lowerPredicate = new BinaryPredicate(BinaryType.GE, slotRef, lowerBound);
                rangeParts.add(lowerPredicate);
            } else {
                // close, open range
                Expr lowerBound = range.lowerEndpoint().getKeys().get(0);
                BinaryPredicate lowerPredicate = new BinaryPredicate(BinaryType.GE, slotRef, lowerBound);

                Expr upperBound = range.upperEndpoint().getKeys().get(0);
                BinaryPredicate upperPredicate = new BinaryPredicate(BinaryType.LT, slotRef, upperBound);

                CompoundPredicate andPredicate = new CompoundPredicate(CompoundPredicate.Operator.AND, lowerPredicate,
                        upperPredicate);
                rangeParts.add(andPredicate);
            }
        }
        return rangeParts;
    }

    public static List<Expr> convertList(Expr slotRef, List<LiteralExpr> values) {
        List<Expr> listPart = Lists.newArrayList();
        for (LiteralExpr value : values) {
            BinaryPredicate predicate = new BinaryPredicate(BinaryType.EQ, slotRef, value);
            listPart.add(predicate);
        }
        return listPart;
    }

    public static List<Range<PartitionKey>> mergeRanges(List<Range<PartitionKey>> ranges) {
        ranges.sort(RangeUtils.RANGE_COMPARATOR);
        List<Range<PartitionKey>> mergedRanges = Lists.newArrayList();
        for (Range<PartitionKey> currentRange : ranges) {
            boolean merged = false;
            for (int j = 0; j < mergedRanges.size(); j++) {
                // 1 < r < 10, 10 <= r < 20 => 1 < r < 20
                Range<PartitionKey> mergedRange = mergedRanges.get(j);
                if (currentRange.isConnected(mergedRange)) {
                    // for partition range, the intersection must be empty
                    Preconditions.checkState(currentRange.intersection(mergedRange).isEmpty());
                    mergedRanges.set(j, mergedRange.span(currentRange));
                    merged = true;
                    break;
                }
            }
            if (!merged) {
                mergedRanges.add(currentRange);
            }
        }
        return mergedRanges;
    }

    private static boolean supportCompensatePartitionPredicateForHiveScan(List<PartitionKey> partitionKeys) {
        for (PartitionKey partitionKey : partitionKeys) {
            // only support one partition column now.
            if (partitionKey.getKeys().size() != 1) {
                return false;
            }
            LiteralExpr e = partitionKey.getKeys().get(0);
            // Only support date/int type
            if (!(e instanceof DateLiteral || e instanceof IntLiteral)) {
                return false;
            }
        }
        return true;
    }

    public static List<ScalarOperator> compensatePartitionPredicateForHiveScan(LogicalHiveScanOperator scanOperator) {
        ScanOperatorPredicates scanOperatorPredicates = scanOperator.getScanOperatorPredicates();
        List<ScalarOperator> partitionPredicates = Lists.newArrayList();

        Preconditions.checkState(scanOperator.getTable().isHiveTable());
        HiveTable hiveTable = (HiveTable) scanOperator.getTable();
        if (hiveTable.isUnPartitioned()) {
            return partitionPredicates;
        }
        if (scanOperatorPredicates.getSelectedPartitionIds().size() ==
                scanOperatorPredicates.getIdToPartitionKey().size()) {
            return partitionPredicates;
        }
        if (!supportCompensatePartitionPredicateForHiveScan(scanOperatorPredicates.getSelectedPartitionKeys())) {
            return partitionPredicates;
        }
        List<Range<PartitionKey>> ranges = Lists.newArrayList();
        for (PartitionKey selectedPartitionKey : scanOperatorPredicates.getSelectedPartitionKeys()) {
            try {
                LiteralExpr expr = PartitionUtil.addOffsetForLiteral(selectedPartitionKey.getKeys().get(0), 1,
                        PartitionUtil.getDateTimeInterval(hiveTable, hiveTable.getPartitionColumns().get(0)));
                PartitionKey partitionKey = new PartitionKey(ImmutableList.of(expr), selectedPartitionKey.getTypes());
                ranges.add(Range.closedOpen(selectedPartitionKey, partitionKey));
            } catch (AnalysisException e) {
                LOG.warn("Compute partition key range failed. ", e);
                return partitionPredicates;
            }
        }

        List<Range<PartitionKey>> mergedRanges = mergeRanges(ranges);
        ColumnRefOperator partitionColumnRef = scanOperator.getColumnReference(hiveTable.getPartitionColumns().get(0));
        ScalarOperator partitionPredicate = convertPartitionKeysToPredicate(partitionColumnRef, mergedRanges);
        if (partitionPredicate != null) {
            partitionPredicates.add(partitionPredicate);
        }

        return partitionPredicates;
    }

    public static final ImmutableList<OperatorType> SUPPORTED_PARTITION_COMPENSATE_SCAN_TYPES =
            ImmutableList.<OperatorType>builder()
            .add(OperatorType.LOGICAL_OLAP_SCAN)
            .add(OperatorType.LOGICAL_HIVE_SCAN)
            .build();
    /**
     * - if `isCompensate` is true, use `selectedPartitionIds` to compensate complete partition ranges
     *  with lower and upper bound.
     * - otherwise use original pruned partition predicates as the compensated partition
     *  predicates.
     * NOTE: When MV has enough partitions for the query, no need to compensate anymore for both mv and the query's plan.
     *       A query can be rewritten just by the original SQL.
     * NOTE: It's not safe if `isCompensate` is always false:
     *      - partitionPredicate is null if olap scan operator cannot prune partitions.
     *      - partitionPredicate is not exact even if olap scan operator has pruned partitions.
     * eg:
     *      t1:
     *       PARTITION p1 VALUES [("0000-01-01"), ("2020-01-01")), has data
     *       PARTITION p2 VALUES [("2020-01-01"), ("2020-02-01")), has data
     *       PARTITION p3 VALUES [("2020-02-01"), ("2020-03-01")), has data
     *       PARTITION p4 VALUES [("2020-03-01"), ("2020-04-01")), no data
     *       PARTITION p5 VALUES [("2020-04-01"), ("2020-05-01")), no data
     *
     *      query1 : SELECT k1, sum(v1) as sum_v1 FROM t1 group by k1;
     *      `partitionPredicate` : null
     *
     *      query2 : SELECT k1, sum(v1) as sum_v1 FROM t1 where k1>='2020-02-01' group by k1;
     *      `partitionPredicate` : k1>='2020-02-11'
     *      however for mv  we need: k1>='2020-02-11' and k1 < "2020-03-01"
     */
    public static ScalarOperator compensatePartitionPredicate(MaterializationContext mvContext,
                                                              ColumnRefFactory columnRefFactory,
                                                              OptExpression queryExpression) {
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryExpression);
        if (scanOperators.isEmpty()) {
            return ConstantOperator.createBoolean(true);
        }

        List<ScalarOperator> partitionPredicates = Lists.newArrayList();
        boolean isCompensatePartition = mvContext.getOrInitCompensatePartitionPredicate(queryExpression);
        // Compensate partition predicates and add them into query predicate.
        Map<Pair<LogicalScanOperator, Boolean>, List<ScalarOperator>> scanOperatorScalarOperatorMap =
                mvContext.getScanOpToPartitionCompensatePredicates();
        for (LogicalScanOperator scanOperator : scanOperators) {
            if (!SUPPORTED_PARTITION_COMPENSATE_SCAN_TYPES.contains(scanOperator.getOpType())) {
                continue;
            }
            List<ScalarOperator> partitionPredicate = scanOperatorScalarOperatorMap
                    .computeIfAbsent(Pair.create(scanOperator, isCompensatePartition), x -> {
                        return isCompensatePartition ? getCompensatePartitionPredicates(columnRefFactory, scanOperator) :
                                getScanOpPrunedPartitionPredicates(scanOperator);
                    });
            if (partitionPredicate == null) {
                return null;
            }
            partitionPredicates.addAll(partitionPredicate);
        }
        return partitionPredicates.isEmpty() ? ConstantOperator.createBoolean(true) :
                Utils.compoundAnd(partitionPredicates);
    }

    private static List<ScalarOperator> getCompensatePartitionPredicates(ColumnRefFactory columnRefFactory,
                                                                         LogicalScanOperator scanOperator) {
        List<ScalarOperator> partitionPredicate = null;
        if (scanOperator instanceof LogicalOlapScanOperator) {
            partitionPredicate = compensatePartitionPredicateForOlapScan((LogicalOlapScanOperator) scanOperator,
                    columnRefFactory);
        } else if (scanOperator instanceof LogicalHiveScanOperator) {
            partitionPredicate = compensatePartitionPredicateForHiveScan((LogicalHiveScanOperator) scanOperator);
        } else {
            return null;
        }
        return partitionPredicate;
    }

    /**
     * Determine whether to compensate extra partition predicates,
     * - if it needs compensate, use `selectedPartitionIds` to compensate complete partition ranges
     *  with lower and upper bound.
     * - if not compensate, use original pruned partition predicates as the compensated partition
     *  predicates.
     * @param plan : query opt expression
     * @param mvContext : materialized view context
     * @return
     */
    public static Optional<Boolean> isNeedCompensatePartitionPredicate(OptExpression plan,
                                                                       MaterializationContext mvContext) {
        Set<String> mvPartitionNameToRefresh = mvContext.getMvPartitionNamesToRefresh();
        // If mv contains no partitions to refresh, no need compensate
        if (Objects.isNull(mvPartitionNameToRefresh) || mvPartitionNameToRefresh.isEmpty()) {
            return Optional.of(false);
        }

        // If ref table contains no partitions to refresh, no need compensate.
        // If the mv is partitioned and non-ref table need refresh, then all partitions need to be refreshed,
        // it can not be a candidate.
        Set<String> refTablePartitionNameToRefresh = mvContext.getRefTableUpdatePartitionNames();
        if (Objects.isNull(refTablePartitionNameToRefresh) || refTablePartitionNameToRefresh.isEmpty()) {
            // NOTE: This should not happen: `mvPartitionNameToRefresh` is not empty, so `refTablePartitionNameToRefresh`
            // should not empty. Return true in the situation to avoid bad cases.
            return Optional.of(true);
        }

        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(plan);
        // If no scan operator, no need compensate
        if (scanOperators.isEmpty()) {
            return Optional.of(false);
        }
        if (scanOperators.stream().anyMatch(scan -> scan instanceof LogicalViewScanOperator)) {
            return Optional.of(true);
        }

        // If no partition table and columns, no need compensate
        MaterializedView mv = mvContext.getMv();
        Pair<Table, Column> partitionTableAndColumns = mv.getDirectTableAndPartitionColumn();
        if (partitionTableAndColumns == null) {
            return Optional.of(false);
        }
        Table refBaseTable = partitionTableAndColumns.first;
        Optional<LogicalScanOperator> optRefScanOperator =
                scanOperators.stream().filter(x -> isRefBaseTable(x, refBaseTable)).findFirst();
        if (!optRefScanOperator.isPresent()) {
            return Optional.empty();
        }

        LogicalScanOperator scanOperator = optRefScanOperator.get();
        if (scanOperator instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) scanOperator;
            OlapTable olapTable = (OlapTable) olapScanOperator.getTable();
            // If table's not partitioned, no need compensate
            if (olapTable.getPartitionInfo() instanceof SinglePartitionInfo) {
                return Optional.of(false);
            }

            List<Long> selectPartitionIds = olapScanOperator.getSelectedPartitionId();
            if (Objects.isNull(selectPartitionIds) || selectPartitionIds.size() == 0) {
                return Optional.of(false);
            }

            // determine whether query's partitions can be satisfied by materialized view.
            for (Long selectPartitionId : selectPartitionIds) {
                Partition partition = olapTable.getPartition(selectPartitionId);
                if (partition != null && refTablePartitionNameToRefresh.contains(partition.getName())) {
                    return Optional.of(true);
                }
            }
            return Optional.of(false);
        } else if (scanOperator instanceof LogicalHiveScanOperator) {
            HiveTable hiveTable = (HiveTable) scanOperator.getTable();
            // If table's not partitioned, no need compensate
            if (hiveTable.isUnPartitioned()) {
                return Optional.of(false);
            }
            LogicalHiveScanOperator hiveScanOperator = (LogicalHiveScanOperator)  scanOperator;
            ScanOperatorPredicates scanOperatorPredicates = hiveScanOperator.getScanOperatorPredicates();
            Collection<Long> selectPartitionIds = scanOperatorPredicates.getSelectedPartitionIds();
            if (Objects.isNull(selectPartitionIds) || selectPartitionIds.size() == 0) {
                return Optional.of(false);
            }
            // determine whether query's partitions can be satisfied by materialized view.
            List<PartitionKey> selectPartitionKeys = scanOperatorPredicates.getSelectedPartitionKeys();
            for (PartitionKey partitionKey : selectPartitionKeys) {
                String mvPartitionName = PartitionUtil.generateMVPartitionName(partitionKey);
                if (refTablePartitionNameToRefresh.contains(mvPartitionName)) {
                    return Optional.of(true);
                }
            }
            return Optional.of(false);
        } else {
            return Optional.of(true);
        }
    }

    /**
     * Compensate olap table's partition predicates from olap scan operator which may be pruned by optimizer before or not.
     *
     * @param olapScanOperator   : olap scan operator that needs to compensate partition predicates.
     * @param columnRefFactory   : column ref factory that used to generate new partition predicate epxr.
     * @return
     */
    private static List<ScalarOperator> compensatePartitionPredicateForOlapScan(LogicalOlapScanOperator olapScanOperator,
                                                                                ColumnRefFactory columnRefFactory) {
        List<ScalarOperator> partitionPredicates = Lists.newArrayList();
        Preconditions.checkState(olapScanOperator.getTable().isNativeTableOrMaterializedView());
        OlapTable olapTable = (OlapTable) olapScanOperator.getTable();

        // compensate nothing for single partition table
        if (olapTable.getPartitionInfo() instanceof SinglePartitionInfo) {
            return partitionPredicates;
        }

        // compensate nothing if selected partitions are the same with the total partitions.
        if (olapScanOperator.getSelectedPartitionId() != null
                && olapScanOperator.getSelectedPartitionId().size() == olapTable.getPartitions().size()) {
            return partitionPredicates;
        }

        // if no partitions are selected, return pruned partition predicates directly.
        if (olapScanOperator.getSelectedPartitionId().isEmpty()) {
            return olapScanOperator.getPrunedPartitionPredicates();
        }

        if (olapTable.getPartitionInfo() instanceof ExpressionRangePartitionInfo) {
            ExpressionRangePartitionInfo partitionInfo =
                    (ExpressionRangePartitionInfo) olapTable.getPartitionInfo();
            Expr partitionExpr = partitionInfo.getPartitionExprs().get(0);
            List<SlotRef> slotRefs = Lists.newArrayList();
            partitionExpr.collect(SlotRef.class, slotRefs);
            Preconditions.checkState(slotRefs.size() == 1);
            Optional<ColumnRefOperator> partitionColumn =
                    olapScanOperator.getColRefToColumnMetaMap().keySet().stream()
                            .filter(columnRefOperator -> columnRefOperator.getName()
                                    .equals(slotRefs.get(0).getColumnName()))
                            .findFirst();
            // compensate nothing if there is no partition column predicate in the scan node.
            if (!partitionColumn.isPresent()) {
                return null;
            }

            ExpressionMapping mapping =
                    new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()));
            mapping.put(slotRefs.get(0), partitionColumn.get());
            ScalarOperator partitionScalarOperator =
                    SqlToScalarOperatorTranslator.translate(partitionExpr, mapping, columnRefFactory);
            List<Range<PartitionKey>> selectedRanges = Lists.newArrayList();
            // compensate selected partition ranges from selected partition id
            for (long pid : olapScanOperator.getSelectedPartitionId()) {
                selectedRanges.add(partitionInfo.getRange(pid));
            }

            // normalize selected partition ranges
            List<Range<PartitionKey>> mergedRanges = MvUtils.mergeRanges(selectedRanges);
            ScalarOperator partitionPredicate =
                    convertPartitionKeysToPredicate(partitionScalarOperator, mergedRanges);
            partitionPredicates.add(partitionPredicate);
        } else if (olapTable.getPartitionInfo() instanceof RangePartitionInfo) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
            List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
            if (partitionColumns.size() != 1) {
                // now do not support more than one partition columns
                return null;
            }
            List<Range<PartitionKey>> selectedRanges = Lists.newArrayList();
            for (long pid : olapScanOperator.getSelectedPartitionId()) {
                selectedRanges.add(rangePartitionInfo.getRange(pid));
            }
            ColumnRefOperator partitionColumnRef = olapScanOperator.getColumnReference(partitionColumns.get(0));
            List<Range<PartitionKey>> mergedRanges = MvUtils.mergeRanges(selectedRanges);
            ScalarOperator partitionPredicate =
                    convertPartitionKeysToPredicate(partitionColumnRef, mergedRanges);
            if (partitionPredicate != null) {
                partitionPredicates.add(partitionPredicate);
            }
        } else {
            return null;
        }

        return partitionPredicates;
    }

    // try to get partial partition predicates of partitioned mv.
    // eg, mv1's base partition table is t1, partition column is k1 and has two partition:
    // p1:[2022-01-01, 2022-01-02), p1 is updated(refreshed),
    // p2:[2022-01-02, 2022-01-03), p2 is outdated,
    // then this function will return predicate:
    // k1 >= "2022-01-01" and k1 < "2022-01-02"
    // NOTE: This method can be only used in query rewrite and cannot be used in insert routine.
    public static ScalarOperator getMvPartialPartitionPredicates(
            MaterializedView mv,
            OptExpression mvPlan,
            Set<String> mvPartitionNamesToRefresh) throws AnalysisException {
        Pair<Table, Column> partitionTableAndColumns = mv.getDirectTableAndPartitionColumn();
        if (partitionTableAndColumns == null) {
            return null;
        }

        Table refBaseTable = partitionTableAndColumns.first;
        List<Range<PartitionKey>> latestBaseTableRanges =
                getLatestPartitionRangeForTable(refBaseTable, partitionTableAndColumns.second,
                        mv, mvPartitionNamesToRefresh);
        if (latestBaseTableRanges.isEmpty()) {
            // if there isn't an updated partition, do not rewrite
            return null;
        }

        Column partitionColumn = partitionTableAndColumns.second;
        List<OptExpression> scanExprs = MvUtils.collectScanExprs(mvPlan);
        Expr partitionExpr = mv.getFirstPartitionRefTableExpr();
        for (OptExpression scanExpr : scanExprs) {
            LogicalScanOperator scanOperator = (LogicalScanOperator) scanExpr.getOp();
            if (!isRefBaseTable(scanOperator, refBaseTable)) {
                continue;
            }
            final Optional<ColumnRefOperator> columnRefOption;
            if (scanOperator instanceof LogicalViewScanOperator) {
                LogicalViewScanOperator viewScanOperator = scanOperator.cast();
                columnRefOption = Optional.ofNullable(viewScanOperator.getExpressionMapping(partitionExpr));
            } else {
                columnRefOption = Optional.ofNullable(scanOperator.getColumnReference(partitionColumn));
            }
            if (!columnRefOption.isPresent()) {
                continue;
            }
            return convertPartitionKeysToPredicate(columnRefOption.get(), latestBaseTableRanges);
        }
        return null;
    }

    public static List<ScalarOperator> getMVPrunedPartitionPredicates(MaterializedView mv,
                                                                      OptExpression mvPlan) {
        Pair<Table, Column> partitionTableAndColumns = mv.getDirectTableAndPartitionColumn();
        if (partitionTableAndColumns == null) {
            return null;
        }

        Table refBaseTable = partitionTableAndColumns.first;
        List<OptExpression> scanExprs = MvUtils.collectScanExprs(mvPlan);
        for (OptExpression scanExpr : scanExprs) {
            LogicalScanOperator scanOperator = (LogicalScanOperator) scanExpr.getOp();
            if (!isRefBaseTable(scanOperator, refBaseTable)) {
                continue;
            }

            List<ScalarOperator> prunedPredicates = getScanOpPrunedPartitionPredicates(scanOperator);
            if (prunedPredicates == null || prunedPredicates.isEmpty()) {
                return List.of(ConstantOperator.TRUE);
            } else {
                return prunedPredicates;
            }
        }
        return null;
    }

    private static List<ScalarOperator> getScanOpPrunedPartitionPredicates(LogicalScanOperator scanOperator) {
        if (scanOperator == null) {
            return null;
        }

        if (scanOperator instanceof LogicalOlapScanOperator) {
            return ((LogicalOlapScanOperator) scanOperator).getPrunedPartitionPredicates();
        } else if (scanOperator instanceof LogicalHiveScanOperator) {
            ScanOperatorPredicates scanOperatorPredicates =
                    ((LogicalHiveScanOperator) scanOperator).getScanOperatorPredicates();
            return scanOperatorPredicates.getPrunedPartitionConjuncts();
        } else {
            // Cannot decide whether it has been pruned or not, return null for now.
            return null;
        }
    }

    private static boolean isRefBaseTable(LogicalScanOperator scanOperator, Table refBaseTable) {
        Table scanTable = scanOperator.getTable();
        if (scanTable.isNativeTableOrMaterializedView() && !scanTable.equals(refBaseTable)) {
            return false;
        }
        if (scanOperator instanceof LogicalViewScanOperator) {
            return true;
        }
        if (!scanTable.isNativeTableOrMaterializedView() && !scanTable.getTableIdentifier().equals(
                refBaseTable.getTableIdentifier())) {
            return false;
        }
        return true;
    }

    // convert varchar date to date type
    public static Range<PartitionKey> convertToDateRange(Range<PartitionKey> from) throws AnalysisException {
        if (from.hasLowerBound() && from.hasUpperBound()) {
            StringLiteral lowerString = (StringLiteral) from.lowerEndpoint().getKeys().get(0);
            LocalDateTime lowerDateTime = DateUtils.parseDatTimeString(lowerString.getStringValue());
            PartitionKey lowerPartitionKey = PartitionKey.ofDate(lowerDateTime.toLocalDate());

            StringLiteral upperString = (StringLiteral) from.upperEndpoint().getKeys().get(0);
            LocalDateTime upperDateTime = DateUtils.parseDatTimeString(upperString.getStringValue());
            PartitionKey upperPartitionKey = PartitionKey.ofDate(upperDateTime.toLocalDate());
            return Range.range(lowerPartitionKey, from.lowerBoundType(), upperPartitionKey, from.upperBoundType());
        } else if (from.hasUpperBound()) {
            StringLiteral upperString = (StringLiteral) from.upperEndpoint().getKeys().get(0);
            LocalDateTime upperDateTime = DateUtils.parseDatTimeString(upperString.getStringValue());
            PartitionKey upperPartitionKey = PartitionKey.ofDate(upperDateTime.toLocalDate());
            return Range.upTo(upperPartitionKey, from.upperBoundType());
        } else if (from.hasLowerBound()) {
            StringLiteral lowerString = (StringLiteral) from.lowerEndpoint().getKeys().get(0);
            LocalDateTime lowerDateTime = DateUtils.parseDatTimeString(lowerString.getStringValue());
            PartitionKey lowerPartitionKey = PartitionKey.ofDate(lowerDateTime.toLocalDate());
            return Range.downTo(lowerPartitionKey, from.lowerBoundType());
        }
        return Range.all();
    }

    public static boolean isDateRange(Range<PartitionKey> range) {
        if (range.hasUpperBound()) {
            PartitionKey partitionKey = range.upperEndpoint();
            return partitionKey.getKeys().get(0) instanceof DateLiteral;
        } else if (range.hasLowerBound()) {
            PartitionKey partitionKey = range.lowerEndpoint();
            return partitionKey.getKeys().get(0) instanceof DateLiteral;
        }
        return false;
    }

    // convert date to varchar type
    public static Range<PartitionKey> convertToVarcharRange(
            Range<PartitionKey> from, String dateFormat) throws AnalysisException {
        DateTimeFormatter formatter = DateUtils.unixDatetimeFormatter(dateFormat);
        if (from.hasLowerBound() && from.hasUpperBound()) {
            PartitionKey lowerPartitionKey = convertToVarcharPartitionKey(
                    (DateLiteral) from.lowerEndpoint().getKeys().get(0), formatter);

            PartitionKey upperPartitionKey = convertToVarcharPartitionKey(
                    (DateLiteral) from.upperEndpoint().getKeys().get(0), formatter);
            return Range.range(lowerPartitionKey, from.lowerBoundType(), upperPartitionKey, from.upperBoundType());
        } else if (from.hasUpperBound()) {
            PartitionKey upperPartitionKey = convertToVarcharPartitionKey(
                    (DateLiteral) from.upperEndpoint().getKeys().get(0), formatter);
            return Range.upTo(upperPartitionKey, from.upperBoundType());
        } else if (from.hasLowerBound()) {
            PartitionKey lowerPartitionKey = convertToVarcharPartitionKey(
                    (DateLiteral) from.lowerEndpoint().getKeys().get(0), formatter);
            return Range.downTo(lowerPartitionKey, from.lowerBoundType());
        }
        return Range.all();
    }

    private static PartitionKey convertToVarcharPartitionKey(DateLiteral dateLiteral, DateTimeFormatter formatter) {
        String lowerDateString = dateLiteral.toLocalDateTime().toLocalDate().format(formatter);
        PartitionKey partitionKey = PartitionKey.ofString(lowerDateString);
        return partitionKey;
    }

    /**
     * Return the updated partition key ranges of the specific table.
     *
     * NOTE: This method can be only used in query rewrite and cannot be used to insert routine.
     * @param partitionByTable          : the base table of the mv
     * @param partitionColumn           : the partition column of the base table
     * @param mv                        : the materialized view
     * @param mvPartitionNamesToRefresh : the updated partition names  of the materialized view
     * @return
     */
    private static List<Range<PartitionKey>> getLatestPartitionRangeForTable(
            Table partitionByTable,
            Column partitionColumn,
            MaterializedView mv,
            Set<String> mvPartitionNamesToRefresh) throws AnalysisException {
        Set<String> refBaseTableUpdatedPartitionNames = mv.getUpdatedPartitionNamesOfTable(partitionByTable, true);
        List<Range<PartitionKey>> refBaseTableRanges = getLatestPartitionRange(partitionByTable, partitionColumn,
                refBaseTableUpdatedPartitionNames, MaterializedView.getPartitionExpr(mv));
        // date to varchar range
        Map<Range<PartitionKey>, Range<PartitionKey>> baseRangeMapping = null;
        boolean isConvertToDate = PartitionUtil.isConvertToDate(mv.getFirstPartitionRefTableExpr(), partitionColumn);
        if (isConvertToDate) {
            baseRangeMapping = Maps.newHashMap();
            // convert varchar range to date range
            List<Range<PartitionKey>> baseTableDateRanges = Lists.newArrayList();
            for (Range<PartitionKey> range : refBaseTableRanges) {
                Range<PartitionKey> datePartitionRange = convertToDateRange(range);
                baseTableDateRanges.add(datePartitionRange);
                baseRangeMapping.put(datePartitionRange, range);
            }
            refBaseTableRanges = baseTableDateRanges;
        }
        // materialized view latest partition ranges except to-refresh partitions
        List<Range<PartitionKey>> mvRanges = getLatestPartitionRangeForNativeTable(mv, mvPartitionNamesToRefresh);

        List<Range<PartitionKey>> latestBaseTableRanges = Lists.newArrayList();
        for (Range<PartitionKey> range : refBaseTableRanges) {
            // if materialized view's partition range can enclose the ref base table range, we think that
            // the materialized view's partition has been refreshed and should be compensated into the materialized
            // view's partition predicate.
            if (mvRanges.stream().anyMatch(mvRange -> mvRange.encloses(range))) {
                latestBaseTableRanges.add(range);
            }
        }
        if (isConvertToDate) {
            // treat string type partition as list, so no need merge
            List<Range<PartitionKey>> tmpRangeList = Lists.newArrayList();
            for (Range<PartitionKey> range : latestBaseTableRanges) {
                tmpRangeList.add(baseRangeMapping.get(range));
            }
            return tmpRangeList;
        } else {
            return MvUtils.mergeRanges(latestBaseTableRanges);
        }
    }

    private static List<Range<PartitionKey>> getLatestPartitionRangeForNativeTable(OlapTable partitionTable,
                                                                                   Set<String> modifiedPartitionNames) {
        // partitions that will be excluded
        Set<Long> filteredIds = Sets.newHashSet();
        for (Partition p : partitionTable.getPartitions()) {
            if (modifiedPartitionNames.contains(p.getName()) || !p.hasData()) {
                filteredIds.add(p.getId());
            }
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionTable.getPartitionInfo();
        return rangePartitionInfo.getRangeList(filteredIds, false);
    }

    private static List<Range<PartitionKey>> getLatestPartitionRange(
            Table table, Column partitionColumn, Set<String> modifiedPartitionNames, Expr partitionExpr) {
        if (table.isNativeTableOrMaterializedView()) {
            return getLatestPartitionRangeForNativeTable((OlapTable) table, modifiedPartitionNames);
        } else {
            Map<String, Range<PartitionKey>> partitionMap;
            try {
                partitionMap = PartitionUtil.getPartitionKeyRange(table, partitionColumn, partitionExpr);
            } catch (UserException e) {
                LOG.warn("Materialized view Optimizer compute partition range failed.", e);
                return Lists.newArrayList();
            }
            return partitionMap.entrySet().stream().filter(entry -> !modifiedPartitionNames.contains(entry.getKey())).
                    map(Map.Entry::getValue).collect(Collectors.toList());
        }
    }

    public static String toString(Object o) {
        if (o == null) {
            return "";
        }
        return o.toString();
    }


    /**
     * Return the max refresh timestamp of all partition infos.
     */
    public static long  getMaxTablePartitionInfoRefreshTime(
            Collection<Map<String, MaterializedView.BasePartitionInfo>> partitionInfos) {
        return partitionInfos.stream()
                .flatMap(x -> x.values().stream())
                .map(x -> x.getLastRefreshTime())
                .max(Long::compareTo)
                .filter(Objects::nonNull)
                .orElse(System.currentTimeMillis());
    }

    public static List<ScalarOperator> collectOnPredicate(OptExpression optExpression) {
        List<ScalarOperator> onPredicates = Lists.newArrayList();
        collectOnPredicate(optExpression, onPredicates, false);
        return onPredicates;
    }

    public static List<ScalarOperator> collectOuterAntiJoinOnPredicate(OptExpression optExpression) {
        List<ScalarOperator> onPredicates = Lists.newArrayList();
        collectOnPredicate(optExpression, onPredicates, true);
        return onPredicates;
    }

    public static void collectOnPredicate(
            OptExpression optExpression, List<ScalarOperator> onPredicates, boolean onlyOuterAntiJoin) {
        for (OptExpression child : optExpression.getInputs()) {
            collectOnPredicate(child, onPredicates, onlyOuterAntiJoin);
        }
        if (optExpression.getOp() instanceof LogicalJoinOperator) {
            LogicalJoinOperator joinOperator = optExpression.getOp().cast();
            if (onlyOuterAntiJoin &&
                    !(joinOperator.getJoinType().isOuterJoin() || joinOperator.getJoinType().isAntiJoin())) {
                return;
            }
            onPredicates.addAll(Utils.extractConjuncts(joinOperator.getOnPredicate()));
        }
    }

    public static boolean isSupportViewDelta(OptExpression optExpression) {
        return getAllJoinOperators(optExpression).stream().allMatch(x -> isSupportViewDelta(x));
    }

    public static boolean isSupportViewDelta(JoinOperator joinOperator) {
        return  joinOperator.isLeftOuterJoin() || joinOperator.isInnerJoin();
    }

    public static SlotRef extractPartitionSlotRef(Expr paritionExpr) {
        List<SlotRef> slotRefs = Lists.newArrayList();
        paritionExpr.collect(SlotRef.class, slotRefs);
        Preconditions.checkState(slotRefs.size() == 1);
        return slotRefs.get(0);
    }

    /**
     * Inactive related mvs after modified columns have been done. Only inactive mvs after
     * modified columns have done because the modified process may be failed and in this situation
     * should not inactive mvs then.
     */
    public static void inactiveRelatedMaterializedViews(Database db,
                                                        OlapTable olapTable,
                                                        Set<String> modifiedColumns) {
        if (modifiedColumns == null || modifiedColumns.isEmpty()) {
            return;
        }
        // inactive related asynchronous mvs
        for (MvId mvId : olapTable.getRelatedMaterializedViews()) {
            MaterializedView mv = (MaterializedView) db.getTable(mvId.getId());
            if (mv == null) {
                LOG.warn("Ignore materialized view {} does not exists", mvId);
                continue;

            }
            // TODO: support more types for base table's schema change.
            try {
                List<MvPlanContext> mvPlanContexts = MvPlanContextBuilder.getPlanContext(mv);
                for (MvPlanContext mvPlanContext : mvPlanContexts) {
                    if (mvPlanContext != null) {
                        OptExpression mvPlan = mvPlanContext.getLogicalPlan();
                        List<ColumnRefOperator> usedColRefs = MvUtils.collectScanColumn(mvPlan, scan -> {
                            if (scan == null) {
                                return false;
                            }
                            Table table = scan.getTable();
                            return table.getId() == olapTable.getId();
                        });
                        Set<String> usedColNames = usedColRefs.stream()
                                .map(x -> x.getName())
                                .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
                        for (String modifiedColumn : modifiedColumns) {
                            if (usedColNames.contains(modifiedColumn)) {
                                LOG.warn("Setting the materialized view {}({}) to invalid because " +
                                                "the column {} of the table {} was modified.", mv.getName(), mv.getId(),
                                        modifiedColumn, olapTable.getName());
                                mv.setInactiveAndReason(
                                        "base table schema changed for columns: " + Joiner.on(",").join(modifiedColumns));
                            }
                        }
                    }
                }
            } catch (SemanticException e) {
                LOG.warn("Get related materialized view {} failed:", mv.getName(), e);
                LOG.warn("Setting the materialized view {}({}) to invalid because " +
                                "the columns  of the table {} was modified.", mv.getName(), mv.getId(),
                        olapTable.getName());
                mv.setInactiveAndReason(
                        "base table schema changed for columns: " + Joiner.on(",").join(modifiedColumns));
            } catch (Exception e) {
                LOG.warn("Get related materialized view {} failed:", mv.getName(), e);
                // basic check: may lose some situations
                for (Column mvColumn : mv.getColumns()) {
                    if (modifiedColumns.contains(mvColumn.getName())) {
                        LOG.warn("Setting the materialized view {}({}) to invalid because " +
                                        "the column {} of the table {} was modified.", mv.getName(), mv.getId(),
                                mvColumn.getName(), olapTable.getName());
                        mv.setInactiveAndReason(
                                "base table schema changed for columns: " + Joiner.on(",").join(modifiedColumns));
                        break;
                    }
                }
            }
        }
    }

    public static void collectViewScanOperator(OptExpression tree, Collection<Operator> viewScanOperators) {
        if (tree.getOp() instanceof LogicalViewScanOperator) {
            viewScanOperators.add(tree.getOp());
        } else {
            for (OptExpression input : tree.getInputs()) {
                collectViewScanOperator(input, viewScanOperators);
            }
        }
    }

    public static OptExpression replaceLogicalViewScanOperator(
            OptExpression queryExpression, List<LogicalViewScanOperator> viewScans) {
        if (viewScans == null) {
            return queryExpression;
        }
        // add a LogicalTreeAnchorOperator to replace the tree easier
        OptExpression anchorExpr = OptExpression.create(new LogicalTreeAnchorOperator(), queryExpression);
        doReplaceLogicalViewScanOperator(anchorExpr, 0, queryExpression, viewScans);
        List<Operator> viewScanOperators = Lists.newArrayList();
        MvUtils.collectViewScanOperator(anchorExpr, viewScanOperators);
        if (!viewScanOperators.isEmpty()) {
            return null;
        }
        OptExpression newQuery = anchorExpr.inputAt(0);
        deriveLogicalProperty(newQuery);
        return newQuery;
    }

    private static void doReplaceLogicalViewScanOperator(
            OptExpression parent,
            int index,
            OptExpression queryExpression,
            List<LogicalViewScanOperator> viewScans) {
        LogicalOperator op = queryExpression.getOp().cast();
        if (op instanceof LogicalViewScanOperator) {
            LogicalViewScanOperator viewScanOperator = op.cast();
            OptExpression viewPlan = viewScanOperator.getOriginalPlan();
            parent.setChild(index, viewPlan);
            return;
        }
        for (int i = 0; i < queryExpression.getInputs().size(); i++) {
            doReplaceLogicalViewScanOperator(queryExpression, i, queryExpression.inputAt(i), viewScans);
        }
    }

    public static void deriveLogicalProperty(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            deriveLogicalProperty(child);
        }

        ExpressionContext context = new ExpressionContext(root);
        context.deriveLogicalProperty();
        root.setLogicalProperty(context.getRootProperty());
    }

    public static OptExpression cloneExpression(OptExpression logicalTree) {
        List<OptExpression> inputs = Lists.newArrayList();
        for (OptExpression input : logicalTree.getInputs()) {
            OptExpression clone = cloneExpression(input);
            inputs.add(clone);
        }
        Operator.Builder builder = OperatorBuilderFactory.build(logicalTree.getOp());
        builder.withOperator(logicalTree.getOp());
        Operator newOp = builder.build();
        return OptExpression.create(newOp, inputs);
    }
}
