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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.common.util.SRStringUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.mv.analyzer.MVPartitionExpr;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RandomDistributionDesc;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.PRangeCell;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MaterializedViewOptimizer;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.OptimizerOptions;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSetOperator;
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
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.mv.MVUtils;
import com.starrocks.sql.optimizer.rule.mv.MaterializedViewWrapper;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.optimizer.transformer.TransformerContext;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.util.Box;
import org.apache.commons.collections4.SetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;
import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;

public class MvUtils {
    private static final Logger LOG = LogManager.getLogger(MvUtils.class);

    public static Set<MaterializedViewWrapper> getRelatedMvs(ConnectContext connectContext,
                                                             int maxLevel,
                                                             Set<Table> tablesToCheck) {
        if (tablesToCheck.isEmpty()) {
            return Sets.newHashSet();
        }
        Set<MaterializedViewWrapper> mvs = Sets.newHashSet();
        getRelatedMvs(connectContext, maxLevel, 0, tablesToCheck, mvs);
        return mvs;
    }

    public static void getRelatedMvs(ConnectContext connectContext,
                                     int maxLevel, int currentLevel,
                                     Set<Table> tablesToCheck, Set<MaterializedViewWrapper> mvs) {
        if (currentLevel >= maxLevel) {
            logMVPrepare("Current level {} is greater than max level {}", currentLevel, maxLevel);
            return;
        }
        Set<MvId> newMvIds = Sets.newHashSet();
        for (Table table : tablesToCheck) {
            Set<MvId> mvIds = table.getRelatedMaterializedViews();
            if (mvIds != null && !mvIds.isEmpty()) {
                logMVPrepare("Table/MaterializedView {} has related materialized views: {}",
                        table.getName(), mvIds);
                newMvIds.addAll(mvIds);
            } else if (currentLevel == 0) {
                logMVPrepare("Table/MaterializedView {} has no related materialized views, " +
                        "identifier:{}", table.getName(), table.getTableIdentifier());
            }
        }
        if (newMvIds.isEmpty()) {
            return;
        }
        Set<Table> newMvs = Sets.newHashSet();
        for (MvId mvId : newMvIds) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvId.getDbId());
            if (db == null) {
                logMVPrepare("Cannot find database from mvId:{}", mvId);
                continue;
            }
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), mvId.getId());
            if (table == null) {
                logMVPrepare("Cannot find materialized view from mvId:{}", mvId);
                continue;
            }
            newMvs.add(table);
            mvs.add(MaterializedViewWrapper.create((MaterializedView) table, currentLevel));
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
    public static List<TableScanDesc> getTableScanDescs(OptExpression root, ColumnRefFactory refFactory) {
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
                Integer relationId = getRelationId(scanOperator);
                TableScanDesc tableScanDesc = new TableScanDesc(table, id, scanOperator, null, false, relationId);
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
                        Integer relationId = getRelationId(scanOperator);
                        TableScanDesc tableScanDesc = new TableScanDesc(
                                table, id, scanOperator, optExpression, i == 0, relationId);
                        context.getTableScanDescs().add(tableScanDesc);
                        scanContext.getTableIdMap().put(table, ++id);
                    } else {
                        child.getOp().accept(this, child, context);
                    }
                }
                return null;
            }

            private Integer getRelationId(LogicalScanOperator scanOperator) {
                Optional<ColumnRefOperator> columnOpt =
                        scanOperator.getColRefToColumnMetaMap().keySet().stream().findFirst();
                Preconditions.checkState(columnOpt.isPresent());
                Integer relationId = refFactory.getRelationId(columnOpt.get().getId());
                Preconditions.checkState(relationId != -1);
                return relationId;
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
        if (root == null) {
            return Lists.newArrayList();
        }
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

    public static String getInvalidReason(OptExpression expr, boolean inlineView) {
        List<Operator> operators = collectOperators(expr);
        String viewRewriteHint = inlineView ? "no view rewrite" : "view rewrite";
        if (operators.stream().anyMatch(op -> !isLogicalSPJGOperator(op))) {
            String nonSPJGOperators =
                    operators.stream().filter(x -> !isLogicalSPJGOperator(x))
                            .map(Operator::toString)
                            .collect(Collectors.joining(","));
            return String.format("MV contains non-SPJG operators(%s): %s", viewRewriteHint, nonSPJGOperators);
        }
        return String.format("MV is not SPJG structure(%s)", viewRewriteHint);
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
            return level == 0 ? false : isLogicalSPJ(root);
        } else {
            LogicalAggregationOperator agg = (LogicalAggregationOperator) operator;
            if (agg.getType() != AggType.GLOBAL) {
                return false;
            }
            // Aggregate nested with aggregate is not supported yet.
            // eg:
            // create view v1 as
            // select count(distinct cnt)
            // from
            //   (
            //      select c_city, count(*) as cnt
            //      from customer
            //      group by c_city
            //    ) t
            if (level > 0) {
                return false;
            }
            OptExpression child = root.inputAt(0);
            return isLogicalSPJG(child, level + 1);
        }
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
        if (operator instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) operator;
            if (olapScanOperator.isSample()) {
                return false;
            }
        }
        for (OptExpression child : root.getInputs()) {
            if (!isLogicalSPJ(child)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isLogicalSPJGOperator(Operator operator) {
        if (operator instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) operator;
            if (olapScanOperator.isSample()) {
                return false;
            }
        }
        return (operator instanceof LogicalScanOperator)
                || (operator instanceof LogicalProjectOperator)
                || (operator instanceof LogicalFilterOperator)
                || (operator instanceof LogicalJoinOperator)
                || (operator instanceof LogicalAggregationOperator);
    }

    public static StatementBase parse(MaterializedView mv,
                                      String sql,
                                      ConnectContext connectContext) {
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
        return mvStmt;
    }

    public static Pair<OptExpression, LogicalPlan> getRuleOptimizedLogicalPlan(StatementBase mvStmt,
                                                                               ColumnRefFactory columnRefFactory,
                                                                               ConnectContext connectContext,
                                                                               OptimizerOptions optimizerOptions,
                                                                               boolean inlineView) {
        Preconditions.checkState(mvStmt instanceof QueryStatement);
        Analyzer.analyze(mvStmt, connectContext);
        QueryRelation query = ((QueryStatement) mvStmt).getQueryRelation();
        TransformerContext transformerContext =
                new TransformerContext(columnRefFactory, connectContext, inlineView, null);
        LogicalPlan logicalPlan = new RelationTransformer(transformerContext).transform(query);
        Optimizer optimizer =
                OptimizerFactory.create(
                        OptimizerFactory.initContext(connectContext, columnRefFactory, optimizerOptions));
        OptExpression optimizedPlan = optimizer.optimize(
                logicalPlan.getRoot(),
                new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()));
        return Pair.create(optimizedPlan, logicalPlan);
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

    public static void splitPredicate(
            Set<ScalarOperator> predicates,
            Set<String> equivalenceColumns,
            Set<String> nonEquivalenceColumns) {
        for (ScalarOperator operator : predicates) {
            if (!MVUtils.isPredicateUsedForPrefixIndex(operator)) {
                continue;
            }

            if (MVUtils.isEquivalencePredicate(operator)) {
                equivalenceColumns.add(operator.getColumnRefs().get(0).getName().toLowerCase());
            } else {
                nonEquivalenceColumns.add(operator.getColumnRefs().get(0).getName().toLowerCase());
            }
        }
    }

    public static Set<ScalarOperator> getAllValidPredicatesFromScans(OptExpression root) {
        List<LogicalScanOperator> scanOperators = getScanOperator(root);
        Set<ScalarOperator> predicates = Sets.newHashSet();
        scanOperators.stream().forEach(scanOperator -> predicates.addAll(Utils.extractConjuncts(scanOperator.getPredicate())));
        return predicates.stream().filter(MvUtils::isValidPredicate).collect(Collectors.toSet());
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
                }
                optExpression.inputAt(0).getOp().accept(this, optExpression.inputAt(0), joinKeyColumns);
                optExpression.inputAt(1).getOp().accept(this, optExpression.inputAt(1), joinKeyColumns);
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
     * both rather than one use `canonizePredicate` or `canonizePredicateForRewrite`.
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

    public static Set<ColumnRefOperator> collectScanColumn(OptExpression optExpression) {
        return collectScanColumn(optExpression, Predicates.alwaysTrue());
    }

    public static Set<ColumnRefOperator> collectScanColumn(OptExpression optExpression,
                                                            Predicate<LogicalScanOperator> predicate) {

        Set<ColumnRefOperator> columnRefOperators = Sets.newHashSet();
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

    /**
     * Convert partition range to IN predicate
     *
     * @param slotRef the comparison column
     * @param values  the target partition values
     * @return in predicate
     */
    public static Expr convertToInPredicate(Expr slotRef, List<Expr> values) {
        if (values == null || values.isEmpty()) {
            return new BoolLiteral(true);
        }
        // to avoid duplicate values
        if (values.size() == 1) {
            return new BinaryPredicate(BinaryType.EQ, slotRef, values.get(0));
        } else {
            return new InPredicate(slotRef, Lists.newArrayList(Sets.newHashSet(values)), false);
        }
    }

    /**
     * Convert partition range to IN predicate scalar operator
     *
     * @param col    the comparison operator
     * @param values the target scalar operators
     * @return in predicate scalar operator
     */
    public static ScalarOperator convertToInPredicate(ScalarOperator col,
                                                      List<ScalarOperator> values) {
        if (values == null || values.isEmpty()) {
            return ConstantOperator.TRUE;
        }
        List<ScalarOperator> inArgs = Lists.newArrayList();
        inArgs.add(col);
        inArgs.addAll(values);
        return new InPredicateOperator(false, inArgs);
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

    public static List<Range<PartitionKey>> mergeRanges(List<Pair<Long, Range<PartitionKey>>> ranges,
                                                        Map<Box<Range<PartitionKey>>, Set<Long>> queryMergeRangesToPartitionIds) {
        ranges.sort(Comparator.comparing(k -> k.second.lowerEndpoint()));
        List<Range<PartitionKey>> mergedRanges = Lists.newArrayList();
        for (Pair<Long, Range<PartitionKey>> currentRangePair : ranges) {
            boolean merged = false;
            Range<PartitionKey> currentRange = currentRangePair.second;
            long partitionId = currentRangePair.first;
            for (int j = 0; j < mergedRanges.size(); j++) {
                // 1 < r < 10, 10 <= r < 20 => 1 < r < 20
                Range<PartitionKey> mergedRange = mergedRanges.get(j);
                if (currentRange.isConnected(mergedRange)) {
                    // for partition range, the intersection must be empty
                    Preconditions.checkState(currentRange.intersection(mergedRange).isEmpty());
                    Range<PartitionKey> newRange = mergedRange.span(currentRange);
                    mergedRanges.set(j, newRange);

                    Box<Range<PartitionKey>> mergedRangeBox = Box.of(mergedRange);
                    Set<Long> mergePartitionIds = queryMergeRangesToPartitionIds.get(mergedRangeBox);
                    queryMergeRangesToPartitionIds.remove(mergedRangeBox);
                    mergePartitionIds.add(partitionId);
                    queryMergeRangesToPartitionIds.put(Box.of(newRange), mergePartitionIds);

                    merged = true;
                    break;
                }
            }
            if (!merged) {
                mergedRanges.add(currentRange);
                queryMergeRangesToPartitionIds.put(Box.of(currentRange), Sets.newHashSet(partitionId));
            }
        }
        return mergedRanges;
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


    public static String toString(Object o) {
        if (o == null) {
            return "";
        }
        return o.toString();
    }

    /**
     * Return the max refresh timestamp of all partition infos.
     */
    public static long getMaxTablePartitionInfoRefreshTime(
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
        return joinOperator.isLeftOuterJoin() || joinOperator.isInnerJoin();
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

    public static OptExpression replaceLogicalViewScanOperator(OptExpression queryExpression) {
        // add a LogicalTreeAnchorOperator to replace the tree easier
        OptExpression anchorExpr = OptExpression.create(new LogicalTreeAnchorOperator(), queryExpression);
        doReplaceLogicalViewScanOperator(anchorExpr, 0, queryExpression);
        List<Operator> viewScanOperators = Lists.newArrayList();
        MvUtils.collectViewScanOperator(anchorExpr, viewScanOperators);
        if (!viewScanOperators.isEmpty()) {
            return null;
        }
        OptExpression newQuery = anchorExpr.inputAt(0);
        deriveLogicalProperty(newQuery);
        return newQuery;
    }

    private static void doReplaceLogicalViewScanOperator(OptExpression parent,
                                                         int index,
                                                         OptExpression queryExpression) {
        LogicalOperator op = queryExpression.getOp().cast();
        if (op instanceof LogicalViewScanOperator) {
            LogicalViewScanOperator viewScanOperator = op.cast();
            OptExpression viewPlan = viewScanOperator.getOriginalPlanEvaluator();
            parent.setChild(index, viewPlan);
            return;
        }
        for (int i = 0; i < queryExpression.getInputs().size(); i++) {
            doReplaceLogicalViewScanOperator(queryExpression, i, queryExpression.inputAt(i));
        }
    }

    public static void deriveLogicalProperty(OptExpression root) {
        // TODO: avoid duplicate derive
        if (root.getLogicalProperty() != null) {
            return;
        }

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

    /**
     * Return mv's plan context. If mv's plan context is not in cache, optimize it.
     *
     * @param connectContext: connect context
     * @param mv:             input mv
     * @param isInlineView:   whether to inline mv's difined query.
     */
    public static MvPlanContext getMVPlanContext(ConnectContext connectContext,
                                                 MaterializedView mv,
                                                 boolean isInlineView,
                                                 boolean isCheckNonDeterministicFunction) {
        // step1: get from mv plan cache
        List<MvPlanContext> mvPlanContexts = CachingMvPlanContextBuilder.getInstance()
                .getPlanContext(connectContext.getSessionVariable(), mv);
        if (mvPlanContexts != null && !mvPlanContexts.isEmpty() && mvPlanContexts.get(0).getLogicalPlan() != null) {
            // TODO: distinguish normal mv plan and view rewrite plan
            return mvPlanContexts.get(0);
        }
        // step2: get from optimize
        return new MaterializedViewOptimizer().optimize(mv, connectContext, isInlineView, isCheckNonDeterministicFunction);
    }

    /**
     * Get column refs of scanMvOperator by MV's defined output columns order.
     *
     * @param mv:       mv to be referenced for output's order
     * @param scanMvOp: scan mv operator which contains mv
     * @return: column refs of scanMvOperator in the defined order
     */
    public static List<ColumnRefOperator> getMvScanOutputColumnRefs(MaterializedView mv,
                                                                    LogicalOlapScanOperator scanMvOp) {
        List<ColumnRefOperator> scanMvOutputColumns = Lists.newArrayList();
        for (Column column : mv.getOrderedOutputColumns()) {
            scanMvOutputColumns.add(scanMvOp.getColumnReference(column));
        }
        return scanMvOutputColumns;
    }

    public static OptExpression addExtraPredicate(OptExpression result,
                                                  ScalarOperator extraPredicate) {
        Operator op = result.getOp();
        if (op instanceof LogicalSetOperator) {
            LogicalFilterOperator filter = new LogicalFilterOperator(extraPredicate);
            // use PUSH_DOWN_PREDICATE rule to push down filter after union all set after mv rewrite rule.
            return OptExpression.create(filter, result);
        } else {
            // If op is aggregate operator, use setPredicate directly.
            ScalarOperator origPredicate = op.getPredicate();
            if (op.getProjection() != null) {
                ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(op.getProjection().getColumnRefMap());
                ScalarOperator rewrittenExtraPredicate = rewriter.rewrite(extraPredicate);
                op.setPredicate(Utils.compoundAnd(origPredicate, rewrittenExtraPredicate));
            } else {
                op.setPredicate(Utils.compoundAnd(origPredicate, extraPredicate));
            }
            return result;
        }
    }

    public static ParseNode getQueryAst(String query, ConnectContext connectContext) {
        try {
            List<StatementBase> statementBases =
                    com.starrocks.sql.parser.SqlParser.parse(query, connectContext.getSessionVariable());
            if (statementBases.size() != 1) {
                return null;
            }
            StatementBase stmt = statementBases.get(0);
            Analyzer.analyze(stmt, connectContext);
            return stmt;
        } catch (ParsingException parsingException) {
            LOG.warn("Parse query {} failed:{}", query, parsingException);
        }
        return null;
    }

    /**
     * Return the query predicate split with/without compensate :
     * - with compensate    : deducing from the selected partition ids.
     * - without compensate : only get the partition predicate from pruned partitions of scan operator
     * eg: for sync mv without partition columns, we always no need compensate partition predicates because
     * mv and the base table are always synced.
     */
    public static PredicateSplit getQuerySplitPredicate(OptimizerContext optimizerContext,
                                                        MaterializationContext mvContext,
                                                        OptExpression queryExpression,
                                                        ColumnRefFactory queryColumnRefFactory,
                                                        ReplaceColumnRefRewriter queryColumnRefRewriter,
                                                        Rule rule) {
        // Cache partition predicate predicates because it's expensive time costing if there are too many materialized views or
        // query expressions are too complex.
        final ScalarOperator queryPartitionPredicate = MvPartitionCompensator.compensateQueryPartitionPredicate(
                mvContext, rule, queryColumnRefFactory, queryExpression);
        if (queryPartitionPredicate == null) {
            logMVRewrite(mvContext.getOptimizerContext(), rule, "Compensate query expression's partition " +
                    "predicates from pruned partitions failed.");
            return null;
        }

        Set<ScalarOperator> queryConjuncts = MvUtils.getPredicateForRewrite(queryExpression);
        // only add valid predicates into query split predicate
        if (!ConstantOperator.TRUE.equals(queryPartitionPredicate)) {
            queryConjuncts.addAll(MvUtils.getAllValidPredicates(queryPartitionPredicate));
        }

        QueryMaterializationContext queryMaterializationContext = optimizerContext.getQueryMaterializationContext();
        // Cache predicate split for predicates because it's time costing if there are too many materialized views.
        return queryMaterializationContext.getPredicateSplit(queryConjuncts, queryColumnRefRewriter);
    }

    public static Optional<Table> getTable(BaseTableInfo baseTableInfo) {
        return GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(new ConnectContext(), baseTableInfo);
    }

    public static Optional<Table> getTableWithIdentifier(BaseTableInfo baseTableInfo) {
        return GlobalStateMgr.getCurrentState().getMetadataMgr().getTableWithIdentifier(new ConnectContext(), baseTableInfo);
    }

    public static Table getTableChecked(BaseTableInfo baseTableInfo) {
        return GlobalStateMgr.getCurrentState().getMetadataMgr().getTableChecked(new ConnectContext(), baseTableInfo);
    }

    public static Optional<FunctionCallExpr> getStr2DateExpr(Expr partitionExpr) {
        List<Expr> matches = Lists.newArrayList();
        partitionExpr.collect(expr -> isStr2Date(expr), matches);
        if (matches.size() != 1) {
            return Optional.empty();
        }
        return Optional.of(matches.get(0).cast());
    }

    public static boolean isFuncCallExpr(Expr expr, String expectFuncName) {
        if (expr == null) {
            return false;
        }
        return expr instanceof FunctionCallExpr
                && ((FunctionCallExpr) expr).getFnName().getFunction().equalsIgnoreCase(expectFuncName);
    }

    public static boolean isStr2Date(Expr expr) {
        return isFuncCallExpr(expr, FunctionSet.STR2DATE);
    }

    public static Map<String, String> getPartitionProperties(MaterializedView materializedView) {
        Map<String, String> partitionProperties = new HashMap<>(4);
        partitionProperties.put("replication_num",
                String.valueOf(materializedView.getDefaultReplicationNum()));
        partitionProperties.put("storage_medium", materializedView.getStorageMedium());
        String storageCooldownTime =
                materializedView.getTableProperty().getProperties().get("storage_cooldown_time");
        if (storageCooldownTime != null) {
            // cast long str to time str e.g.  '1587473111000' -> '2020-04-21 15:00:00'
            String storageCooldownTimeStr = TimeUtils.longToTimeString(Long.parseLong(storageCooldownTime));
            partitionProperties.put("storage_cooldown_time", storageCooldownTimeStr);
        }
        return partitionProperties;
    }

    public static DistributionDesc getDistributionDesc(MaterializedView materializedView) {
        DistributionInfo distributionInfo = materializedView.getDefaultDistributionInfo();
        if (distributionInfo instanceof HashDistributionInfo) {
            List<String> distColumnNames = MetaUtils.getColumnNamesByColumnIds(
                    materializedView.getIdToColumn(), distributionInfo.getDistributionColumns());
            return new HashDistributionDesc(distributionInfo.getBucketNum(), distColumnNames);
        } else {
            return new RandomDistributionDesc();
        }
    }

    /**
     * Trim the input set if its size is larger than maxLength.
     *
     * @return the trimmed set.
     */
    public static <K> Collection<K> shrinkToSize(Collection<K> set, int maxLength) {
        if (set != null && set.size() > maxLength) {
            return set.stream().limit(maxLength).collect(Collectors.toSet());
        }
        return set;
    }

    /**
     * Trim the input map if its size is larger than maxLength.
     *
     * @return the trimmed map.
     */
    public static <K, V> Map<K, V> shrinkToSize(Map<K, V> map, int maxLength) {
        if (map != null && map.size() > maxLength) {
            return map.entrySet().stream().limit(maxLength).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        return map;
    }

    /**
     * Get the mv partition expr by partition expr maps and table.
     *
     * @param partitionExprMaps partition expr maps of the specific mv
     * @param table             the base table to find the specific partition expr
     * @return the mv partition expr if found, otherwise empty
     */
    public static List<MVPartitionExpr> getMvPartitionExpr(Map<Expr, SlotRef> partitionExprMaps, Table table) {
        if (partitionExprMaps == null || partitionExprMaps.isEmpty() || table == null) {
            return null;
        }
        return partitionExprMaps.entrySet().stream()
                .filter(entry -> SRStringUtils.areTableNamesEqual(table, entry.getValue().getTblNameWithoutAnalyzed().getTbl()))
                .map(entry -> new MVPartitionExpr(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    /**
     * Get the column by slot ref from table's columns.
     * @return the column if found, otherwise empty
     */
    public static Optional<Column> getColumnBySlotRef(List<Column> columns, SlotRef slotRef) {
        return columns.stream()
                .filter(col -> SRStringUtils.areColumnNamesEqual(slotRef.getColumnName(), col.getName())).findFirst();
    }

    /**
     * Format the base table infos to readable string.
     *
     * @param baseTableInfos: input base table infos
     * @return formatted string
     */
    public static String formatBaseTableInfos(List<BaseTableInfo> baseTableInfos) {
        if (baseTableInfos == null) {
            return "";
        }
        return baseTableInfos.stream().map(BaseTableInfo::getReadableString).collect(Collectors.joining(","));
    }

    public static ScalarOperator convertPartitionKeyRangesToListPredicate(List<? extends ScalarOperator> partitionColRefs,
                                                                          Collection<PRangeCell> pRangeCells,
                                                                          boolean areAllRangePartitionsSingleton) {
        final List<Range<PartitionKey>> partitionRanges = pRangeCells
                .stream()
                .map(PRangeCell::getRange)
                .collect(Collectors.toList());

        return convertPartitionKeysToListPredicate(partitionColRefs, partitionRanges, areAllRangePartitionsSingleton);
    }

    public static ScalarOperator convertPartitionKeysToListPredicate(List<? extends ScalarOperator> partitionColRefs,
                                                                     Collection<PartitionKey> partitionRanges) {
        final List<ScalarOperator> values = Lists.newArrayList();
        if (partitionColRefs.size() == 1) {
            for (PartitionKey key : partitionRanges) {
                final List<LiteralExpr> literalExprs = key.getKeys();
                Preconditions.checkArgument(literalExprs.size() == partitionColRefs.size());
                final LiteralExpr literalExpr = literalExprs.get(0);
                final ConstantOperator upperBound = (ConstantOperator) SqlToScalarOperatorTranslator.translate(literalExpr);
                values.add(upperBound);
            }
            return MvUtils.convertToInPredicate(partitionColRefs.get(0), values);
        } else {
            for (PartitionKey key : partitionRanges) {
                List<LiteralExpr> literalExprs = key.getKeys();
                Preconditions.checkArgument(literalExprs.size() == partitionColRefs.size());
                // TODO: use row operator instead
                List<ScalarOperator> predicates = Lists.newArrayList();
                for (int i = 0; i < literalExprs.size(); i++) {
                    ScalarOperator partitionColRef = partitionColRefs.get(i);
                    LiteralExpr literalExpr = literalExprs.get(i);
                    ConstantOperator upperBound = (ConstantOperator) SqlToScalarOperatorTranslator.translate(literalExpr);
                    ScalarOperator eq = new BinaryPredicateOperator(BinaryType.EQ, partitionColRef, upperBound);
                    predicates.add(eq);
                }
                values.add(Utils.compoundAnd(predicates));
            }
            return Utils.compoundOr(values);
        }
    }

    private static ScalarOperator convertPartitionKeysToListPredicate(List<? extends ScalarOperator> partitionColRefs,
                                                                      Collection<Range<PartitionKey>> partitionRanges,
                                                                      boolean areAllRangePartitionsSingleton) {

        if (areAllRangePartitionsSingleton) {
            List<PartitionKey> partitionKeys = partitionRanges
                    .stream()
                    .map(Range::lowerEndpoint)
                    .collect(Collectors.toList());
            return convertPartitionKeysToListPredicate(partitionColRefs, partitionKeys);
        } else {
            final List<ScalarOperator> values = Lists.newArrayList();
            partitionRanges
                    .stream()
                    .map(range -> getPartitionKeyRangePredicate(partitionColRefs, range))
                    .forEach(values::add);
            return Utils.compoundOr(values);
        }
    }

    private static ScalarOperator getPartitionKeyRangePredicate(List<? extends ScalarOperator> partitionColRefs,
                                                                Range<PartitionKey> range) {
        final List<LiteralExpr> lowerLiteralExprs = range.lowerEndpoint().getKeys();
        final List<LiteralExpr> upperLiteralExprs = range.upperEndpoint().getKeys();
        Preconditions.checkArgument(lowerLiteralExprs.size() == upperLiteralExprs.size());
        Preconditions.checkArgument(lowerLiteralExprs.size() == partitionColRefs.size());
        final List<ScalarOperator> predicates = Lists.newArrayList();
        for (int i = 0; i < lowerLiteralExprs.size(); i++) {
            final ScalarOperator partitionColRef = partitionColRefs.get(i);
            final LiteralExpr lowerLiteralExpr = lowerLiteralExprs.get(i);
            final LiteralExpr upperLiteralExpr = upperLiteralExprs.get(i);
            final ConstantOperator lowerBound =
                    (ConstantOperator) SqlToScalarOperatorTranslator.translate(lowerLiteralExpr);
            final ConstantOperator upperBound =
                    (ConstantOperator) SqlToScalarOperatorTranslator.translate(upperLiteralExpr);
            final ScalarOperator gt = new BinaryPredicateOperator(BinaryType.GE, partitionColRef, lowerBound);
            final ScalarOperator ls = new BinaryPredicateOperator(BinaryType.LT, partitionColRef, upperBound);
            predicates.add(Utils.compoundAnd(gt, ls));
        }
        return Utils.compoundAnd(predicates);
    }

    /**
     * Optimize the inlined view plan.
     * @param logicalTree logical opt expression tree which has not been optimized
     * @param connectContext connect context
     * @param requiredColumns required columns
     * @param columnRefFactory query column ref factory
     * @return optimized view plan which has been rule based optimized
     */
    public static OptExpression optimizeViewPlan(OptExpression logicalTree,
                                                 ConnectContext connectContext,
                                                 ColumnRefSet requiredColumns,
                                                 ColumnRefFactory columnRefFactory) {
        OptimizerOptions optimizerOptions = OptimizerOptions.newRuleBaseOpt();
        optimizerOptions.disableRule(RuleType.GP_SINGLE_TABLE_MV_REWRITE);
        optimizerOptions.disableRule(RuleType.GP_MULTI_TABLE_MV_REWRITE);
        Optimizer optimizer = OptimizerFactory.create(
                OptimizerFactory.initContext(connectContext, columnRefFactory, optimizerOptions));
        OptExpression optimizedViewPlan = optimizer.optimize(logicalTree,
                new PhysicalPropertySet(), requiredColumns);
        return optimizedViewPlan;
    }

    /**
     * Trim the input string if its length is larger than maxLength.
     * @param input the input string
     * @param maxLength the max length
     */
    public static String shrinkToSize(String input, int maxLength) {
        if (input == null) {
            return "";
        }
        return input.length() > maxLength ? input.substring(0, maxLength) : input;
    }
}