// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.plan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.AggregateInfo;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.AssertNumRowsElement;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.SortInfo;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.IdGenerator;
import com.starrocks.common.UserException;
import com.starrocks.planner.AggregationNode;
import com.starrocks.planner.AnalyticEvalNode;
import com.starrocks.planner.AssertNumRowsNode;
import com.starrocks.planner.CrossJoinNode;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.EmptySetNode;
import com.starrocks.planner.EsScanNode;
import com.starrocks.planner.ExceptNode;
import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.HashJoinNode;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.IntersectNode;
import com.starrocks.planner.MetaScanNode;
import com.starrocks.planner.MysqlScanNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlannerContext;
import com.starrocks.planner.ProjectNode;
import com.starrocks.planner.RepeatNode;
import com.starrocks.planner.RuntimeFilterId;
import com.starrocks.planner.SchemaScanNode;
import com.starrocks.planner.SelectNode;
import com.starrocks.planner.SetOperationNode;
import com.starrocks.planner.SortNode;
import com.starrocks.planner.TableFunctionNode;
import com.starrocks.planner.UnionNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.GatherDistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSetOperation;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils;
import com.starrocks.thrift.TPartitionType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF;
import static com.starrocks.planner.AdapterNode.checkPlanIsVectorized;
import static com.starrocks.sql.common.ErrorType.INTERNAL_ERROR;
import static com.starrocks.sql.common.UnsupportedException.unsupportedException;
import static com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils.getEqConj;

/**
 * PlanFragmentBuilder used to transform physical operator to exec plan fragment
 */
public class PlanFragmentBuilder {
    private static final Logger LOG = LogManager.getLogger(PlanFragmentBuilder.class);

    public ExecPlan createPhysicalPlan(OptExpression plan, PlannerContext plannerContext, ConnectContext connectContext,
                                       List<ColumnRefOperator> outputColumns, ColumnRefFactory columnRefFactory,
                                       List<String> colNames) {
        ExecPlan execPlan = new ExecPlan(plannerContext, connectContext, colNames);
        createOutputFragment(new PhysicalPlanTranslator(columnRefFactory).visit(plan, execPlan), execPlan,
                outputColumns);

        try {
            List<PlanFragment> fragments = execPlan.getFragments();
            for (PlanFragment fragment : fragments) {
                fragment.finalize(null, false);
            }
            Collections.reverse(fragments);
            checkPlanIsVectorized(fragments);
        } catch (UserException e) {
            throw new StarRocksPlannerException("Create fragment fail, " + e.getMessage(), INTERNAL_ERROR);
        }

        return execPlan;
    }

    public ExecPlan createStatisticPhysicalPlan(OptExpression plan, PlannerContext plannerContext,
                                                ConnectContext connectContext,
                                                List<ColumnRefOperator> outputColumns,
                                                ColumnRefFactory columnRefFactory, boolean isStatistic) {
        ExecPlan execPlan = new ExecPlan(plannerContext, connectContext, new ArrayList<>());
        createOutputFragment(new PhysicalPlanTranslator(columnRefFactory).visit(plan, execPlan), execPlan,
                outputColumns);

        List<PlanFragment> fragments = execPlan.getFragments();
        for (PlanFragment fragment : fragments) {
            fragment.finalizeForStatistic(isStatistic);
        }
        Collections.reverse(fragments);
        return execPlan;
    }

    private void createOutputFragment(PlanFragment inputFragment, ExecPlan execPlan,
                                      List<ColumnRefOperator> outputColumns) {
        if (inputFragment.getPlanRoot() instanceof ExchangeNode || !inputFragment.isPartitioned()) {
            List<Expr> outputExprs = outputColumns.stream().map(variable -> ScalarOperatorToExpr
                    .buildExecExpression(variable,
                            new ScalarOperatorToExpr.FormatterContext(execPlan.getColRefToExpr()))
            ).collect(Collectors.toList());
            inputFragment.setOutputExprs(outputExprs);
            execPlan.getOutputExprs().addAll(outputExprs);
            return;
        }

        List<Expr> outputExprs = outputColumns.stream().map(variable -> ScalarOperatorToExpr
                .buildExecExpression(variable, new ScalarOperatorToExpr.FormatterContext(execPlan.getColRefToExpr())))
                .collect(Collectors.toList());
        execPlan.getOutputExprs().addAll(outputExprs);

        // Single tablet direct output
        if (execPlan.getScanNodes().stream().allMatch(d -> d instanceof OlapScanNode)
                && execPlan.getScanNodes().stream().map(d -> ((OlapScanNode) d).getScanTabletIds().size())
                .reduce(Integer::sum).orElse(2) <= 1) {
            inputFragment.setOutputExprs(outputExprs);
            return;
        }

        ExchangeNode exchangeNode =
                new ExchangeNode(execPlan.getPlanCtx().getNextNodeId(), inputFragment.getPlanRoot(), false);
        exchangeNode.setNumInstances(1);
        PlanFragment exchangeFragment =
                new PlanFragment(execPlan.getPlanCtx().getNextFragmentId(), exchangeNode, DataPartition.UNPARTITIONED);
        inputFragment.setDestination(exchangeNode);
        inputFragment.setOutputPartition(DataPartition.UNPARTITIONED);

        exchangeFragment.setOutputExprs(outputExprs);
        execPlan.getFragments().add(exchangeFragment);
    }

    private static class PhysicalPlanTranslator extends OptExpressionVisitor<PlanFragment, ExecPlan> {
        private final ColumnRefFactory columnRefFactory;
        private final IdGenerator<RuntimeFilterId> runtimeFilterIdIdGenerator = RuntimeFilterId.createGenerator();

        public PhysicalPlanTranslator(ColumnRefFactory columnRefFactory) {
            this.columnRefFactory = columnRefFactory;
        }

        @Override
        public PlanFragment visitPhysicalProject(OptExpression optExpr, ExecPlan context) {
            PhysicalProjectOperator node = (PhysicalProjectOperator) optExpr.getOp();
            PlanFragment inputFragment = visit(optExpr.inputAt(0), context);

            Preconditions.checkState(!node.getColumnRefMap().isEmpty());

            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();

            Map<SlotId, Expr> commonSubOperatorMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : node.getCommonSubOperatorMap().entrySet()) {
                Expr expr = ScalarOperatorToExpr.buildExecExpression(entry.getValue(),
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr(),
                                node.getCommonSubOperatorMap()));

                commonSubOperatorMap.put(new SlotId(entry.getKey().getId()), expr);

                SlotDescriptor slotDescriptor =
                        context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(entry.getKey().getId()));
                slotDescriptor.setIsNullable(expr.isNullable());
                slotDescriptor.setIsMaterialized(false);
                slotDescriptor.setType(expr.getType());
                context.getColRefToExpr().put(entry.getKey(), new SlotRef(entry.getKey().toString(), slotDescriptor));
            }

            Map<SlotId, Expr> projectMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : node.getColumnRefMap().entrySet()) {
                Expr expr = ScalarOperatorToExpr.buildExecExpression(entry.getValue(),
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr(), node.getColumnRefMap()));

                projectMap.put(new SlotId(entry.getKey().getId()), expr);

                SlotDescriptor slotDescriptor =
                        context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(entry.getKey().getId()));
                slotDescriptor.setIsNullable(expr.isNullable());
                slotDescriptor.setIsMaterialized(true);
                slotDescriptor.setType(expr.getType());

                context.getColRefToExpr().put(entry.getKey(), new SlotRef(entry.getKey().toString(), slotDescriptor));
            }

            ProjectNode projectNode =
                    new ProjectNode(context.getPlanCtx().getNextNodeId(),
                            tupleDescriptor,
                            inputFragment.getPlanRoot(),
                            projectMap,
                            commonSubOperatorMap);

            projectNode.setHasNullableGenerateChild();
            projectNode.computeStatistics(optExpr.getStatistics());
            for (SlotId sid : projectMap.keySet()) {
                SlotDescriptor slotDescriptor = tupleDescriptor.getSlot(sid.asInt());
                slotDescriptor.setIsNullable(slotDescriptor.getIsNullable() | projectNode.isHasNullableGenerateChild());
            }
            tupleDescriptor.computeMemLayout();

            projectNode.setLimit(inputFragment.getPlanRoot().getLimit());
            inputFragment.setPlanRoot(projectNode);
            return inputFragment;
        }

        @Override
        public PlanFragment visitPhysicalOlapScan(OptExpression optExpr, ExecPlan context) {
            PhysicalOlapScanOperator node = (PhysicalOlapScanOperator) optExpr.getOp();

            OlapTable referenceTable = (OlapTable) node.getTable();
            context.getDescTbl().addReferencedTable(referenceTable);
            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            tupleDescriptor.setTable(referenceTable);

            OlapScanNode scanNode =
                    new OlapScanNode(context.getPlanCtx().getNextNodeId(), tupleDescriptor, "OlapScanNode");
            scanNode.setLimit(node.getLimit());
            scanNode.computeStatistics(optExpr.getStatistics());

            // set tablet
            try {
                scanNode.updateScanInfo(node.getSelectedPartitionId(),
                        node.getSelectedTabletId(),
                        node.getSelectedIndexId());
                long selectedIndexId = node.getSelectedIndexId();
                long totalTabletsNum = 0;
                // Compatible with old tablet selected, copy from "OlapScanNode::computeTabletInfo"
                // we can remove code when refactor tablet select
                for (Long partitionId : node.getSelectedPartitionId()) {
                    final Partition partition = referenceTable.getPartition(partitionId);
                    final MaterializedIndex selectedTable = partition.getIndex(selectedIndexId);

                    final List<Tablet> tablets = Lists.newArrayList();
                    for (Long id : node.getSelectedTabletId()) {
                        if (selectedTable.getTablet(id) != null) {
                            tablets.add(selectedTable.getTablet(id));
                        }
                    }

                    long localBeId = -1;
                    if (Config.enable_local_replica_selection) {
                        localBeId = Catalog.getCurrentSystemInfo()
                                .getBackendIdByHost(FrontendOptions.getLocalHostAddress());
                    }

                    List<Long> allTabletIds = selectedTable.getTabletIdsInOrder();
                    Map<Long, Integer> tabletId2BucketSeq = Maps.newHashMap();
                    for (int i = 0; i < allTabletIds.size(); i++) {
                        tabletId2BucketSeq.put(allTabletIds.get(i), i);
                    }

                    totalTabletsNum += selectedTable.getTablets().size();
                    scanNode.setTabletId2BucketSeq(tabletId2BucketSeq);
                    scanNode.addScanRangeLocations(partition, selectedTable, tablets, localBeId);
                }
                scanNode.setTotalTabletsNum(totalTabletsNum);
            } catch (UserException e) {
                throw new StarRocksPlannerException(
                        "Build Exec OlapScanNode fail, scan info is invalid," + e.getMessage(),
                        INTERNAL_ERROR);
            }

            // set slot
            for (Map.Entry<ColumnRefOperator, Column> entry : node.getColRefToColumnMetaMap().entrySet()) {
                SlotDescriptor slotDescriptor =
                        context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(entry.getKey().getId()));
                slotDescriptor.setColumn(entry.getValue());
                slotDescriptor.setIsNullable(entry.getValue().isAllowNull());
                slotDescriptor.setIsMaterialized(true);
                context.getColRefToExpr().put(entry.getKey(), new SlotRef(entry.getKey().toString(), slotDescriptor));
            }

            // set predicate
            List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());

            for (ScalarOperator predicate : predicates) {
                scanNode.getConjuncts().add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
            }

            tupleDescriptor.computeMemLayout();

            // set isPreAggregation
            scanNode.setIsPreAggregation(node.isPreAggregation(), node.getTurnOffReason());

            context.getScanNodes().add(scanNode);
            PlanFragment fragment =
                    new PlanFragment(context.getPlanCtx().getNextFragmentId(), scanNode, DataPartition.RANDOM);
            context.getFragments().add(fragment);
            return fragment;
        }

        @Override
        public PlanFragment visitPhysicalMetaScan(OptExpression optExpression, ExecPlan context) {
            PhysicalMetaScanOperator scan = (PhysicalMetaScanOperator) optExpression.getOp();

            context.getDescTbl().addReferencedTable(scan.getTable());
            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            tupleDescriptor.setTable(scan.getTable());

            MetaScanNode scanNode =
                    new MetaScanNode(context.getPlanCtx().getNextNodeId(),
                            tupleDescriptor, (OlapTable) scan.getTable(), scan.getAggColumnIdToNames());
            scanNode.computeRangeLocations();

            for (Map.Entry<ColumnRefOperator, Column> entry : scan.getColRefToColumnMetaMap().entrySet()) {
                SlotDescriptor slotDescriptor =
                        context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(entry.getKey().getId()));
                slotDescriptor.setColumn(entry.getValue());
                slotDescriptor.setIsNullable(entry.getValue().isAllowNull());
                slotDescriptor.setIsMaterialized(true);
                context.getColRefToExpr().put(entry.getKey(), new SlotRef(entry.getKey().getName(), slotDescriptor));
            }
            tupleDescriptor.computeMemLayout();

            context.getScanNodes().add(scanNode);
            PlanFragment fragment =
                    new PlanFragment(context.getPlanCtx().getNextFragmentId(), scanNode, DataPartition.RANDOM);
            context.getFragments().add(fragment);
            return fragment;
        }

        @Override
        public PlanFragment visitPhysicalHiveScan(OptExpression optExpression, ExecPlan context) {
            PhysicalHiveScanOperator node = (PhysicalHiveScanOperator) optExpression.getOp();

            Table referenceTable = node.getTable();
            context.getDescTbl().addReferencedTable(referenceTable);
            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            tupleDescriptor.setTable(referenceTable);

            // set slot
            for (Map.Entry<ColumnRefOperator, Column> entry : node.getColRefToColumnMetaMap().entrySet()) {
                SlotDescriptor slotDescriptor =
                        context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(entry.getKey().getId()));
                slotDescriptor.setColumn(entry.getValue());
                slotDescriptor.setIsNullable(entry.getValue().isAllowNull());
                slotDescriptor.setIsMaterialized(true);
                context.getColRefToExpr().put(entry.getKey(), new SlotRef(entry.getKey().toString(), slotDescriptor));
            }

            HdfsScanNode hdfsScanNode =
                    new HdfsScanNode(context.getPlanCtx().getNextNodeId(), tupleDescriptor, "HdfsScanNode");
            hdfsScanNode.computeStatistics(optExpression.getStatistics());
            try {
                hdfsScanNode.setSelectedPartitionIds(node.getSelectedPartitionIds());
                hdfsScanNode.setIdToPartitionKey(node.getIdToPartitionKey());
                hdfsScanNode.getScanRangeLocations(context.getDescTbl());
                // set predicate
                List<ScalarOperator> noEvalPartitionConjuncts = node.getNoEvalPartitionConjuncts();
                List<ScalarOperator> nonPartitionConjuncts = node.getNonPartitionConjuncts();
                List<ScalarOperator> minMaxConjuncts = node.getMinMaxConjuncts();
                ScalarOperatorToExpr.FormatterContext formatterContext =
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());

                for (ScalarOperator noEvalPartitionConjunct : noEvalPartitionConjuncts) {
                    hdfsScanNode.getNoEvalPartitionConjuncts().
                            add(ScalarOperatorToExpr.buildExecExpression(noEvalPartitionConjunct, formatterContext));
                }
                for (ScalarOperator nonPartitionConjunct : nonPartitionConjuncts) {
                    hdfsScanNode.getNonPartitionConjuncts().
                            add(ScalarOperatorToExpr.buildExecExpression(nonPartitionConjunct, formatterContext));
                }

                /*
                 * populates 'minMaxTuple' with slots for statistics values,
                 * and populates 'minMaxConjuncts' with conjuncts pointing into the 'minMaxTuple'
                 */
                TupleDescriptor minMaxTuple = context.getDescTbl().createTupleDescriptor();
                for (ScalarOperator minMaxConjunct : minMaxConjuncts) {
                    for (ColumnRefOperator columnRefOperator : Utils.extractColumnRef(minMaxConjunct)) {
                        SlotDescriptor slotDescriptor =
                                context.getDescTbl()
                                        .addSlotDescriptor(minMaxTuple, new SlotId(columnRefOperator.getId()));
                        Column column = node.getMinMaxColumnRefMap().get(columnRefOperator);
                        slotDescriptor.setColumn(column);
                        slotDescriptor.setIsNullable(column.isAllowNull());
                        slotDescriptor.setIsMaterialized(true);
                        context.getColRefToExpr()
                                .put(columnRefOperator, new SlotRef(columnRefOperator.toString(), slotDescriptor));
                    }
                }
                minMaxTuple.computeMemLayout();
                hdfsScanNode.setMinMaxTuple(minMaxTuple);
                ScalarOperatorToExpr.FormatterContext minMaxFormatterContext =
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
                for (ScalarOperator minMaxConjunct : minMaxConjuncts) {
                    hdfsScanNode.getMinMaxConjuncts().
                            add(ScalarOperatorToExpr.buildExecExpression(minMaxConjunct, minMaxFormatterContext));
                }
            } catch (Exception e) {
                LOG.warn("Hdfs scan node get scan range locations failed : " + e);
                throw new StarRocksPlannerException(e.getMessage(), INTERNAL_ERROR);
            }

            hdfsScanNode.setLimit(node.getLimit());

            tupleDescriptor.computeMemLayout();
            context.getScanNodes().add(hdfsScanNode);

            PlanFragment fragment =
                    new PlanFragment(context.getPlanCtx().getNextFragmentId(), hdfsScanNode, DataPartition.RANDOM);
            context.getFragments().add(fragment);
            return fragment;
        }

        @Override
        public PlanFragment visitPhysicalSchemaScan(OptExpression optExpression, ExecPlan context) {
            PhysicalSchemaScanOperator node = (PhysicalSchemaScanOperator) optExpression.getOp();

            context.getDescTbl().addReferencedTable(node.getTable());
            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            tupleDescriptor.setTable(node.getTable());

            for (Map.Entry<ColumnRefOperator, Column> entry : node.getColRefToColumnMetaMap().entrySet()) {
                SlotDescriptor slotDescriptor =
                        context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(entry.getKey().getId()));
                slotDescriptor.setColumn(entry.getValue());
                slotDescriptor.setIsNullable(entry.getValue().isAllowNull());
                slotDescriptor.setIsMaterialized(true);
                context.getColRefToExpr().put(entry.getKey(), new SlotRef(entry.getKey().toString(), slotDescriptor));
            }

            tupleDescriptor.computeMemLayout();

            SchemaScanNode scanNode = new SchemaScanNode(context.getPlanCtx().getNextNodeId(), tupleDescriptor);

            scanNode.setFrontendIP(FrontendOptions.getLocalHostAddress());
            scanNode.setFrontendPort(Config.rpc_port);
            scanNode.setUser(context.getConnectContext().getQualifiedUser());
            scanNode.setUserIp(context.getConnectContext().getRemoteIP());
            scanNode.setLimit(node.getLimit());

            // set predicate
            List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());

            for (ScalarOperator predicate : predicates) {
                scanNode.getConjuncts().add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
            }

            context.getScanNodes().add(scanNode);
            PlanFragment fragment =
                    new PlanFragment(context.getPlanCtx().getNextFragmentId(), scanNode, DataPartition.UNPARTITIONED);
            context.getFragments().add(fragment);
            return fragment;
        }

        @Override
        public PlanFragment visitPhysicalMysqlScan(OptExpression optExpression, ExecPlan context) {
            PhysicalMysqlScanOperator node = (PhysicalMysqlScanOperator) optExpression.getOp();

            context.getDescTbl().addReferencedTable(node.getTable());
            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            tupleDescriptor.setTable(node.getTable());

            for (Map.Entry<ColumnRefOperator, Column> entry : node.getColRefToColumnMetaMap().entrySet()) {
                SlotDescriptor slotDescriptor =
                        context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(entry.getKey().getId()));
                slotDescriptor.setColumn(entry.getValue());
                slotDescriptor.setIsNullable(entry.getValue().isAllowNull());
                slotDescriptor.setIsMaterialized(true);
                context.getColRefToExpr().put(entry.getKey(), new SlotRef(entry.getKey().getName(), slotDescriptor));
            }
            tupleDescriptor.computeMemLayout();

            MysqlScanNode scanNode = new MysqlScanNode(context.getPlanCtx().getNextNodeId(), tupleDescriptor,
                    (MysqlTable) node.getTable());

            // set predicate
            List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
            formatterContext.setImplicitCast(true);
            for (ScalarOperator predicate : predicates) {
                scanNode.getConjuncts().add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
            }

            scanNode.setLimit(node.getLimit());
            scanNode.computeColumnsAndFilters();
            scanNode.computeStatistics(optExpression.getStatistics());

            context.getScanNodes().add(scanNode);
            PlanFragment fragment =
                    new PlanFragment(context.getPlanCtx().getNextFragmentId(), scanNode, DataPartition.UNPARTITIONED);
            context.getFragments().add(fragment);
            return fragment;
        }

        @Override
        public PlanFragment visitPhysicalEsScan(OptExpression optExpression, ExecPlan context) {
            PhysicalEsScanOperator node = (PhysicalEsScanOperator) optExpression.getOp();

            context.getDescTbl().addReferencedTable(node.getTable());
            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            tupleDescriptor.setTable(node.getTable());

            for (Map.Entry<ColumnRefOperator, Column> entry : node.getColRefToColumnMetaMap().entrySet()) {
                SlotDescriptor slotDescriptor =
                        context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(entry.getKey().getId()));
                slotDescriptor.setColumn(entry.getValue());
                slotDescriptor.setIsNullable(entry.getValue().isAllowNull());
                slotDescriptor.setIsMaterialized(true);
                context.getColRefToExpr().put(entry.getKey(), new SlotRef(entry.getKey().toString(), slotDescriptor));
            }
            tupleDescriptor.computeMemLayout();

            EsScanNode scanNode = new EsScanNode(context.getPlanCtx().getNextNodeId(), tupleDescriptor, "EsScanNode");
            // set predicate
            List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());

            for (ScalarOperator predicate : predicates) {
                scanNode.getConjuncts().add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
            }
            scanNode.setLimit(node.getLimit());
            scanNode.computeStatistics(optExpression.getStatistics());
            try {
                scanNode.assignBackends();
            } catch (UserException e) {
                throw new StarRocksPlannerException(e.getMessage(), INTERNAL_ERROR);
            }
            scanNode.setShardScanRanges(scanNode.computeShardLocations(node.getSelectedIndex()));

            context.getScanNodes().add(scanNode);
            PlanFragment fragment =
                    new PlanFragment(context.getPlanCtx().getNextFragmentId(), scanNode, DataPartition.RANDOM);
            context.getFragments().add(fragment);
            return fragment;
        }

        @Override
        public PlanFragment visitPhysicalValues(OptExpression optExpr, ExecPlan context) {
            PhysicalValuesOperator valuesOperator = (PhysicalValuesOperator) optExpr.getOp();

            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            for (ColumnRefOperator columnRefOperator : valuesOperator.getColumnRefSet()) {
                SlotDescriptor slotDescriptor =
                        context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(columnRefOperator.getId()));
                slotDescriptor.setIsNullable(columnRefOperator.isNullable());
                slotDescriptor.setIsMaterialized(true);
                slotDescriptor.setType(columnRefOperator.getType());
                context.getColRefToExpr()
                        .put(columnRefOperator, new SlotRef(columnRefOperator.toString(), slotDescriptor));
            }
            tupleDescriptor.computeMemLayout();

            if (valuesOperator.getRows().isEmpty()) {
                EmptySetNode emptyNode = new EmptySetNode(context.getPlanCtx().getNextNodeId(),
                        Lists.newArrayList(tupleDescriptor.getId()));
                emptyNode.computeStatistics(optExpr.getStatistics());
                PlanFragment fragment = new PlanFragment(context.getPlanCtx().getNextFragmentId(), emptyNode,
                        DataPartition.UNPARTITIONED);
                context.getFragments().add(fragment);
                return fragment;
            } else {
                UnionNode unionNode = new UnionNode(context.getPlanCtx().getNextNodeId(), tupleDescriptor.getId());
                unionNode.setLimit(valuesOperator.getLimit());

                List<List<Expr>> consts = new ArrayList<>();
                for (List<ScalarOperator> row : valuesOperator.getRows()) {
                    List<Expr> exprRow = new ArrayList<>();
                    for (ScalarOperator field : row) {
                        exprRow.add(ScalarOperatorToExpr.buildExecExpression(
                                field, new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())));
                    }
                    consts.add(exprRow);
                }

                unionNode.setMaterializedConstExprLists_(consts);
                unionNode.computeStatistics(optExpr.getStatistics());
                /*
                 * TODO(lhy):
                 * It doesn't make sense for vectorized execution engines, but it will appear in explain.
                 * we can delete this when refactoring explain in the future,
                 */
                consts.forEach(unionNode::addConstExprList);

                PlanFragment fragment = new PlanFragment(context.getPlanCtx().getNextFragmentId(), unionNode,
                        DataPartition.UNPARTITIONED);
                context.getFragments().add(fragment);
                return fragment;
            }
        }

        @Override
        public PlanFragment visitPhysicalHashAggregate(OptExpression optExpr, ExecPlan context) {
            PhysicalHashAggregateOperator node = (PhysicalHashAggregateOperator) optExpr.getOp();
            PlanFragment inputFragment = visit(optExpr.inputAt(0), context);

            /*
             * Create aggregate TupleDescriptor
             */
            TupleDescriptor outputTupleDesc = context.getDescTbl().createTupleDescriptor();

            ArrayList<Expr> groupingExpressions = Lists.newArrayList();
            for (ColumnRefOperator grouping : node.getGroupBys()) {
                Expr groupingExpr = ScalarOperatorToExpr.buildExecExpression(grouping,
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr()));

                groupingExpressions.add(groupingExpr);

                SlotDescriptor slotDesc =
                        context.getDescTbl().addSlotDescriptor(outputTupleDesc, new SlotId(grouping.getId()));
                slotDesc.setType(groupingExpr.getType());
                slotDesc.setIsNullable(groupingExpr.isNullable());
                slotDesc.setIsMaterialized(true);
                context.getColRefToExpr().put(grouping, new SlotRef(grouping.toString(), slotDesc));
            }

            ArrayList<FunctionCallExpr> aggregateExprList = Lists.newArrayList();
            for (Map.Entry<ColumnRefOperator, CallOperator> aggregation : node.getAggregations().entrySet()) {
                FunctionCallExpr aggExpr = (FunctionCallExpr) ScalarOperatorToExpr.buildExecExpression(
                        aggregation.getValue(), new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr()));

                aggregateExprList.add(aggExpr);

                SlotDescriptor slotDesc = context.getDescTbl()
                        .addSlotDescriptor(outputTupleDesc, new SlotId(aggregation.getKey().getId()));
                slotDesc.setType(aggregation.getValue().getType());
                slotDesc.setIsNullable(aggExpr.isNullable());
                slotDesc.setIsMaterialized(true);
                context.getColRefToExpr()
                        .put(aggregation.getKey(), new SlotRef(aggregation.getKey().toString(), slotDesc));
            }

            List<Expr> partitionExpressions = Lists.newArrayList();
            for (ColumnRefOperator column : node.getPartitionByColumns()) {
                Expr partitionExpr = ScalarOperatorToExpr.buildExecExpression(column,
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr()));

                SlotDescriptor slotDesc =
                        context.getDescTbl().addSlotDescriptor(outputTupleDesc, new SlotId(column.getId()));
                slotDesc.setType(partitionExpr.getType());
                slotDesc.setIsNullable(partitionExpr.isNullable());
                slotDesc.setIsMaterialized(true);
                context.getColRefToExpr().put(column, new SlotRef(column.toString(), slotDesc));

                partitionExpressions.add(new SlotRef(slotDesc));
            }

            outputTupleDesc.computeMemLayout();

            AggregationNode aggregationNode;
            if (node.getType().isLocal()) {
                AggregateInfo aggInfo = AggregateInfo.create(
                        groupingExpressions,
                        aggregateExprList,
                        outputTupleDesc, outputTupleDesc,
                        AggregateInfo.AggPhase.FIRST);
                aggregationNode =
                        new AggregationNode(context.getPlanCtx().getNextNodeId(), inputFragment.getPlanRoot(), aggInfo);
                aggregationNode.unsetNeedsFinalize();
                aggregationNode.setIsPreagg(context.getPlanCtx());
                aggregationNode.setIntermediateTuple();

                if (!partitionExpressions.isEmpty()) {
                    inputFragment.setOutputPartition(DataPartition.hashPartitioned(partitionExpressions));
                }
            } else if (node.getType().isGlobal()) {
                if (node.hasSingleDistinct()) {
                    // For SQL: select count(id_int) as a, sum(DISTINCT id_bigint) as b from test_basic group by id_int;
                    // sum function is update function, but count is merge function
                    for (int i = 0; i < aggregateExprList.size(); i++) {
                        if (i != node.getSingleDistinctFunctionPos()) {
                            aggregateExprList.get(i).setMergeAggFn();
                        }
                    }

                    AggregateInfo aggInfo = AggregateInfo.create(
                            groupingExpressions,
                            aggregateExprList,
                            outputTupleDesc, outputTupleDesc,
                            AggregateInfo.AggPhase.SECOND);
                    aggregationNode =
                            new AggregationNode(context.getPlanCtx().getNextNodeId(), inputFragment.getPlanRoot(),
                                    aggInfo);
                } else if (!node.isSplit()) {
                    rewriteAggDistinctFirstStageFunction(context.getPlanCtx().getRootAnalyzer(), aggregateExprList);
                    AggregateInfo aggInfo = AggregateInfo.create(
                            groupingExpressions,
                            aggregateExprList,
                            outputTupleDesc, outputTupleDesc,
                            AggregateInfo.AggPhase.FIRST);
                    aggregationNode =
                            new AggregationNode(context.getPlanCtx().getNextNodeId(), inputFragment.getPlanRoot(),
                                    aggInfo);
                    if ((aggregationNode.getChild(0) instanceof OlapScanNode ||
                            (aggregationNode.getChild(0) instanceof ProjectNode &&
                                    aggregationNode.getChild(0).getChild(0) instanceof OlapScanNode))) {
                        ColocateTableIndex colocateIndex = Catalog.getCurrentColocateIndex();
                        OlapScanNode scanNode;
                        if (aggregationNode.getChild(0) instanceof ProjectNode) {
                            scanNode = (OlapScanNode) aggregationNode.getChild(0).getChild(0);
                        } else {
                            scanNode = (OlapScanNode) aggregationNode.getChild(0);
                        }
                        if (colocateIndex.isColocateTable(scanNode.getOlapTable().getId())) {
                            aggregationNode.setColocate(true);
                        }
                    }
                } else {
                    aggregateExprList.forEach(FunctionCallExpr::setMergeAggFn);
                    AggregateInfo aggInfo = AggregateInfo.create(
                            groupingExpressions,
                            aggregateExprList,
                            outputTupleDesc, outputTupleDesc,
                            AggregateInfo.AggPhase.SECOND_MERGE);
                    aggregationNode =
                            new AggregationNode(context.getPlanCtx().getNextNodeId(), inputFragment.getPlanRoot(),
                                    aggInfo);
                }

                // set predicate
                List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
                ScalarOperatorToExpr.FormatterContext formatterContext =
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());

                for (ScalarOperator predicate : predicates) {
                    aggregationNode.getConjuncts()
                            .add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
                }
                aggregationNode.setLimit(node.getLimit());
            } else if (node.getType().isDistinctGlobal()) {
                aggregateExprList.forEach(FunctionCallExpr::setMergeAggFn);
                AggregateInfo aggInfo = AggregateInfo.create(
                        groupingExpressions,
                        aggregateExprList,
                        outputTupleDesc, outputTupleDesc,
                        AggregateInfo.AggPhase.FIRST_MERGE);
                aggregationNode =
                        new AggregationNode(context.getPlanCtx().getNextNodeId(), inputFragment.getPlanRoot(), aggInfo);
                aggregationNode.unsetNeedsFinalize();
                aggregationNode.setIntermediateTuple();
            } else if (node.getType().isDistinctLocal()) {
                // For SQL: select count(distinct id_bigint), sum(id_int) from test_basic;
                // count function is update function, but sum is merge function
                for (int i = 0; i < aggregateExprList.size(); i++) {
                    if (i != node.getSingleDistinctFunctionPos()) {
                        aggregateExprList.get(i).setMergeAggFn();
                    }
                }

                AggregateInfo aggInfo = AggregateInfo.create(
                        groupingExpressions,
                        aggregateExprList,
                        outputTupleDesc, outputTupleDesc,
                        AggregateInfo.AggPhase.SECOND);
                aggregationNode =
                        new AggregationNode(context.getPlanCtx().getNextNodeId(), inputFragment.getPlanRoot(), aggInfo);
                aggregationNode.unsetNeedsFinalize();
                aggregationNode.setIsPreagg(context.getPlanCtx());
                aggregationNode.setIntermediateTuple();
            } else {
                throw unsupportedException("Not support aggregate type : " + node.getType());
            }

            aggregationNode.setStreamingPreaggregationMode(context.getConnectContext().
                    getSessionVariable().getStreamingPreaggregationMode());
            aggregationNode.setHasNullableGenerateChild();
            aggregationNode.computeStatistics(optExpr.getStatistics());
            inputFragment.setPlanRoot(aggregationNode);
            return inputFragment;
        }

        public void rewriteAggDistinctFirstStageFunction(Analyzer analyzer, List<FunctionCallExpr> aggregateExprList) {
            int singleDistinctCount = 0;
            int singleDistinctIndex = 0;
            FunctionCallExpr functionCallExpr = null;
            for (int i = 0; i < aggregateExprList.size(); ++i) {
                FunctionCallExpr callExpr = aggregateExprList.get(i);
                if (callExpr.isDistinct()) {
                    ++singleDistinctCount;
                    functionCallExpr = callExpr;
                    singleDistinctIndex = i;
                }
            }
            if (singleDistinctCount == 1) {
                FunctionCallExpr replaceExpr = null;
                final String functionName = functionCallExpr.getFnName().getFunction();
                if (functionName.equalsIgnoreCase(FunctionSet.COUNT)) {
                    replaceExpr = new FunctionCallExpr("MULTI_DISTINCT_COUNT", functionCallExpr.getParams());
                    replaceExpr.setFn(Expr.getBuiltinFunction("MULTI_DISTINCT_COUNT",
                            new Type[] {functionCallExpr.getChild(0).getType()},
                            IS_NONSTRICT_SUPERTYPE_OF));
                    replaceExpr.getParams().setIsDistinct(false);
                } else if (functionName.equalsIgnoreCase("SUM")) {
                    replaceExpr = new FunctionCallExpr("MULTI_DISTINCT_SUM", functionCallExpr.getParams());
                    replaceExpr.setFn(Expr.getBuiltinFunction("MULTI_DISTINCT_SUM",
                            new Type[] {functionCallExpr.getChild(0).getType()},
                            IS_NONSTRICT_SUPERTYPE_OF));
                    replaceExpr.getParams().setIsDistinct(false);
                }
                Preconditions.checkState(replaceExpr != null);
                replaceExpr.analyzeNoThrow(analyzer);

                aggregateExprList.set(singleDistinctIndex, replaceExpr);
            }
        }

        @Override
        public PlanFragment visitPhysicalDistribution(OptExpression optExpr, ExecPlan context) {
            PlanFragment inputFragment = visit(optExpr.inputAt(0), context);
            PhysicalDistributionOperator distribution = (PhysicalDistributionOperator) optExpr.getOp();

            ExchangeNode exchangeNode = new ExchangeNode(context.getPlanCtx().getNextNodeId(),
                    inputFragment.getPlanRoot(), false, distribution.getDistributionSpec().getType());

            DataPartition dataPartition;
            if (DistributionSpec.DistributionType.GATHER.equals(distribution.getDistributionSpec().getType())) {
                exchangeNode.setNumInstances(1);
                dataPartition = DataPartition.UNPARTITIONED;
                GatherDistributionSpec spec = (GatherDistributionSpec) distribution.getDistributionSpec();
                if (spec.hasLimit()) {
                    exchangeNode.setLimit(spec.getLimit());
                }
            } else if (DistributionSpec.DistributionType.BROADCAST
                    .equals(distribution.getDistributionSpec().getType())) {
                exchangeNode.setNumInstances(inputFragment.getPlanRoot().getNumInstances());
                dataPartition = DataPartition.UNPARTITIONED;
            } else if (DistributionSpec.DistributionType.SHUFFLE.equals(distribution.getDistributionSpec().getType())) {
                exchangeNode.setNumInstances(inputFragment.getPlanRoot().getNumInstances());
                List<Integer> columnRefSet =
                        ((HashDistributionSpec) distribution.getDistributionSpec()).getHashDistributionDesc()
                                .getColumns();
                Preconditions.checkState(!columnRefSet.isEmpty());
                List<ColumnRefOperator> partitionColumns = new ArrayList<>();
                for (int columnId : columnRefSet) {
                    partitionColumns.add(columnRefFactory.getColumnRef(columnId));
                }
                List<Expr> distributeExpressions =
                        partitionColumns.stream().map(e -> ScalarOperatorToExpr.buildExecExpression(e,
                                new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())))
                                .collect(Collectors.toList());
                dataPartition = DataPartition.hashPartitioned(distributeExpressions);
            } else {
                throw new StarRocksPlannerException("Unsupport exchange type : "
                        + distribution.getDistributionSpec().getType(), INTERNAL_ERROR);
            }

            PlanFragment fragment =
                    new PlanFragment(context.getPlanCtx().getNextFragmentId(), exchangeNode, dataPartition);
            inputFragment.setDestination(exchangeNode);
            inputFragment.setOutputPartition(dataPartition);

            context.getFragments().add(fragment);
            return fragment;
        }

        @Override
        public PlanFragment visitPhysicalTopN(OptExpression optExpr, ExecPlan context) {
            PlanFragment inputFragment = visit(optExpr.inputAt(0), context);
            PhysicalTopNOperator topN = (PhysicalTopNOperator) optExpr.getOp();
            if (!topN.isSplit()) {
                return buildPartialTopNFragment(optExpr, context, topN.getOrderSpec(), topN.getLimit(),
                        topN.getOffset(),
                        inputFragment);
            } else {
                return buildFinalTopNFragment(context, topN.getLimit(), topN.getOffset(), inputFragment, optExpr);
            }
        }

        private PlanFragment buildFinalTopNFragment(ExecPlan context, long limit, long offset,
                                                    PlanFragment inputFragment,
                                                    OptExpression optExpr) {
            ExchangeNode exchangeNode = new ExchangeNode(context.getPlanCtx().getNextNodeId(),
                    inputFragment.getPlanRoot(), false,
                    DistributionSpec.DistributionType.GATHER);

            exchangeNode.setNumInstances(1);
            DataPartition dataPartition = DataPartition.UNPARTITIONED;

            Preconditions.checkState(inputFragment.getPlanRoot() instanceof SortNode);
            SortNode sortNode = (SortNode) inputFragment.getPlanRoot();
            exchangeNode.setMergeInfo(sortNode.getSortInfo(), offset);
            exchangeNode.computeStatistics(optExpr.getStatistics());
            exchangeNode.setLimit(limit);

            PlanFragment fragment =
                    new PlanFragment(context.getPlanCtx().getNextFragmentId(), exchangeNode, dataPartition);
            inputFragment.setDestination(exchangeNode);
            inputFragment.setOutputPartition(dataPartition);

            context.getFragments().add(fragment);
            return fragment;
        }

        private PlanFragment buildPartialTopNFragment(OptExpression optExpr, ExecPlan context,
                                                      OrderSpec orderSpec, long limit, long offset,
                                                      PlanFragment inputFragment) {
            List<Expr> resolvedTupleExprs = new ArrayList<>();
            List<Expr> sortExprs = new ArrayList<>();
            TupleDescriptor sortTuple = context.getDescTbl().createTupleDescriptor();

            for (Ordering ordering : orderSpec.getOrderDescs()) {
                Expr sortExpr = ScalarOperatorToExpr.buildExecExpression(ordering.getColumnRef(),
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr()));

                SlotDescriptor slotDesc =
                        context.getDescTbl().addSlotDescriptor(sortTuple, new SlotId(ordering.getColumnRef().getId()));
                slotDesc.initFromExpr(sortExpr);
                slotDesc.setIsMaterialized(true);
                slotDesc.setIsNullable(sortExpr.isNullable());
                slotDesc.setType(sortExpr.getType());

                context.getColRefToExpr()
                        .put(ordering.getColumnRef(), new SlotRef(ordering.getColumnRef().toString(), slotDesc));
                resolvedTupleExprs.add(sortExpr);
                sortExprs.add(new SlotRef(slotDesc));
            }

            ColumnRefSet columnRefSet = optExpr.getLogicalProperty().getOutputColumns();
            for (int i = 0; i < columnRefSet.getColumnIds().length; ++i) {
                /*
                 * Add column not be used in ordering
                 */
                ColumnRefOperator columnRef = columnRefFactory.getColumnRef(columnRefSet.getColumnIds()[i]);
                if (orderSpec.getOrderDescs().stream().map(Ordering::getColumnRef)
                        .noneMatch(c -> c.equals(columnRef))) {
                    Expr outputExpr = ScalarOperatorToExpr.buildExecExpression(columnRef,
                            new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr()));

                    SlotDescriptor slotDesc =
                            context.getDescTbl().addSlotDescriptor(sortTuple, new SlotId(columnRef.getId()));
                    slotDesc.initFromExpr(outputExpr);
                    slotDesc.setIsMaterialized(true);
                    slotDesc.setIsNullable(outputExpr.isNullable());
                    slotDesc.setType(outputExpr.getType());

                    context.getColRefToExpr().put(columnRef, new SlotRef(columnRef.toString(), slotDesc));
                    resolvedTupleExprs.add(outputExpr);
                }
            }

            sortTuple.computeMemLayout();
            SortInfo sortInfo = new SortInfo(
                    sortExprs,
                    orderSpec.getOrderDescs().stream().map(Ordering::isAscending).collect(Collectors.toList()),
                    orderSpec.getOrderDescs().stream().map(Ordering::isNullsFirst).collect(Collectors.toList()));
            sortInfo.setMaterializedTupleInfo(sortTuple, resolvedTupleExprs);

            SortNode sortNode = new SortNode(
                    context.getPlanCtx().getNextNodeId(),
                    inputFragment.getPlanRoot(),
                    sortInfo,
                    limit != -1,
                    limit == -1,
                    0);
            sortNode.setLimit(limit);
            sortNode.setOffset(offset);
            sortNode.resolvedTupleExprs = resolvedTupleExprs;
            sortNode.setHasNullableGenerateChild();
            sortNode.computeStatistics(optExpr.getStatistics());

            inputFragment.setPlanRoot(sortNode);
            return inputFragment;
        }

        private void setJoinPushDown(HashJoinNode node) {
            // Push down the predicates constructed by the right child when the
            // join op is inner join or left semi join or right join(semi, outer, anti)
            if (ConnectContext.get().getSessionVariable().isHashJoinPushDownRightTable()
                    && (node.getJoinOp().isInnerJoin() || node.getJoinOp().isLeftSemiJoin() ||
                    node.getJoinOp().isRightJoin())) {
                node.setIsPushDown(true);
            } else {
                node.setIsPushDown(false);
            }
        }

        @Override
        public PlanFragment visitPhysicalHashJoin(OptExpression optExpr, ExecPlan context) {
            PlanFragment leftFragment = visit(optExpr.inputAt(0), context);
            PlanFragment rightFragment = visit(optExpr.inputAt(1), context);
            PhysicalHashJoinOperator node = (PhysicalHashJoinOperator) optExpr.getOp();

            ColumnRefSet leftChildColumns = optExpr.inputAt(0).getLogicalProperty().getOutputColumns();
            ColumnRefSet rightChildColumns = optExpr.inputAt(1).getLogicalProperty().getOutputColumns();

            // 2. Get eqJoinConjuncts
            List<BinaryPredicateOperator> eqOnPredicates = getEqConj(
                    leftChildColumns,
                    rightChildColumns,
                    Utils.extractConjuncts(node.getJoinPredicate()));

            if (node.getJoinType().isCrossJoin() ||
                    (node.getJoinType().isInnerJoin() && eqOnPredicates.isEmpty())) {
                CrossJoinNode joinNode = new CrossJoinNode(context.getPlanCtx().getNextNodeId(),
                        leftFragment.getPlanRoot(),
                        rightFragment.getPlanRoot(),
                        null);
                joinNode.setLimit(node.getLimit());
                joinNode.computeStatistics(optExpr.getStatistics());
                List<Expr> conjuncts = Utils.extractConjuncts(node.getPredicate()).stream()
                        .map(e -> ScalarOperatorToExpr.buildExecExpression(node.getPredicate(),
                                new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())))
                        .collect(Collectors.toList());
                joinNode.addConjuncts(conjuncts);
                List<Expr> onConjuncts = Utils.extractConjuncts(node.getJoinPredicate()).stream()
                        .map(e -> ScalarOperatorToExpr.buildExecExpression(node.getJoinPredicate(),
                                new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())))
                        .collect(Collectors.toList());
                joinNode.addConjuncts(onConjuncts);
                // Connect parent and child fragment
                rightFragment.getPlanRoot().setFragment(leftFragment);

                // Currently, we always generate new fragment for PhysicalDistribution.
                // So we need to remove exchange node only fragment for Join.
                context.getFragments().remove(rightFragment);

                // Move leftFragment to end, it depends on all of its children
                context.getFragments().remove(leftFragment);
                context.getFragments().add(leftFragment);

                leftFragment.setPlanRoot(joinNode);
                if (!rightFragment.getChildren().isEmpty()) {
                    // right table isn't value operator
                    leftFragment.addChild(rightFragment.getChild(0));
                }

                if (!(joinNode.getChild(1) instanceof ExchangeNode)) {
                    joinNode.setReplicated(true);
                }

                return leftFragment;
            } else {
                JoinOperator joinOperator = node.getJoinType();

                // 1. Get distributionMode
                HashJoinNode.DistributionMode distributionMode;
                if (leftFragment.getPlanRoot() instanceof ExchangeNode &&
                        ((ExchangeNode) leftFragment.getPlanRoot()).getDistributionType()
                                .equals(DistributionSpec.DistributionType.SHUFFLE) &&
                        rightFragment.getPlanRoot() instanceof ExchangeNode &&
                        ((ExchangeNode) rightFragment.getPlanRoot()).getDistributionType()
                                .equals(DistributionSpec.DistributionType.SHUFFLE)) {
                    distributionMode = HashJoinNode.DistributionMode.PARTITIONED;
                } else if (rightFragment.getPlanRoot() instanceof ExchangeNode &&
                        ((ExchangeNode) rightFragment.getPlanRoot()).getDistributionType()
                                .equals(DistributionSpec.DistributionType.BROADCAST)) {
                    distributionMode = HashJoinNode.DistributionMode.BROADCAST;
                } else if (!(leftFragment.getPlanRoot() instanceof ExchangeNode) &&
                        !(rightFragment.getPlanRoot() instanceof ExchangeNode)) {
                    if (ConnectContext.get().getSessionVariable().isEnableReplicationJoin() &&
                            rightFragment.getPlanRoot().canDoReplicatedJoin()) {
                        distributionMode = HashJoinNode.DistributionMode.REPLICATED;
                    } else {
                        distributionMode = HashJoinNode.DistributionMode.COLOCATE;
                    }
                } else if (isShuffleHashBucket(leftFragment.getPlanRoot(), rightFragment.getPlanRoot())) {
                    distributionMode = HashJoinNode.DistributionMode.SHUFFLE_HASH_BUCKET;
                } else {
                    distributionMode = HashJoinNode.DistributionMode.LOCAL_HASH_BUCKET;
                }

                for (BinaryPredicateOperator s : eqOnPredicates) {
                    if (!optExpr.inputAt(0).getLogicalProperty().getOutputColumns()
                            .contains(s.getChild(0).getUsedColumns())) {
                        s.swap();
                    }
                }

                List<Expr> eqJoinConjuncts =
                        eqOnPredicates.stream().map(e -> ScalarOperatorToExpr.buildExecExpression(e,
                                new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())))
                                .collect(Collectors.toList());

                for (Expr expr : eqJoinConjuncts) {
                    if (expr.isConstant()) {
                        throw unsupportedException("Support join on constant predicate later");
                    }
                }

                List<ScalarOperator> otherJoin = Utils.extractConjuncts(node.getJoinPredicate());
                otherJoin.removeAll(eqOnPredicates);
                List<Expr> otherJoinConjuncts = otherJoin.stream().map(e -> ScalarOperatorToExpr.buildExecExpression(e,
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())))
                        .collect(Collectors.toList());

                // 3. Get conjuncts
                List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
                List<Expr> conjuncts = predicates.stream().map(e -> ScalarOperatorToExpr.buildExecExpression(e,
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())))
                        .collect(Collectors.toList());

                if (joinOperator.isLeftOuterJoin()) {
                    for (TupleId tupleId : rightFragment.getPlanRoot().getTupleIds()) {
                        context.getDescTbl().getTupleDesc(tupleId).getSlots().forEach(slot -> slot.setIsNullable(true));
                    }
                } else if (joinOperator.isRightOuterJoin()) {
                    for (TupleId tupleId : leftFragment.getPlanRoot().getTupleIds()) {
                        context.getDescTbl().getTupleDesc(tupleId).getSlots().forEach(slot -> slot.setIsNullable(true));
                    }
                } else if (joinOperator.isFullOuterJoin()) {
                    for (TupleId tupleId : leftFragment.getPlanRoot().getTupleIds()) {
                        context.getDescTbl().getTupleDesc(tupleId).getSlots().forEach(slot -> slot.setIsNullable(true));
                    }
                    for (TupleId tupleId : rightFragment.getPlanRoot().getTupleIds()) {
                        context.getDescTbl().getTupleDesc(tupleId).getSlots().forEach(slot -> slot.setIsNullable(true));
                    }
                }

                HashJoinNode hashJoinNode = new HashJoinNode(
                        context.getPlanCtx().getNextNodeId(),
                        leftFragment.getPlanRoot(), rightFragment.getPlanRoot(),
                        joinOperator, eqJoinConjuncts, otherJoinConjuncts);
                hashJoinNode.setDistributionMode(distributionMode);
                hashJoinNode.getConjuncts().addAll(conjuncts);
                hashJoinNode.setLimit(node.getLimit());
                hashJoinNode.computeStatistics(optExpr.getStatistics());

                if (ConnectContext.get().getSessionVariable().getEnableGlobalRuntimeFilter()) {
                    hashJoinNode.buildRuntimeFilters(runtimeFilterIdIdGenerator, hashJoinNode.getChild(1),
                            hashJoinNode.getEqJoinConjuncts(), joinOperator);
                }

                if (distributionMode.equals(HashJoinNode.DistributionMode.BROADCAST)) {
                    setJoinPushDown(hashJoinNode);

                    // Connect parent and child fragment
                    rightFragment.getPlanRoot().setFragment(leftFragment);

                    // Currently, we always generate new fragment for PhysicalDistribution.
                    // So we need to remove exchange node only fragment for Join.
                    context.getFragments().remove(rightFragment);

                    // Move leftFragment to end, it depends on all of its children
                    context.getFragments().remove(leftFragment);
                    context.getFragments().add(leftFragment);
                    leftFragment.setPlanRoot(hashJoinNode);
                    leftFragment.addChild(rightFragment.getChild(0));
                    return leftFragment;
                } else if (distributionMode.equals(HashJoinNode.DistributionMode.PARTITIONED)) {
                    List<Integer> leftOnPredicateColumns = new ArrayList<>();
                    List<Integer> rightOnPredicateColumns = new ArrayList<>();
                    JoinPredicateUtils.getJoinOnPredicatesColumns(eqOnPredicates, leftChildColumns, rightChildColumns,
                            leftOnPredicateColumns, rightOnPredicateColumns);

                    List<ScalarOperator> leftPredicates = leftOnPredicateColumns.stream()
                            .map(columnRefFactory::getColumnRef).collect(Collectors.toList());
                    List<Expr> leftJoinExprs =
                            leftPredicates.stream().map(e -> ScalarOperatorToExpr.buildExecExpression(e,
                                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())))
                                    .collect(Collectors.toList());

                    List<ScalarOperator> rightPredicates = rightOnPredicateColumns.stream()
                            .map(columnRefFactory::getColumnRef).collect(Collectors.toList());
                    List<Expr> rightJoinExprs =
                            rightPredicates.stream().map(e -> ScalarOperatorToExpr.buildExecExpression(e,
                                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())))
                                    .collect(Collectors.toList());

                    DataPartition lhsJoinPartition = new DataPartition(TPartitionType.HASH_PARTITIONED,
                            Expr.cloneList(leftJoinExprs, null));
                    DataPartition rhsJoinPartition =
                            new DataPartition(TPartitionType.HASH_PARTITIONED, rightJoinExprs);

                    leftFragment.getChild(0).setOutputPartition(lhsJoinPartition);
                    rightFragment.getChild(0).setOutputPartition(rhsJoinPartition);

                    // Currently, we always generate new fragment for PhysicalDistribution.
                    // So we need to remove exchange node only fragment for Join.
                    context.getFragments().remove(leftFragment);
                    context.getFragments().remove(rightFragment);

                    PlanFragment joinFragment = new PlanFragment(context.getPlanCtx().getNextFragmentId(),
                            hashJoinNode, lhsJoinPartition);
                    joinFragment.addChild(leftFragment.getChild(0));
                    joinFragment.addChild(rightFragment.getChild(0));

                    context.getFragments().add(joinFragment);

                    return joinFragment;
                } else if (distributionMode.equals(HashJoinNode.DistributionMode.COLOCATE) ||
                        distributionMode.equals(HashJoinNode.DistributionMode.REPLICATED)) {
                    if (distributionMode.equals(HashJoinNode.DistributionMode.COLOCATE)) {
                        hashJoinNode.setColocate(true, "");
                    } else {
                        hashJoinNode.setReplicated(true);
                    }
                    setJoinPushDown(hashJoinNode);

                    hashJoinNode.setChild(0, leftFragment.getPlanRoot());
                    hashJoinNode.setChild(1, rightFragment.getPlanRoot());
                    leftFragment.setPlanRoot(hashJoinNode);
                    context.getFragments().remove(rightFragment);

                    context.getFragments().remove(leftFragment);
                    context.getFragments().add(leftFragment);
                    return leftFragment;
                } else if (distributionMode.equals(HashJoinNode.DistributionMode.SHUFFLE_HASH_BUCKET)) {
                    List<Integer> leftOnPredicateColumns = new ArrayList<>();
                    List<Integer> rightOnPredicateColumns = new ArrayList<>();
                    JoinPredicateUtils.getJoinOnPredicatesColumns(eqOnPredicates, leftChildColumns, rightChildColumns,
                            leftOnPredicateColumns, rightOnPredicateColumns);
                    setJoinPushDown(hashJoinNode);

                    // distributionMode is SHUFFLE_HASH_BUCKET
                    if (leftFragment.getPlanRoot() instanceof ExchangeNode &&
                            !(rightFragment.getPlanRoot() instanceof ExchangeNode)) {
                        return computeRunTimeBucketShufflePlanFragment(context, leftOnPredicateColumns, rightFragment,
                                leftFragment, hashJoinNode);
                    } else {
                        return computeRunTimeBucketShufflePlanFragment(context, rightOnPredicateColumns, leftFragment,
                                rightFragment, hashJoinNode);
                    }
                } else {
                    List<Integer> leftOnPredicateColumns = new ArrayList<>();
                    List<Integer> rightOnPredicateColumns = new ArrayList<>();
                    JoinPredicateUtils.getJoinOnPredicatesColumns(eqOnPredicates, leftChildColumns, rightChildColumns,
                            leftOnPredicateColumns, rightOnPredicateColumns);
                    setJoinPushDown(hashJoinNode);

                    // distributionMode is BUCKET_SHUFFLE
                    if (leftFragment.getPlanRoot() instanceof ExchangeNode &&
                            !(rightFragment.getPlanRoot() instanceof ExchangeNode)) {
                        return computeBucketShufflePlanFragment(context, leftOnPredicateColumns, rightFragment,
                                leftFragment, hashJoinNode);
                    } else {
                        return computeBucketShufflePlanFragment(context, rightOnPredicateColumns, leftFragment,
                                rightFragment, hashJoinNode);
                    }
                }
            }
        }

        public boolean isShuffleJoin(HashJoinNode node) {
            if (node.getChild(0) instanceof ExchangeNode && ((ExchangeNode) node.getChild(0)).getDistributionType()
                    .equals(DistributionSpec.DistributionType.SHUFFLE) &&
                    node.getChild(1) instanceof ExchangeNode && ((ExchangeNode) node.getChild(1)).getDistributionType()
                    .equals(DistributionSpec.DistributionType.SHUFFLE)) {
                return true;
            }
            return false;
        }

        public boolean isShuffleHashBucket(PlanNode left, PlanNode right) {
            if (left instanceof ProjectNode) {
                return isShuffleHashBucket(left.getChild(0), right);
            }
            if (left instanceof HashJoinNode) {
                HashJoinNode hashJoinNode = (HashJoinNode) left;
                if (hashJoinNode.isLocalHashBucket()) {
                    return false;
                }
                if (hashJoinNode.isShuffleHashBucket() || isShuffleJoin(hashJoinNode)) {
                    return true;
                }
            }
            // left is not hashJoinNode
            if (right instanceof ProjectNode) {
                return isShuffleHashBucket(left, right.getChild(0));
            }
            if (right instanceof HashJoinNode) {
                return true;
            }
            return false;
        }

        public PlanFragment computeBucketShufflePlanFragment(ExecPlan context, List<Integer> columns,
                                                             PlanFragment stayFragment,
                                                             PlanFragment removeFragment, HashJoinNode hashJoinNode) {
            hashJoinNode.setLocalHashBucket(true);
            removeFragment.getChild(0)
                    .setOutputPartition(new DataPartition(TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED,
                            removeFragment.getDataPartition().getPartitionExprs()));

            // Currently, we always generate new fragment for PhysicalDistribution.
            // So we need to remove exchange node only fragment for Join.
            context.getFragments().remove(removeFragment);

            context.getFragments().remove(stayFragment);
            context.getFragments().add(stayFragment);

            stayFragment.setPlanRoot(hashJoinNode);
            stayFragment.addChild(removeFragment.getChild(0));
            return stayFragment;
        }

        public PlanFragment computeRunTimeBucketShufflePlanFragment(ExecPlan context, List<Integer> columns,
                                                                    PlanFragment stayFragment,
                                                                    PlanFragment removeFragment,
                                                                    HashJoinNode hashJoinNode) {
            hashJoinNode.setShuffleHashBucket(true);
            removeFragment.getChild(0)
                    .setOutputPartition(new DataPartition(TPartitionType.HASH_PARTITIONED,
                            removeFragment.getDataPartition().getPartitionExprs()));

            // Currently, we always generate new fragment for PhysicalDistribution.
            // So we need to remove exchange node only fragment for Join.
            context.getFragments().remove(removeFragment);

            context.getFragments().remove(stayFragment);
            context.getFragments().add(stayFragment);

            stayFragment.setPlanRoot(hashJoinNode);
            stayFragment.addChild(removeFragment.getChild(0));
            return stayFragment;
        }

        @Override
        public PlanFragment visitPhysicalAssertOneRow(OptExpression optExpression, ExecPlan context) {
            PlanFragment inputFragment = visit(optExpression.inputAt(0), context);

            PhysicalAssertOneRowOperator assertOneRow = (PhysicalAssertOneRowOperator) optExpression.getOp();
            AssertNumRowsNode node =
                    new AssertNumRowsNode(context.getPlanCtx().getNextNodeId(), inputFragment.getPlanRoot(),
                            new AssertNumRowsElement(assertOneRow.getCheckRows(), assertOneRow.getTips(),
                                    assertOneRow.getAssertion()));
            node.computeStatistics(optExpression.getStatistics());
            inputFragment.setPlanRoot(node);
            return inputFragment;
        }

        @Override
        public PlanFragment visitPhysicalAnalytic(OptExpression optExpr, ExecPlan context) {
            PlanFragment inputFragment = visit(optExpr.inputAt(0), context);
            PhysicalWindowOperator node = (PhysicalWindowOperator) optExpr.getOp();

            List<Expr> analyticFnCalls = new ArrayList<>();
            TupleDescriptor outputTupleDesc = context.getDescTbl().createTupleDescriptor();
            for (Map.Entry<ColumnRefOperator, CallOperator> analyticCall : node.getAnalyticCall().entrySet()) {
                Expr analyticFunction = ScalarOperatorToExpr.buildExecExpression(analyticCall.getValue(),
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr()));
                analyticFnCalls.add(analyticFunction);

                SlotDescriptor slotDesc = context.getDescTbl()
                        .addSlotDescriptor(outputTupleDesc, new SlotId(analyticCall.getKey().getId()));
                slotDesc.setType(analyticFunction.getType());
                slotDesc.setIsNullable(analyticFunction.isNullable());
                slotDesc.setIsMaterialized(true);
                context.getColRefToExpr()
                        .put(analyticCall.getKey(), new SlotRef(analyticCall.getKey().toString(), slotDesc));
            }

            List<Expr> partitionExprs =
                    node.getPartitionExpressions().stream().map(e -> ScalarOperatorToExpr.buildExecExpression(e,
                            new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())))
                            .collect(Collectors.toList());

            List<OrderByElement> orderByElements = node.getOrderByElements().stream().map(e -> new OrderByElement(
                    ScalarOperatorToExpr.buildExecExpression(e.getColumnRef(),
                            new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())),
                    e.isAscending(), e.isNullsFirst())).collect(Collectors.toList());

            AnalyticEvalNode analyticEvalNode = new AnalyticEvalNode(
                    context.getPlanCtx().getNextNodeId(),
                    inputFragment.getPlanRoot(),
                    analyticFnCalls,
                    partitionExprs,
                    orderByElements,
                    node.getAnalyticWindow(),
                    null, outputTupleDesc,
                    null, null, null,
                    context.getDescTbl().createTupleDescriptor());
            analyticEvalNode.setSubstitutedPartitionExprs(partitionExprs);
            analyticEvalNode.setLimit(node.getLimit());
            analyticEvalNode.setHasNullableGenerateChild();
            analyticEvalNode.computeStatistics(optExpr.getStatistics());

            // set predicate
            List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
            for (ScalarOperator predicate : predicates) {
                analyticEvalNode.getConjuncts()
                        .add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
            }

            inputFragment.setPlanRoot(analyticEvalNode);
            return inputFragment;
        }

        private PlanFragment buildSetOperation(OptExpression optExpr, ExecPlan context, OperatorType operatorType) {
            PhysicalSetOperation setOperation = (PhysicalSetOperation) optExpr.getOp();
            TupleDescriptor setOperationTuple = context.getDescTbl().createTupleDescriptor();

            for (ColumnRefOperator columnRefOperator : setOperation.getOutputColumnRefOp()) {
                SlotDescriptor slotDesc = context.getDescTbl()
                        .addSlotDescriptor(setOperationTuple, new SlotId(columnRefOperator.getId()));
                slotDesc.setType(columnRefOperator.getType());
                slotDesc.setIsMaterialized(true);
                slotDesc.setIsNullable(columnRefOperator.isNullable());

                context.getColRefToExpr().put(columnRefOperator, new SlotRef(columnRefOperator.toString(), slotDesc));
            }

            SetOperationNode setOperationNode;
            boolean isUnionAll = false;
            if (operatorType.equals(OperatorType.PHYSICAL_UNION)) {
                setOperationNode = new UnionNode(context.getPlanCtx().getNextNodeId(), setOperationTuple.getId());
                isUnionAll = ((PhysicalUnionOperator) setOperation).isUnionAll();
                setOperationNode.setFirstMaterializedChildIdx_(optExpr.arity());

                List<Map<Integer, Integer>> passThroughSlotMaps = new ArrayList<>();
                for (int childIdx = 0; childIdx < optExpr.arity(); ++childIdx) {
                    Map<Integer, Integer> passThroughMap = new HashMap<>();
                    List<ColumnRefOperator> childOutput = setOperation.getChildOutputColumns().get(childIdx);
                    Preconditions.checkState(childOutput.size() == setOperation.getOutputColumnRefOp().size());
                    for (int columnIdx = 0; columnIdx < setOperation.getOutputColumnRefOp().size(); ++columnIdx) {
                        Integer resultColumnIdx = setOperation.getOutputColumnRefOp().get(columnIdx).getId();
                        passThroughMap.put(resultColumnIdx, childOutput.get(columnIdx).getId());
                    }
                    passThroughSlotMaps.add(passThroughMap);
                    Preconditions.checkState(passThroughMap.size() == setOperation.getOutputColumnRefOp().size());
                }
                setOperationNode.setPassThroughSlotMaps(passThroughSlotMaps);
            } else if (operatorType.equals(OperatorType.PHYSICAL_EXCEPT)) {
                setOperationNode = new ExceptNode(context.getPlanCtx().getNextNodeId(), setOperationTuple.getId());
            } else if (operatorType.equals(OperatorType.PHYSICAL_INTERSECT)) {
                setOperationNode = new IntersectNode(context.getPlanCtx().getNextNodeId(), setOperationTuple.getId());
            } else {
                throw new StarRocksPlannerException("Unsupported set operation", INTERNAL_ERROR);
            }

            Preconditions.checkState(optExpr.getInputs().size() == setOperation.getChildOutputColumns().size());

            PlanFragment setOperationFragment =
                    new PlanFragment(context.getPlanCtx().getNextFragmentId(), setOperationNode, DataPartition.RANDOM);
            List<List<Expr>> materializedResultExprLists = Lists.newArrayList();

            for (int i = 0; i < optExpr.getInputs().size(); i++) {
                List<ColumnRefOperator> childOutput = setOperation.getChildOutputColumns().get(i);
                PlanFragment fragment = visit(optExpr.getInputs().get(i), context);

                List<Expr> materializedExpressions = Lists.newArrayList();

                // keep output column order
                for (ColumnRefOperator ref : childOutput) {
                    SlotDescriptor slotDescriptor = context.getDescTbl().getSlotDesc(new SlotId(ref.getId()));
                    materializedExpressions.add(new SlotRef(slotDescriptor));
                }

                materializedResultExprLists.add(materializedExpressions);

                if (isUnionAll) {
                    fragment.setOutputPartition(DataPartition.RANDOM);
                } else {
                    fragment.setOutputPartition(DataPartition.hashPartitioned(materializedExpressions));
                }

                // nothing distribute can satisfy set-operator, must shuffle data
                ExchangeNode exchangeNode = new ExchangeNode(context.getPlanCtx().getNextNodeId(),
                        fragment.getPlanRoot(), false);

                exchangeNode.setFragment(setOperationFragment);
                fragment.setDestination(exchangeNode);
                setOperationNode.addChild(exchangeNode);
            }

            // reset column is nullable, for handle union select xx join select xxx...
            setOperationNode.setHasNullableGenerateChild();
            for (ColumnRefOperator columnRefOperator : setOperation.getOutputColumnRefOp()) {
                SlotDescriptor slotDesc = context.getDescTbl().getSlotDesc(new SlotId(columnRefOperator.getId()));
                slotDesc.setIsNullable(slotDesc.getIsNullable() | setOperationNode.isHasNullableGenerateChild());
            }
            setOperationTuple.computeMemLayout();

            setOperationNode.setMaterializedResultExprLists_(materializedResultExprLists);
            setOperationNode.setLimit(setOperation.getLimit());
            setOperationNode.computeStatistics(optExpr.getStatistics());

            context.getFragments().add(setOperationFragment);
            return setOperationFragment;
        }

        @Override
        public PlanFragment visitPhysicalUnion(OptExpression optExpr, ExecPlan context) {
            return buildSetOperation(optExpr, context, OperatorType.PHYSICAL_UNION);
        }

        @Override
        public PlanFragment visitPhysicalExcept(OptExpression optExpr, ExecPlan context) {
            return buildSetOperation(optExpr, context, OperatorType.PHYSICAL_EXCEPT);
        }

        @Override
        public PlanFragment visitPhysicalIntersect(OptExpression optExpr, ExecPlan context) {
            return buildSetOperation(optExpr, context, OperatorType.PHYSICAL_INTERSECT);
        }

        @Override
        public PlanFragment visitPhysicalRepeat(OptExpression optExpr, ExecPlan context) {
            PlanFragment inputFragment = visit(optExpr.inputAt(0), context);
            PhysicalRepeatOperator repeatOperator = (PhysicalRepeatOperator) optExpr.getOp();

            TupleDescriptor outputGroupingTuple = context.getDescTbl().createTupleDescriptor();
            for (ColumnRefOperator columnRefOperator : repeatOperator.getOutputGrouping()) {
                SlotDescriptor slotDesc = context.getDescTbl()
                        .addSlotDescriptor(outputGroupingTuple, new SlotId(columnRefOperator.getId()));
                slotDesc.setType(columnRefOperator.getType());
                slotDesc.setIsMaterialized(true);
                slotDesc.setIsNullable(columnRefOperator.isNullable());

                context.getColRefToExpr().put(columnRefOperator, new SlotRef(columnRefOperator.toString(), slotDesc));
            }
            outputGroupingTuple.computeMemLayout();

            //RepeatSlotIdList
            List<Set<Integer>> repeatSlotIdList = new ArrayList<>();
            for (Set<ColumnRefOperator> repeat : repeatOperator.getRepeatColumnRef()) {
                repeatSlotIdList.add(
                        repeat.stream().map(ColumnRefOperator::getId).collect(Collectors.toSet()));
            }

            RepeatNode repeatNode = new RepeatNode(
                    context.getPlanCtx().getNextNodeId(),
                    inputFragment.getPlanRoot(),
                    outputGroupingTuple,
                    repeatSlotIdList,
                    repeatOperator.getGroupingIds());
            repeatNode.computeStatistics(optExpr.getStatistics());

            inputFragment.setPlanRoot(repeatNode);
            return inputFragment;
        }

        @Override
        public PlanFragment visitPhysicalFilter(OptExpression optExpr, ExecPlan context) {
            PlanFragment inputFragment = visit(optExpr.inputAt(0), context);
            PhysicalFilterOperator filter = (PhysicalFilterOperator) optExpr.getOp();

            List<Expr> predicates = Utils.extractConjuncts(filter.getPredicate()).stream()
                    .map(d -> ScalarOperatorToExpr.buildExecExpression(d,
                            new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())))
                    .collect(Collectors.toList());

            SelectNode selectNode =
                    new SelectNode(context.getPlanCtx().getNextNodeId(), inputFragment.getPlanRoot(), predicates);
            selectNode.setLimit(filter.getLimit());
            selectNode.computeStatistics(optExpr.getStatistics());
            inputFragment.setPlanRoot(selectNode);
            return inputFragment;
        }

        @Override
        public PlanFragment visitPhysicalTableFunction(OptExpression optExpression, ExecPlan context) {
            PlanFragment inputFragment = visit(optExpression.inputAt(0), context);
            PhysicalTableFunctionOperator physicalTableFunction = (PhysicalTableFunctionOperator) optExpression.getOp();

            TupleDescriptor udtfOutputTuple = context.getDescTbl().createTupleDescriptor();
            for (int columnId : physicalTableFunction.getOutputColumns().getColumnIds()) {
                ColumnRefOperator columnRefOperator = columnRefFactory.getColumnRef(columnId);

                SlotDescriptor slotDesc =
                        context.getDescTbl().addSlotDescriptor(udtfOutputTuple, new SlotId(columnRefOperator.getId()));
                slotDesc.setType(columnRefOperator.getType());
                slotDesc.setIsMaterialized(true);
                slotDesc.setIsNullable(columnRefOperator.isNullable());

                context.getColRefToExpr().put(columnRefOperator, new SlotRef(columnRefOperator.toString(), slotDesc));
            }
            udtfOutputTuple.computeMemLayout();

            TableFunctionNode tableFunctionNode = new TableFunctionNode(context.getPlanCtx().getNextNodeId(),
                    inputFragment.getPlanRoot(),
                    udtfOutputTuple,
                    physicalTableFunction.getFn(),
                    Arrays.stream(physicalTableFunction.getParamColumnRefs().getColumnIds()).boxed()
                            .collect(Collectors.toList()),
                    Arrays.stream(physicalTableFunction.getOuterColumnRefSet().getColumnIds()).boxed()
                            .collect(Collectors.toList()),
                    Arrays.stream(physicalTableFunction.getFnResultColumnRefSet().getColumnIds()).boxed()
                            .collect(Collectors.toList()));
            tableFunctionNode.setLimit(physicalTableFunction.getLimit());
            inputFragment.setPlanRoot(tableFunctionNode);
            return inputFragment;
        }
    }
}
