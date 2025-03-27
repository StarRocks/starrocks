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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.AggregateInfo;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.SortInfo;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.catalog.system.information.FeMetricsSystemTable;
import com.starrocks.catalog.system.information.LoadTrackingLogsSystemTable;
import com.starrocks.catalog.system.information.LoadsSystemTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.IdGenerator;
import com.starrocks.common.LocalExchangerType;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.metadata.MetadataTable;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.planner.AggregationNode;
import com.starrocks.planner.AnalyticEvalNode;
import com.starrocks.planner.AssertNumRowsNode;
import com.starrocks.planner.BinlogScanNode;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.DecodeNode;
import com.starrocks.planner.DeltaLakeScanNode;
import com.starrocks.planner.EmptySetNode;
import com.starrocks.planner.EsScanNode;
import com.starrocks.planner.ExceptNode;
import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.ExecGroup;
import com.starrocks.planner.ExecGroupSets;
import com.starrocks.planner.FileScanNode;
import com.starrocks.planner.FileTableScanNode;
import com.starrocks.planner.FragmentNormalizer;
import com.starrocks.planner.HashJoinNode;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.HudiScanNode;
import com.starrocks.planner.IcebergMetadataScanNode;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.IntersectNode;
import com.starrocks.planner.JDBCScanNode;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.KuduScanNode;
import com.starrocks.planner.MergeJoinNode;
import com.starrocks.planner.MetaScanNode;
import com.starrocks.planner.MultiCastPlanFragment;
import com.starrocks.planner.MysqlScanNode;
import com.starrocks.planner.NestLoopJoinNode;
import com.starrocks.planner.OdpsScanNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PaimonScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.ProjectNode;
import com.starrocks.planner.RepeatNode;
import com.starrocks.planner.RuntimeFilterId;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SchemaScanNode;
import com.starrocks.planner.SelectNode;
import com.starrocks.planner.SetOperationNode;
import com.starrocks.planner.SortNode;
import com.starrocks.planner.SplitCastPlanFragment;
import com.starrocks.planner.TableFunctionNode;
import com.starrocks.planner.UnionNode;
import com.starrocks.planner.stream.StreamAggNode;
import com.starrocks.planner.stream.StreamJoinNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.analyzer.DecimalV3FunctionAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AssertNumRowsElement;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.UKFKConstraintsCollector;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.UKFKConstraints;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalConcatenateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDecodeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDeltaLakeScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFileScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHudiScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergEqualityDeleteScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergMetadataScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalKuduScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMergeJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOdpsScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSetOperation;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSplitConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSplitProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionTableScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamAggOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamJoinOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamScanOperator;
import com.starrocks.sql.optimizer.rule.tree.AddDecodeNodeForDictStringRule.DecodeVisitor;
import com.starrocks.sql.optimizer.rule.tree.prunesubfield.SubfieldAccessPathNormalizer;
import com.starrocks.sql.optimizer.rule.tree.prunesubfield.SubfieldExpressionCollector;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TFileScanType;
import com.starrocks.thrift.TPartitionType;
import com.starrocks.thrift.TResultSinkType;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF;
import static com.starrocks.sql.common.ErrorType.INTERNAL_ERROR;
import static com.starrocks.sql.common.UnsupportedException.unsupportedException;
import static com.starrocks.sql.optimizer.operator.scalar.ScalarOperator.isColumnEqualConstant;

/**
 * PlanFragmentBuilder used to transform physical operator to exec plan fragment
 */
public class PlanFragmentBuilder {
    private static final Logger LOG = LogManager.getLogger(PlanFragmentBuilder.class);

    public static ExecPlan createPhysicalPlan(OptExpression plan, ConnectContext connectContext,
                                              List<ColumnRefOperator> outputColumns, ColumnRefFactory columnRefFactory,
                                              List<String> colNames,
                                              TResultSinkType resultSinkType,
                                              boolean hasOutputFragment, boolean isShortCircuit) {
        UKFKConstraintsCollector.collectColumnConstraints(plan);
        ExecPlan execPlan = new ExecPlan(connectContext, colNames, plan, outputColumns, isShortCircuit);
        createOutputFragment(new PhysicalPlanTranslator(columnRefFactory).translate(plan, execPlan), execPlan,
                outputColumns, hasOutputFragment);
        execPlan.setPlanCount(plan.getPlanCount());
        return finalizeFragments(execPlan, resultSinkType);
    }

    public static ExecPlan createPhysicalPlan(OptExpression plan, ConnectContext connectContext,
                                              List<ColumnRefOperator> outputColumns, ColumnRefFactory columnRefFactory,
                                              List<String> colNames,
                                              TResultSinkType resultSinkType,
                                              boolean hasOutputFragment) {
        return createPhysicalPlan(plan, connectContext, outputColumns, columnRefFactory, colNames, resultSinkType,
                hasOutputFragment, false);
    }

    public static ExecPlan createPhysicalPlanForMV(ConnectContext connectContext,
                                                   CreateMaterializedViewStatement createStmt,
                                                   OptExpression optExpr,
                                                   LogicalPlan logicalPlan,
                                                   QueryRelation queryRelation,
                                                   ColumnRefFactory columnRefFactory) throws DdlException {
        List<String> colNames = queryRelation.getColumnOutputNames();
        List<ColumnRefOperator> outputColumns = logicalPlan.getOutputColumn();
        ExecPlan execPlan = new ExecPlan(connectContext, colNames, optExpr, outputColumns, false);
        PlanFragment planFragment = new PhysicalPlanTranslator(columnRefFactory).translate(optExpr, execPlan);
        // createOutputFragment(planFragment, execPlan, outputColumns, false);
        execPlan.setPlanCount(optExpr.getPlanCount());
        createStmt.setMaintenancePlan(execPlan, columnRefFactory);

        // Finalize fragments
        for (PlanFragment fragment : execPlan.getFragments()) {
            // TODO(murphy) check it
            fragment.createDataSink(TResultSinkType.MYSQL_PROTOCAL);
        }
        Collections.reverse(execPlan.getFragments());

        // Create a fake table sink here, replaced it after created the MV
        PartitionInfo partitionInfo = LocalMetastore.buildPartitionInfo(createStmt, null);
        long mvId = GlobalStateMgr.getCurrentState().getNextId();
        long dbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(createStmt.getTableName().getDb()).getId();
        MaterializedView view = GlobalStateMgr.getCurrentState().getMaterializedViewMgr()
                .createSinkTable(createStmt, partitionInfo, mvId, dbId);
        TupleDescriptor tupleDesc = buildTupleDesc(execPlan, view);
        view.setMaintenancePlan(execPlan);
        List<Long> fakePartitionIds = Arrays.asList(1L, 2L, 3L);

        DataSink tableSink = new OlapTableSink(view, tupleDesc, fakePartitionIds,
                view.writeQuorum(), view.enableReplicatedStorage(), false, false,
                connectContext.getCurrentWarehouseId());
        execPlan.getTopFragment().setSink(tableSink);

        return execPlan;
    }

    private static TupleDescriptor buildTupleDesc(ExecPlan execPlan, Table table) {
        DescriptorTable descriptorTable = execPlan.getDescTbl();
        TupleDescriptor olapTuple = descriptorTable.createTupleDescriptor();
        for (Column column : table.getFullSchema()) {
            SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(olapTuple);
            slotDescriptor.setIsMaterialized(true);
            slotDescriptor.setType(column.getType());
            slotDescriptor.setColumn(column);
            slotDescriptor.setIsNullable(column.isAllowNull());
            // TODO(murphy) support global dict
        }
        olapTuple.computeMemLayout();
        return olapTuple;
    }

    private static void createOutputFragment(PlanFragment inputFragment, ExecPlan execPlan,
                                             List<ColumnRefOperator> outputColumns,
                                             boolean hasOutputFragment) {
        if (inputFragment.getPlanRoot() instanceof ExchangeNode || !inputFragment.isPartitioned() ||
                !hasOutputFragment) {
            List<Expr> outputExprs = outputColumns.stream().map(variable -> ScalarOperatorToExpr
                    .buildExecExpression(variable,
                            new ScalarOperatorToExpr.FormatterContext(execPlan.getColRefToExpr()))
            ).collect(Collectors.toList());
            inputFragment.setOutputExprs(outputExprs);
            execPlan.getOutputExprs().addAll(outputExprs);
            return;
        }

        List<Expr> outputExprs = outputColumns.stream().map(variable -> ScalarOperatorToExpr
                        .buildExecExpression(variable,
                                new ScalarOperatorToExpr.FormatterContext(execPlan.getColRefToExpr())))
                .collect(Collectors.toList());
        execPlan.getOutputExprs().addAll(outputExprs);

        // Single tablet direct output
        // Note: If the fragment has right or full join and the join is local bucket shuffle join,
        // We shouldn't set result sink directly to top fragment, because we will hash multi result sink.
        // Note: If enable compute node and the top fragment not GATHER node, the parallelism of top fragment can't
        // be 1, it's error
        if ((!enableComputeNode(execPlan)
                && !inputFragment.hashLocalBucketShuffleRightOrFullJoin(inputFragment.getPlanRoot())
                && execPlan.getScanNodes().stream().allMatch(d -> d instanceof OlapScanNode)
                && (execPlan.getScanNodes().stream().map(d -> ((OlapScanNode) d).getScanTabletIds().size())
                .reduce(Integer::sum).orElse(2) <= 1)) || execPlan.isShortCircuit()) {
            inputFragment.setOutputExprs(outputExprs);
            return;
        }

        ExchangeNode exchangeNode =
                new ExchangeNode(execPlan.getNextNodeId(), inputFragment.getPlanRoot(), DataPartition.UNPARTITIONED);
        PlanFragment exchangeFragment =
                new PlanFragment(execPlan.getNextFragmentId(), exchangeNode, DataPartition.UNPARTITIONED);
        inputFragment.setDestination(exchangeNode);
        inputFragment.setOutputPartition(DataPartition.UNPARTITIONED);

        exchangeFragment.setOutputExprs(outputExprs);
        execPlan.getFragments().add(exchangeFragment);
    }

    private static boolean enableComputeNode(ExecPlan execPlan) {
        boolean preferComputeNode = execPlan.getConnectContext().getSessionVariable().isPreferComputeNode();
        if (preferComputeNode || RunMode.isSharedDataMode()) {
            return true;
        }
        return false;
    }

    private static boolean useQueryCache(ExecPlan execPlan) {
        if (!execPlan.getConnectContext().getSessionVariable().isEnableQueryCache()) {
            return false;
        }
        return true;
    }

    private static ExecPlan finalizeFragments(ExecPlan execPlan, TResultSinkType resultSinkType) {
        List<PlanFragment> fragments = execPlan.getFragments();
        for (PlanFragment fragment : fragments) {
            fragment.createDataSink(resultSinkType);
            fragment.setCollectExecStatsIds(execPlan.getCollectExecStatsIds());
        }
        Collections.reverse(fragments);
        // assign colocate groups to plan fragment
        if (ConnectContext.get().getSessionVariable().isEnableGroupExecution()) {
            List<ExecGroup> colocateExecGroups =
                    execPlan.getExecGroups().stream().filter(ExecGroup::isColocateExecGroup).collect(
                            Collectors.toList());
            for (PlanFragment fragment : fragments) {
                fragment.assignColocateExecGroups(fragment.getPlanRoot(), colocateExecGroups);
            }
            Preconditions.checkState(colocateExecGroups.isEmpty());
        }

        for (PlanFragment fragment : fragments) {
            fragment.removeRfOnRightOffspringsOfBroadcastJoin();
        }
        // compute local_rf_waiting_set for each PlanNode.
        // when enable_pipeline_engine=true and enable_global_runtime_filter=false, we should clear
        // runtime filters from PlanNode.
        boolean shouldClearRuntimeFilters = ConnectContext.get() != null &&
                !ConnectContext.get().getSessionVariable().getEnableGlobalRuntimeFilter() &&
                ConnectContext.get().getSessionVariable().isEnablePipelineEngine();
        for (PlanFragment fragment : fragments) {
            fragment.computeLocalRfWaitingSet(fragment.getPlanRoot(), shouldClearRuntimeFilters);
        }

        fragments.forEach(PlanFragment::removeDictMappingProbeRuntimeFilters);
        fragments.forEach(PlanFragment::collectBuildRuntimeFilters);
        fragments.forEach(PlanFragment::collectProbeRuntimeFilters);

        if (useQueryCache(execPlan)) {
            for (PlanFragment fragment : execPlan.getFragments()) {
                FragmentNormalizer normalizer = new FragmentNormalizer(execPlan, fragment);
                normalizer.normalize();
            }
        } else if (ConnectContext.get() != null &&
                ConnectContext.get().getSessionVariable().isEnableRuntimeAdaptiveDop()) {
            for (PlanFragment fragment : fragments) {
                if (fragment.canUseRuntimeAdaptiveDop()) {
                    fragment.enableAdaptiveDop();
                }
            }
        }

        return execPlan;
    }

    public static List<ColumnRefOperator> getShuffleColumns(HashDistributionSpec spec,
                                                            ColumnRefFactory columnRefFactory) {
        List<DistributionCol> columns = spec.getShuffleColumns();
        Preconditions.checkState(!columns.isEmpty());

        List<ColumnRefOperator> shuffleColumns = new ArrayList<>();
        for (DistributionCol column : columns) {
            shuffleColumns.add(columnRefFactory.getColumnRef(column.getColId()));
        }
        return shuffleColumns;
    }

    private static class PhysicalPlanTranslator extends OptExpressionVisitor<PlanFragment, ExecPlan> {
        private final ColumnRefFactory columnRefFactory;
        private final IdGenerator<RuntimeFilterId> runtimeFilterIdIdGenerator = RuntimeFilterId.createGenerator();
        private final ExecGroupSets execGroups = new ExecGroupSets();

        private final List<Integer> collectExecStatsIds = Lists.newArrayList();
        private ExecGroup currentExecGroup = execGroups.newExecGroup();

        private boolean canUseLocalShuffleAgg = true;

        public PhysicalPlanTranslator(ColumnRefFactory columnRefFactory) {
            this.columnRefFactory = columnRefFactory;
        }

        public PlanFragment translate(OptExpression optExpression, ExecPlan context) {
            PlanFragment fragment = visit(optExpression, context);
            computeFragmentCost(context, fragment);
            collectExecStatsIds(fragment.getPlanRoot());
            context.setExecGroups(execGroups.getExecGroups());
            context.setCollectExecStatsIds(collectExecStatsIds);
            return fragment;
        }

        private void collectExecStatsIds(PlanNode root) {
            if (ConnectContext.get() == null) {
                return;
            }
            if (!ConnectContext.get().getSessionVariable().isEnablePlanAdvisor()) {
                return;
            }

            for (PlanNode child : root.getChildren()) {
                collectExecStatsIds(child);
            }
            if (root.needCollectExecStats()) {
                collectExecStatsIds.add(root.getId().asInt());
            }
        }

        private void computeFragmentCost(ExecPlan context, PlanFragment fragment) {
            for (PlanFragment child : fragment.getChildren()) {
                computeFragmentCost(context, child);
            }
            OptExpression output = getOptExpressionFromPlanNode(context, fragment.getPlanRoot());

            // scan fragment
            if (fragment.getLeftMostNode() instanceof ScanNode) {
                fragment.setFragmentCost(output.getCost());
                return;
            }

            List<OptExpression> inputs = fragment.getChildren().stream().map(PlanFragment::getPlanRoot)
                    .map(p -> getOptExpressionFromPlanNode(context, p)).collect(Collectors.toList());

            double childCost = inputs.stream().map(OptExpression::getCost).reduce(Double::sum).orElse(0D);
            fragment.setFragmentCost(output.getCost() - childCost);
        }

        private OptExpression getOptExpressionFromPlanNode(ExecPlan context, PlanNode node) {
            if (context.getOptExpression(node.getId().asInt()) == null && node instanceof ProjectNode) {
                node = node.getChild(0);
            }
            return context.getOptExpression(node.getId().asInt());
        }

        @Override
        public PlanFragment visit(OptExpression optExpression, ExecPlan context) {
            canUseLocalShuffleAgg &= optExpression.arity() <= 1;
            PlanFragment fragment = optExpression.getOp().accept(this, optExpression, context);
            Projection projection = (optExpression.getOp()).getProjection();

            if (projection != null) {
                fragment = buildProjectNode(optExpression, projection, fragment, context);
            }
            PlanNode planRoot = fragment.getPlanRoot();
            if (!(optExpression.getOp() instanceof PhysicalProjectOperator) && planRoot instanceof ProjectNode) {
                // This projectNode comes from another node's projection field
                planRoot = planRoot.getChild(0);
            }
            context.recordPlanNodeId2OptExpression(planRoot.getId().asInt(), optExpression);
            return fragment;
        }

        /**
         * Set the columns which do not need to be output by ScanNode. They are columns which are only used in pushdownable
         * predicates rather than columns which are used in non-pushdownable predicates and output columns.
         *
         * <p> The columns that can be pushed down need to meet:
         * <ul>
         * <li> All the columns of duplicate-key model.
         * <li> Keys of primary-key model.
         * <li> Keys of agg-key model (aggregation/unique_key model) in the skip-aggr scan stage.
         * </ul>
         *
         * <p> The predicates that can be pushed down need to meet:
         * <ul>
         * <li> Simple single-column predicates, and the column can be pushed down, such as a = v1.
         * <li> AND and OR predicates, and the sub-predicates are all pushable, such as a = v1 OR (b = v2 AND c = v3).
         * <li> The other predicates are not pushable, such as a + b = v1, a = v1 OR a + b = v2.
         * </ul>
         *
         * <p> The columns in the predicates that cannot be pushed down need to be retained, because these columns are
         * used in the stage after scan.
         */
        private void setUnUsedOutputColumns(PhysicalOlapScanOperator node, OlapScanNode scanNode,
                                            List<ScalarOperator> predicates, OlapTable referenceTable) {
            SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
            if (!sessionVariable.isEnableFilterUnusedColumnsInScanStage()) {
                return;
            }

            // Key columns and value columns cannot be pruned in the non-skip-aggr scan stage.
            // - All the keys columns must be retained to merge and aggregate rows.
            // - Value columns can only be used after merging and aggregating.
            MaterializedIndexMeta materializedIndexMeta = referenceTable.getIndexMetaByIndexId(node.getSelectedIndexId());
            if (materializedIndexMeta.getKeysType().isAggregationFamily() && !node.isPreAggregation()) {
                return;
            }

            // ------------------------------------------------------------------------------------
            // Get outputColumnIds.
            // ------------------------------------------------------------------------------------
            Set<Integer> outputColumnIds = node.getOutputColumns().stream()
                    .map(ColumnRefOperator::getId)
                    .collect(Collectors.toSet());
            // Empty outputColumnIds means that the expression after ScanNode does not need any column from ScanNode.
            // However, at least one column needs to be output, so choose any column as the output column.
            if (outputColumnIds.isEmpty()) {
                if (!scanNode.getSlots().isEmpty()) {
                    outputColumnIds.add(scanNode.getSlots().get(0).getId().asInt());
                }
            }

            // ------------------------------------------------------------------------------------
            // Get nonPushdownColumnIds.
            // ------------------------------------------------------------------------------------
            Set<Integer> nonPushdownColumnIds = Collections.emptySet();
            if (materializedIndexMeta.getKeysType().isAggregationFamily() ||
                    materializedIndexMeta.getKeysType() == KeysType.PRIMARY_KEYS) {
                Map<String, Integer> columnNameToId = scanNode.getSlots().stream().collect(Collectors.toMap(
                        slot -> slot.getColumn().getName(),
                        slot -> slot.getId().asInt()
                ));
                nonPushdownColumnIds = materializedIndexMeta.getSchema().stream()
                        .filter(col -> !col.isKey())
                        .map(Column::getName)
                        .map(columnNameToId::get)
                        .collect(Collectors.toSet());
            }

            // ------------------------------------------------------------------------------------
            // Get pushdownPredUsedColumnIds and nonPushdownPredUsedColumnIds.
            // ------------------------------------------------------------------------------------
            Set<Integer> pushdownPredUsedColumnIds = new HashSet<>();
            Set<Integer> nonPushdownPredUsedColumnIds = new HashSet<>();
            for (ScalarOperator predicate : predicates) {
                ColumnRefSet usedColumns = predicate.getUsedColumns();
                boolean isPushdown =
                        DecodeVisitor.isSimpleStrictPredicate(predicate, sessionVariable.isEnablePushdownOrPredicate())
                                && Arrays.stream(usedColumns.getColumnIds()).noneMatch(nonPushdownColumnIds::contains);
                if (isPushdown) {
                    for (int cid : usedColumns.getColumnIds()) {
                        pushdownPredUsedColumnIds.add(cid);
                    }
                } else {
                    for (int cid : usedColumns.getColumnIds()) {
                        nonPushdownPredUsedColumnIds.add(cid);
                    }
                }
            }

            // ------------------------------------------------------------------------------------
            // Get unUsedOutputColumnIds which are in pushdownPredUsedColumnIds
            // but not in nonPushdownPredUsedColumnIds and outputColumnIds.
            // ------------------------------------------------------------------------------------
            Set<Integer> unUsedOutputColumnIds = pushdownPredUsedColumnIds.stream()
                    .filter(cid -> !nonPushdownPredUsedColumnIds.contains(cid) && !outputColumnIds.contains(cid))
                    .collect(Collectors.toSet());
            scanNode.setUnUsedOutputStringColumns(unUsedOutputColumnIds);
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
                    new ProjectNode(context.getNextNodeId(),
                            tupleDescriptor,
                            inputFragment.getPlanRoot(),
                            projectMap,
                            commonSubOperatorMap);

            projectNode.setHasNullableGenerateChild();
            projectNode.computeStatistics(optExpr.getStatistics());
            currentExecGroup.add(projectNode);

            for (SlotId sid : projectMap.keySet()) {
                SlotDescriptor slotDescriptor = tupleDescriptor.getSlot(sid.asInt());
                slotDescriptor.setIsNullable(slotDescriptor.getIsNullable() | projectNode.isHasNullableGenerateChild());
            }
            tupleDescriptor.computeMemLayout();

            projectNode.setLimit(inputFragment.getPlanRoot().getLimit());
            inputFragment.setPlanRoot(projectNode);
            inputFragment.setShortCircuit(context.isShortCircuit());
            return inputFragment;
        }

        public PlanFragment buildProjectNode(OptExpression optExpression, Projection node, PlanFragment inputFragment,
                                             ExecPlan context) {
            if (node == null) {
                return inputFragment;
            }

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
                    new ProjectNode(context.getNextNodeId(),
                            tupleDescriptor,
                            inputFragment.getPlanRoot(),
                            projectMap,
                            commonSubOperatorMap);

            projectNode.setHasNullableGenerateChild();

            Optional.ofNullable(optExpression.getStatistics()).ifPresent(statistics -> {
                Statistics.Builder b = Statistics.builder();
                b.setOutputRowCount(statistics.getOutputRowCount());
                b.addColumnStatisticsFromOtherStatistic(statistics, new ColumnRefSet(node.getOutputColumns()), true);
                projectNode.computeStatistics(b.build());
            });

            for (Map.Entry<SlotId, Expr> entry : projectMap.entrySet()) {
                SlotDescriptor slotDescriptor = tupleDescriptor.getSlot(entry.getKey().asInt());
                if (entry.getValue().isLiteral() && !entry.getValue().isNullable()) {
                    slotDescriptor.setIsNullable(false);
                } else {
                    slotDescriptor.setIsNullable(
                            slotDescriptor.getIsNullable() | projectNode.isHasNullableGenerateChild());
                }
            }
            tupleDescriptor.computeMemLayout();

            projectNode.setLimit(inputFragment.getPlanRoot().getLimit());
            inputFragment.setPlanRoot(projectNode);
            currentExecGroup.add(projectNode);
            return inputFragment;
        }

        @Override
        public PlanFragment visitPhysicalDecode(OptExpression optExpression, ExecPlan context) {
            PhysicalDecodeOperator node = (PhysicalDecodeOperator) optExpression.getOp();
            PlanFragment inputFragment = visit(optExpression.inputAt(0), context);

            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();

            Map<SlotRef, SlotRef> slotRefMap = Maps.newHashMap();
            Map<Integer, ColumnRefOperator> dictIdToStringRef = node.getDictToStrings().entrySet().stream()
                    .collect(Collectors.toMap(k -> k.getKey().getId(), Map.Entry::getValue));
            for (TupleId tupleId : inputFragment.getPlanRoot().getTupleIds()) {
                TupleDescriptor childTuple = context.getDescTbl().getTupleDesc(tupleId);
                for (SlotDescriptor slot : childTuple.getSlots()) {
                    int slotId = slot.getId().asInt();
                    if (dictIdToStringRef.containsKey(slotId)) {
                        ColumnRefOperator stringRef = dictIdToStringRef.get(slotId);
                        SlotDescriptor slotDescriptor = context.getDescTbl()
                                .addSlotDescriptor(tupleDescriptor, new SlotId(stringRef.getId()));
                        slotDescriptor.setIsNullable(slot.getIsNullable());
                        slotDescriptor.setIsMaterialized(true);
                        slotDescriptor.setType(stringRef.getType());

                        context.getColRefToExpr().put(new ColumnRefOperator(stringRef.getId(), stringRef.getType(),
                                        "<dict-code>", slotDescriptor.getIsNullable()),
                                new SlotRef(stringRef.toString(), slotDescriptor));
                    } else {
                        // Note: must change the parent tuple id
                        SlotDescriptor slotDescriptor = new SlotDescriptor(slot.getId(), tupleDescriptor, slot);
                        tupleDescriptor.addSlot(slotDescriptor);
                        SlotRef inputSlotRef = new SlotRef(slot);
                        SlotRef outputSlotRef = new SlotRef(slotDescriptor);
                        slotRefMap.put(outputSlotRef, inputSlotRef);
                    }
                }
            }

            Map<SlotId, Expr> projectMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : node.getStringFunctions().entrySet()) {
                Expr expr = ScalarOperatorToExpr.buildExecExpression(entry.getValue(),
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr(),
                                node.getStringFunctions()));

                projectMap.put(new SlotId(entry.getKey().getId()), expr);
                Preconditions.checkState(context.getColRefToExpr().containsKey(entry.getKey()));
            }

            tupleDescriptor.computeMemLayout();

            DecodeNode decodeNode = new DecodeNode(context.getNextNodeId(),
                    tupleDescriptor,
                    inputFragment.getPlanRoot(),
                    node.getDictIdToStringsId(), projectMap, slotRefMap);
            decodeNode.computeStatistics(optExpression.getStatistics());
            decodeNode.setLimit(node.getLimit());
            currentExecGroup.add(decodeNode);

            inputFragment.setPlanRoot(decodeNode);
            return inputFragment;
        }

        // get all column access path, and mark paths which one is predicate used
        private List<ColumnAccessPath> computeAllColumnAccessPath(PhysicalScanOperator scan, ExecPlan context) {
            if (!context.getConnectContext().getSessionVariable().isCboPredicateSubfieldPath()) {
                return scan.getColumnAccessPaths();
            }

            Set<String> checkNames = scan.getColRefToColumnMetaMap().keySet().stream().map(ColumnRefOperator::getName)
                    .collect(Collectors.toSet());
            if (scan.getPredicate() == null) {
                // remove path which has pruned
                return scan.getColumnAccessPaths().stream().filter(p -> checkNames.contains(p.getPath()))
                        .collect(Collectors.toList());
            }

            SubfieldExpressionCollector collector = new SubfieldExpressionCollector(
                    context.getConnectContext().getSessionVariable().isCboPruneJsonSubfield());
            scan.getPredicate().accept(collector, null);

            List<ColumnAccessPath> paths = Lists.newArrayList();
            SubfieldAccessPathNormalizer normalizer = new SubfieldAccessPathNormalizer();
            normalizer.collect(collector.getComplexExpressions());

            for (ColumnRefOperator key : scan.getColRefToColumnMetaMap().keySet()) {
                if (!key.getType().isStructType()) {
                    continue;
                }

                String name = scan.getColRefToColumnMetaMap().get(key).getName();
                ColumnAccessPath path = normalizer.normalizePath(key, name);
                if (path.onlyRoot()) {
                    continue;
                }
                path.setFromPredicate(true);
                paths.add(path);
            }
            paths.addAll(scan.getColumnAccessPaths());
            return paths;
        }

        @Override
        public PlanFragment visitPhysicalOlapScan(OptExpression optExpr, ExecPlan context) {
            PhysicalOlapScanOperator node = (PhysicalOlapScanOperator) optExpr.getOp();

            OlapTable referenceTable = (OlapTable) node.getTable();
            context.getDescTbl().addReferencedTable(referenceTable);
            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            tupleDescriptor.setTable(referenceTable);

            OlapScanNode scanNode = new OlapScanNode(context.getNextNodeId(), tupleDescriptor, "OlapScanNode",
                    context.getConnectContext().getCurrentWarehouseId());
            scanNode.setLimit(node.getLimit());
            scanNode.computeStatistics(optExpr.getStatistics());
            scanNode.setScanOptimzeOption(node.getScanOptimzeOption());
            scanNode.setIsSortedByKeyPerTablet(node.needSortedByKeyPerTablet());
            scanNode.setIsOutputChunkByBucket(node.needOutputChunkByBucket());
            scanNode.setWithoutColocateRequirement(node.isWithoutColocateRequirement());
            scanNode.setGtid(node.getGtid());
            scanNode.setVectorSearchOptions(node.getVectorSearchOptions());
            scanNode.setSample(node.getSample());
            currentExecGroup.add(scanNode);
            // set tablet
            try {
                scanNode.updateScanInfo(node.getSelectedPartitionId(),
                        node.getSelectedTabletId(),
                        node.getHintsReplicaId(),
                        node.getSelectedIndexId());
                long selectedIndexId = node.getSelectedIndexId();
                long totalTabletsNum = 0;
                // Compatible with old tablet selected, copy from "OlapScanNode::computeTabletInfo"
                // we can remove code when refactor tablet select
                long localBeId = -1;
                if (Config.enable_local_replica_selection) {
                    localBeId = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                            .getBackendIdByHost(FrontendOptions.getLocalHostAddress());
                }

                // Filter out empty partitions from all selected partitions, original selected partition ids may be
                // only parent partition ids if table contains subpartitions, use the real sub partition ids instead.
                // eg:
                // partition        : 10001 -> (tablet_1)
                //  subpartition1   : 10002 -> (tablet_2)
                //  subpartition2   : 10004 -> (tablet_3)
                // original selected partition id with tablet ids: 10001 -> (tablet_2)
                // after:
                // selected partition ids   : 10002
                // selected tablet ids      : tablet_2
                // total tablets num        : 1
                List<Long> selectedNonEmptyPartitionIds = Lists.newArrayList();
                for (Long partitionId : scanNode.getSelectedPartitionIds()) {
                    final Partition partition = referenceTable.getPartition(partitionId);
                    for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                        List<Long> selectTabletIds = scanNode.getPartitionToScanTabletMap()
                                .get(physicalPartition.getId());
                        if (CollectionUtils.isEmpty(selectTabletIds)) {
                            continue;
                        }
                        selectedNonEmptyPartitionIds.add(partitionId);
                        Map<Long, Integer> tabletId2BucketSeq = Maps.newHashMap();
                        Preconditions.checkState(selectTabletIds != null && !selectTabletIds.isEmpty());
                        final MaterializedIndex selectedTable = physicalPartition.getIndex(selectedIndexId);
                        List<Long> allTabletIds = selectedTable.getTabletIdsInOrder();
                        totalTabletsNum += selectedTable.getTablets().size();
                        for (int i = 0; i < allTabletIds.size(); i++) {
                            tabletId2BucketSeq.put(allTabletIds.get(i), i);
                        }
                        scanNode.setTabletId2BucketSeq(tabletId2BucketSeq);
                        List<Tablet> tablets =
                                selectTabletIds.stream().map(selectedTable::getTablet).collect(Collectors.toList());
                        scanNode.addScanRangeLocations(partition, physicalPartition, selectedTable, tablets, localBeId);
                    }
                }
                scanNode.setSelectedPartitionIds(selectedNonEmptyPartitionIds);
                scanNode.setTotalTabletsNum(totalTabletsNum);
            } catch (StarRocksException e) {
                throw new StarRocksPlannerException(
                        "Build Exec OlapScanNode fail, scan info is invalid", INTERNAL_ERROR, e);
            }

            // set slot
            for (Map.Entry<ColumnRefOperator, Column> entry : node.getColRefToColumnMetaMap().entrySet()) {
                SlotDescriptor slotDescriptor =
                        context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(entry.getKey().getId()));
                slotDescriptor.setColumn(entry.getValue());
                slotDescriptor.setIsNullable(entry.getValue().isAllowNull());
                slotDescriptor.setIsMaterialized(true);
                if (slotDescriptor.getOriginType().isComplexType()) {
                    slotDescriptor.setOriginType(entry.getKey().getType());
                    slotDescriptor.setType(entry.getKey().getType());
                }
                context.getColRefToExpr().put(entry.getKey(), new SlotRef(entry.getKey().toString(), slotDescriptor));
            }

            // set column access path
            scanNode.setColumnAccessPaths(computeAllColumnAccessPath(node, context));

            // set predicate
            List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());

            for (ScalarOperator predicate : predicates) {
                scanNode.getConjuncts().add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
            }

            for (ScalarOperator predicate : node.getPrunedPartitionPredicates()) {
                scanNode.getPrunedPartitionPredicates()
                        .add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
            }

            tupleDescriptor.computeMemLayout();

            // set unused output columns 
            setUnUsedOutputColumns(node, scanNode, predicates, referenceTable);

            // set isPreAggregation
            scanNode.setIsPreAggregation(node.isPreAggregation(), node.getTurnOffReason());
            scanNode.updateAppliedDictStringColumns(node.getGlobalDicts().stream().
                    map(entry -> entry.first).collect(Collectors.toSet()));

            scanNode.setUsePkIndex(node.isUsePkIndex());
            context.getScanNodes().add(scanNode);
            PlanFragment fragment =
                    new PlanFragment(context.getNextFragmentId(), scanNode, DataPartition.RANDOM);
            fragment.setQueryGlobalDicts(node.getGlobalDicts());
            fragment.setQueryGlobalDictExprs(getGlobalDictsExprs(node.getGlobalDictsExpr(), context));
            fragment.setShortCircuit(context.isShortCircuit());
            context.getFragments().add(fragment);

            // set row store literal
            if (context.isShortCircuit()) {
                scanNode.computePointScanRangeLocations();
            }
            return fragment;
        }

        @NotNull
        private static Map<Integer, Expr> getGlobalDictsExprs(Map<Integer, ScalarOperator> dictExprs,
                                                              ExecPlan context) {
            if (dictExprs.isEmpty()) {
                return Collections.emptyMap();
            }
            Map<ColumnRefOperator, Expr> nodeRefs = context.getColRefToExpr();
            List<ColumnRefOperator> columnRefs = Lists.newArrayList();
            dictExprs.values().forEach(v -> v.getColumnRefs(columnRefs));

            if (!nodeRefs.keySet().containsAll(columnRefs)) {
                nodeRefs = Maps.newHashMap(nodeRefs);
                for (ColumnRefOperator f : columnRefs) {
                    nodeRefs.computeIfAbsent(f, k -> new SlotRef(f.getName(),
                            new SlotDescriptor(new SlotId(f.getId()), f.getName(), f.getType(), true)));
                }
            }

            Map<Integer, Expr> globalDictsExprs = Maps.newHashMap();
            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(nodeRefs);

            dictExprs.forEach((k, v) ->
                    globalDictsExprs.put(k, ScalarOperatorToExpr.buildExecExpression(v, formatterContext)));
            return globalDictsExprs;
        }

        @Override
        public PlanFragment visitPhysicalMetaScan(OptExpression optExpression, ExecPlan context) {
            PhysicalMetaScanOperator scan = (PhysicalMetaScanOperator) optExpression.getOp();

            context.getDescTbl().addReferencedTable(scan.getTable());
            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            tupleDescriptor.setTable(scan.getTable());

            MetaScanNode scanNode = new MetaScanNode(context.getNextNodeId(),
                    tupleDescriptor, (OlapTable) scan.getTable(), scan.getAggColumnIdToNames(),
                    scan.getSelectPartitionNames(),
                    context.getConnectContext().getCurrentWarehouseId());
            scanNode.computeRangeLocations();
            scanNode.computeStatistics(optExpression.getStatistics());
            currentExecGroup.add(scanNode, true);

            for (Map.Entry<ColumnRefOperator, Column> entry : scan.getColRefToColumnMetaMap().entrySet()) {
                SlotDescriptor slotDescriptor =
                        context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(entry.getKey().getId()));
                slotDescriptor.setColumn(entry.getValue());
                slotDescriptor.setIsNullable(entry.getValue().isAllowNull());
                slotDescriptor.setIsMaterialized(true);
                slotDescriptor.setType(entry.getKey().getType());
                context.getColRefToExpr().put(entry.getKey(), new SlotRef(entry.getKey().getName(), slotDescriptor));
            }
            tupleDescriptor.computeMemLayout();

            context.getScanNodes().add(scanNode);
            PlanFragment fragment =
                    new PlanFragment(context.getNextFragmentId(), scanNode, DataPartition.RANDOM);
            context.getFragments().add(fragment);
            return fragment;
        }

        private void prepareContextSlots(PhysicalScanOperator node, ExecPlan context, TupleDescriptor tupleDescriptor) {
            // set slot
            for (Map.Entry<ColumnRefOperator, Column> entry : node.getColRefToColumnMetaMap().entrySet()) {
                SlotDescriptor slotDescriptor =
                        context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(entry.getKey().getId()));
                slotDescriptor.setColumn(entry.getValue());
                slotDescriptor.setIsNullable(entry.getValue().isAllowNull());
                slotDescriptor.setIsMaterialized(true);
                slotDescriptor.setIsOutputColumn(node.getOutputColumns().contains(entry.getKey()));
                if (slotDescriptor.getOriginType().isComplexType()) {
                    slotDescriptor.setOriginType(entry.getKey().getType());
                    slotDescriptor.setType(entry.getKey().getType());
                }
                context.getColRefToExpr().put(entry.getKey(), new SlotRef(entry.getKey().toString(), slotDescriptor));
            }
        }

        private void prepareCommonExpr(HDFSScanNodePredicates scanNodePredicates,
                                       ScanOperatorPredicates predicates, ExecPlan context) {
            // set predicate
            List<ScalarOperator> noEvalPartitionConjuncts = predicates.getNoEvalPartitionConjuncts();
            List<ScalarOperator> nonPartitionConjuncts = predicates.getNonPartitionConjuncts();
            List<ScalarOperator> partitionConjuncts = predicates.getPartitionConjuncts();
            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());

            for (ScalarOperator partitionConjunct : partitionConjuncts) {
                scanNodePredicates.getPartitionConjuncts().
                        add(ScalarOperatorToExpr.buildExecExpression(partitionConjunct, formatterContext));
            }
            for (ScalarOperator noEvalPartitionConjunct : noEvalPartitionConjuncts) {
                scanNodePredicates.getNoEvalPartitionConjuncts().
                        add(ScalarOperatorToExpr.buildExecExpression(noEvalPartitionConjunct, formatterContext));
            }
            for (ScalarOperator nonPartitionConjunct : nonPartitionConjuncts) {
                scanNodePredicates.getNonPartitionConjuncts().
                        add(ScalarOperatorToExpr.buildExecExpression(nonPartitionConjunct, formatterContext));
            }
        }

        private void prepareMinMaxExpr(HDFSScanNodePredicates scanNodePredicates,
                                       ScanOperatorPredicates predicates, ExecPlan context, Table referenceTable) {
            /*
             * populates 'minMaxTuple' with slots for statistics values,
             * and populates 'minMaxConjuncts' with conjuncts pointing into the 'minMaxTuple'
             */
            List<ScalarOperator> minMaxConjuncts = predicates.getMinMaxConjuncts();
            TupleDescriptor minMaxTuple = context.getDescTbl().createTupleDescriptor();
            minMaxTuple.setTable(referenceTable);
            for (ScalarOperator minMaxConjunct : minMaxConjuncts) {
                for (ColumnRefOperator columnRefOperator : Utils.extractColumnRef(minMaxConjunct)) {
                    SlotDescriptor slotDescriptor =
                            context.getDescTbl()
                                    .addSlotDescriptor(minMaxTuple, new SlotId(columnRefOperator.getId()));
                    Column column = predicates.getMinMaxColumnRefMap().get(columnRefOperator);
                    // because extractColumnRef both get dict ref and original ref
                    if (column == null) {
                        continue;
                    }
                    slotDescriptor.setType(column.getType());
                    slotDescriptor.setColumn(column);
                    slotDescriptor.setIsNullable(column.isAllowNull());
                    slotDescriptor.setIsMaterialized(true);
                    context.getColRefToExpr()
                            .putIfAbsent(columnRefOperator, new SlotRef(columnRefOperator.toString(), slotDescriptor));
                }
            }
            minMaxTuple.computeMemLayout();
            scanNodePredicates.setMinMaxTuple(minMaxTuple);
            ScalarOperatorToExpr.FormatterContext minMaxFormatterContext =
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
            for (ScalarOperator minMaxConjunct : minMaxConjuncts) {
                scanNodePredicates.getMinMaxConjuncts().
                        add(ScalarOperatorToExpr.buildExecExpression(minMaxConjunct, minMaxFormatterContext));
            }
        }

        @Override
        public PlanFragment visitPhysicalHudiScan(OptExpression optExpression, ExecPlan context) {
            PhysicalHudiScanOperator node = (PhysicalHudiScanOperator) optExpression.getOp();
            ScanOperatorPredicates predicates = node.getScanOperatorPredicates();

            Table referenceTable = node.getTable();
            context.getDescTbl().addReferencedTable(referenceTable);
            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            tupleDescriptor.setTable(referenceTable);

            prepareContextSlots(node, context, tupleDescriptor);

            HudiScanNode hudiScanNode =
                    new HudiScanNode(context.getNextNodeId(), tupleDescriptor, "HudiScanNode");
            hudiScanNode.computeStatistics(optExpression.getStatistics());
            hudiScanNode.setScanOptimzeOption(node.getScanOptimzeOption());
            currentExecGroup.add(hudiScanNode, true);
            try {
                HDFSScanNodePredicates scanNodePredicates = hudiScanNode.getScanNodePredicates();
                scanNodePredicates.setSelectedPartitionIds(predicates.getSelectedPartitionIds());
                scanNodePredicates.setIdToPartitionKey(predicates.getIdToPartitionKey());

                hudiScanNode.setupScanRangeLocations(context.getDescTbl());

                prepareCommonExpr(scanNodePredicates, predicates, context);
                prepareMinMaxExpr(scanNodePredicates, predicates, context, referenceTable);
            } catch (Exception e) {
                LOG.warn("Hudi scan node get scan range locations failed : ", e);
                throw new StarRocksPlannerException(e.getMessage(), INTERNAL_ERROR);
            }

            hudiScanNode.setLimit(node.getLimit());
            hudiScanNode.setDataCacheOptions(node.getDataCacheOptions());

            tupleDescriptor.computeMemLayout();
            context.getScanNodes().add(hudiScanNode);

            PlanFragment fragment =
                    new PlanFragment(context.getNextFragmentId(), hudiScanNode, DataPartition.RANDOM);
            context.getFragments().add(fragment);
            return fragment;
        }

        @Override
        public PlanFragment visitPhysicalHiveScan(OptExpression optExpression, ExecPlan context) {
            PhysicalHiveScanOperator node = (PhysicalHiveScanOperator) optExpression.getOp();
            ScanOperatorPredicates predicates = node.getScanOperatorPredicates();

            Table referenceTable = node.getTable();
            context.getDescTbl().addReferencedTable(referenceTable);
            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            tupleDescriptor.setTable(referenceTable);

            prepareContextSlots(node, context, tupleDescriptor);

            HdfsScanNode hdfsScanNode =
                    new HdfsScanNode(context.getNextNodeId(), tupleDescriptor, "HdfsScanNode");
            hdfsScanNode.setScanOptimzeOption(node.getScanOptimzeOption());
            hdfsScanNode.computeStatistics(optExpression.getStatistics());
            currentExecGroup.add(hdfsScanNode, true);
            try {
                HDFSScanNodePredicates scanNodePredicates = hdfsScanNode.getScanNodePredicates();
                scanNodePredicates.setSelectedPartitionIds(predicates.getSelectedPartitionIds());
                scanNodePredicates.setIdToPartitionKey(predicates.getIdToPartitionKey());

                hdfsScanNode.setupScanRangeLocations(context.getDescTbl());

                prepareCommonExpr(scanNodePredicates, predicates, context);
                prepareMinMaxExpr(scanNodePredicates, predicates, context, referenceTable);
            } catch (Exception e) {
                LOG.warn("Hdfs scan node get scan range locations failed : ", e);
                throw new StarRocksPlannerException(e.getMessage(), INTERNAL_ERROR);
            }

            hdfsScanNode.setLimit(node.getLimit());
            hdfsScanNode.setDataCacheOptions(node.getDataCacheOptions());
            hdfsScanNode.updateAppliedDictStringColumns(node.getGlobalDicts().stream().
                    map(entry -> entry.first).collect(Collectors.toSet()));

            tupleDescriptor.computeMemLayout();
            context.getScanNodes().add(hdfsScanNode);

            PlanFragment fragment =
                    new PlanFragment(context.getNextFragmentId(), hdfsScanNode, DataPartition.RANDOM);
            fragment.setQueryGlobalDicts(node.getGlobalDicts());
            fragment.setQueryGlobalDictExprs(getGlobalDictsExprs(node.getGlobalDictsExpr(), context));
            context.getFragments().add(fragment);
            return fragment;
        }

        @Override
        public PlanFragment visitPhysicalFileScan(OptExpression optExpression, ExecPlan context) {
            PhysicalFileScanOperator node = (PhysicalFileScanOperator) optExpression.getOp();
            ScanOperatorPredicates predicates = node.getScanOperatorPredicates();

            Table referenceTable = node.getTable();
            context.getDescTbl().addReferencedTable(referenceTable);
            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            tupleDescriptor.setTable(referenceTable);

            prepareContextSlots(node, context, tupleDescriptor);

            FileTableScanNode fileTableScanNode =
                    new FileTableScanNode(context.getNextNodeId(), tupleDescriptor, "FileTableScanNode");
            fileTableScanNode.setScanOptimzeOption(node.getScanOptimzeOption());
            fileTableScanNode.computeStatistics(optExpression.getStatistics());
            currentExecGroup.add(fileTableScanNode, true);
            try {
                HDFSScanNodePredicates scanNodePredicates = fileTableScanNode.getScanNodePredicates();

                fileTableScanNode.setupScanRangeLocations();

                prepareCommonExpr(scanNodePredicates, predicates, context);
                prepareMinMaxExpr(scanNodePredicates, predicates, context, referenceTable);
            } catch (Exception e) {
                LOG.warn("Hdfs scan node get scan range locations failed : ", e);
                throw new StarRocksPlannerException(e.getMessage(), INTERNAL_ERROR);
            }

            fileTableScanNode.setLimit(node.getLimit());
            fileTableScanNode.setDataCacheOptions(node.getDataCacheOptions());

            tupleDescriptor.computeMemLayout();
            context.getScanNodes().add(fileTableScanNode);

            PlanFragment fragment =
                    new PlanFragment(context.getNextFragmentId(), fileTableScanNode, DataPartition.RANDOM);
            context.getFragments().add(fragment);
            return fragment;
        }

        @Override
        public PlanFragment visitPhysicalDeltaLakeScan(OptExpression optExpression, ExecPlan context) {
            PhysicalDeltaLakeScanOperator node = (PhysicalDeltaLakeScanOperator) optExpression.getOp();

            Table referenceTable = node.getTable();
            context.getDescTbl().addReferencedTable(referenceTable);
            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            tupleDescriptor.setTable(referenceTable);

            prepareContextSlots(node, context, tupleDescriptor);

            DeltaLakeScanNode deltaLakeScanNode =
                    new DeltaLakeScanNode(context.getNextNodeId(), tupleDescriptor, "DeltaLakeScanNode");
            deltaLakeScanNode.computeStatistics(optExpression.getStatistics());
            deltaLakeScanNode.setScanOptimzeOption(node.getScanOptimzeOption());
            currentExecGroup.add(deltaLakeScanNode, true);
            try {
                // set predicate
                ScalarOperatorToExpr.FormatterContext formatterContext =
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
                List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
                for (ScalarOperator predicate : predicates) {
                    deltaLakeScanNode.getConjuncts()
                            .add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
                }

                List<String> fieldNames = node.getColRefToColumnMetaMap().keySet().stream()
                        .map(ColumnRefOperator::getName)
                        .collect(Collectors.toList());
                deltaLakeScanNode.setupScanRangeSource(node.getPredicate(), fieldNames,
                        context.getConnectContext().getSessionVariable().isEnableConnectorIncrementalScanRanges());

                HDFSScanNodePredicates scanNodePredicates = deltaLakeScanNode.getScanNodePredicates();
                prepareCommonExpr(scanNodePredicates, node.getScanOperatorPredicates(), context);
                prepareMinMaxExpr(scanNodePredicates, node.getScanOperatorPredicates(), context, referenceTable);
            } catch (AnalysisException e) {
                LOG.warn("Delta lake scan node get scan range locations failed : ", e);
                throw new StarRocksPlannerException(e.getMessage(), INTERNAL_ERROR);
            } catch (StarRocksException e) {
                LOG.warn("Delta scan node get scan range locations failed : " + e);
                throw new StarRocksPlannerException(e.getMessage(), INTERNAL_ERROR);
            }

            deltaLakeScanNode.setLimit(node.getLimit());
            deltaLakeScanNode.setDataCacheOptions(node.getDataCacheOptions());

            tupleDescriptor.computeMemLayout();
            context.getScanNodes().add(deltaLakeScanNode);

            PlanFragment fragment =
                    new PlanFragment(context.getNextFragmentId(), deltaLakeScanNode, DataPartition.RANDOM);
            context.getFragments().add(fragment);
            return fragment;
        }

        public PlanFragment visitPhysicalPaimonScan(OptExpression optExpression, ExecPlan context) {
            PhysicalPaimonScanOperator node = (PhysicalPaimonScanOperator) optExpression.getOp();

            Table referenceTable = node.getTable();
            context.getDescTbl().addReferencedTable(referenceTable);
            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            tupleDescriptor.setTable(referenceTable);

            // set slot
            prepareContextSlots(node, context, tupleDescriptor);

            PaimonScanNode paimonScanNode =
                    new PaimonScanNode(context.getNextNodeId(), tupleDescriptor, "PaimonScanNode");
            paimonScanNode.setScanOptimzeOption(node.getScanOptimzeOption());
            paimonScanNode.computeStatistics(optExpression.getStatistics());
            currentExecGroup.add(paimonScanNode, true);
            try {
                // set predicate
                ScalarOperatorToExpr.FormatterContext formatterContext =
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
                List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
                for (ScalarOperator predicate : predicates) {
                    paimonScanNode.getConjuncts()
                            .add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
                }
                paimonScanNode.setupScanRangeLocations(tupleDescriptor, node.getPredicate(), node.getLimit());
                HDFSScanNodePredicates scanNodePredicates = paimonScanNode.getScanNodePredicates();
                prepareCommonExpr(scanNodePredicates, node.getScanOperatorPredicates(), context);
                prepareMinMaxExpr(scanNodePredicates, node.getScanOperatorPredicates(), context, referenceTable);
            } catch (Exception e) {
                LOG.warn("Paimon scan node get scan range locations failed : ", e);
                throw new StarRocksPlannerException(e.getMessage(), INTERNAL_ERROR);
            }

            paimonScanNode.setLimit(node.getLimit());
            paimonScanNode.setDataCacheOptions(node.getDataCacheOptions());

            tupleDescriptor.computeMemLayout();
            context.getScanNodes().add(paimonScanNode);

            PlanFragment fragment =
                    new PlanFragment(context.getNextFragmentId(), paimonScanNode, DataPartition.RANDOM);
            context.getFragments().add(fragment);
            return fragment;
        }

        @Override
        public PlanFragment visitPhysicalOdpsScan(OptExpression optExpression, ExecPlan context) {
            PhysicalOdpsScanOperator node = (PhysicalOdpsScanOperator) optExpression.getOp();

            Table referenceTable = node.getTable();
            context.getDescTbl().addReferencedTable(referenceTable);
            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            tupleDescriptor.setTable(referenceTable);

            // set slot
            prepareContextSlots(node, context, tupleDescriptor);

            OdpsScanNode odpsScanNode =
                    new OdpsScanNode(context.getNextNodeId(), tupleDescriptor, "OdpsScanNode");
            odpsScanNode.setScanOptimzeOption(node.getScanOptimzeOption());
            currentExecGroup.add(odpsScanNode, true);
            try {
                // set predicate
                ScalarOperatorToExpr.FormatterContext formatterContext =
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
                List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
                for (ScalarOperator predicate : predicates) {
                    odpsScanNode.getConjuncts()
                            .add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
                }
                ScanOperatorPredicates scanOperatorPredicates = node.getScanOperatorPredicates();
                Collection<Long> selectedPartitionIds = scanOperatorPredicates.getSelectedPartitionIds();
                List<PartitionKey> partitionKeys =
                        selectedPartitionIds.stream().map(id -> scanOperatorPredicates.getIdToPartitionKey().get(id))
                                .collect(Collectors.toList());
                odpsScanNode.setupScanRangeLocations(tupleDescriptor, node.getPredicate(), partitionKeys);
                HDFSScanNodePredicates scanNodePredicates = odpsScanNode.getScanNodePredicates();
                prepareMinMaxExpr(scanNodePredicates, scanOperatorPredicates, context, referenceTable);
                prepareCommonExpr(scanNodePredicates, scanOperatorPredicates, context);
            } catch (Exception e) {
                LOG.error("Odps scan node get scan range locations failed", e);
                throw new StarRocksPlannerException(e.getMessage(), INTERNAL_ERROR);
            }

            odpsScanNode.setLimit(node.getLimit());

            tupleDescriptor.computeMemLayout();
            context.getScanNodes().add(odpsScanNode);

            PlanFragment fragment =
                    new PlanFragment(context.getNextFragmentId(), odpsScanNode, DataPartition.RANDOM);
            context.getFragments().add(fragment);
            return fragment;
        }

        public PlanFragment visitPhysicalKuduScan(OptExpression optExpression, ExecPlan context) {
            PhysicalKuduScanOperator node = (PhysicalKuduScanOperator) optExpression.getOp();

            Table referenceTable = node.getTable();
            context.getDescTbl().addReferencedTable(referenceTable);
            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            tupleDescriptor.setTable(referenceTable);

            // set slot
            prepareContextSlots(node, context, tupleDescriptor);

            KuduScanNode kuduScanNode =
                    new KuduScanNode(context.getNextNodeId(), tupleDescriptor, "KuduScanNode");
            kuduScanNode.setScanOptimzeOption(node.getScanOptimzeOption());
            try {
                // set predicate
                ScalarOperatorToExpr.FormatterContext formatterContext =
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
                List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
                for (ScalarOperator predicate : predicates) {
                    kuduScanNode.getConjuncts()
                            .add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
                }
                kuduScanNode.setupScanRangeLocations(tupleDescriptor, node.getPredicate());
                HDFSScanNodePredicates scanNodePredicates = kuduScanNode.getScanNodePredicates();
                prepareMinMaxExpr(scanNodePredicates, node.getScanOperatorPredicates(), context, referenceTable);
                prepareCommonExpr(scanNodePredicates, node.getScanOperatorPredicates(), context);
            } catch (Exception e) {
                LOG.warn("Kudu scan node get scan range locations failed : ", e);
                throw new StarRocksPlannerException(e.getMessage(), INTERNAL_ERROR);
            }

            kuduScanNode.setLimit(node.getLimit());

            tupleDescriptor.computeMemLayout();
            context.getScanNodes().add(kuduScanNode);

            PlanFragment fragment =
                    new PlanFragment(context.getNextFragmentId(), kuduScanNode, DataPartition.RANDOM);
            context.getFragments().add(fragment);
            return fragment;
        }

        @Override
        public PlanFragment visitPhysicalIcebergScan(OptExpression optExpression, ExecPlan context) {
            return buildIcebergScanNode(optExpression, context);
        }

        @Override
        public PlanFragment visitPhysicalIcebergEqualityDeleteScan(OptExpression optExpression, ExecPlan context) {
            return buildIcebergScanNode(optExpression, context);
        }


        public PlanFragment buildIcebergScanNode(OptExpression expression, ExecPlan context) {
            PhysicalScanOperator node = expression.getOp().cast();
            Table referenceTable = node.getTable();
            context.getDescTbl().addReferencedTable(referenceTable);
            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            tupleDescriptor.setTable(referenceTable);

            // set slot
            prepareContextSlots(node, context, tupleDescriptor);

            boolean isEqDeleteScan = node.getOpType() != OperatorType.PHYSICAL_ICEBERG_SCAN;
            IcebergScanNode icebergScanNode;
            String planNodeName = isEqDeleteScan ? "IcebergEqualityDeleteScanNode" : "IcebergScanNode";
            if (!isEqDeleteScan) {
                PhysicalIcebergScanOperator op = node.cast();
                icebergScanNode = new IcebergScanNode(context.getNextNodeId(), tupleDescriptor, planNodeName,
                        op.getTableFullMORParams(), op.getMORParams());
                icebergScanNode.updateAppliedDictStringColumns(
                        ((PhysicalIcebergScanOperator) node).getGlobalDicts().stream()
                                .map(entry -> entry.first).collect(Collectors.toSet()));
            } else {
                PhysicalIcebergEqualityDeleteScanOperator op = node.cast();
                icebergScanNode = new IcebergScanNode(context.getNextNodeId(), tupleDescriptor, planNodeName,
                        op.getTableFullMORParams(), op.getMORParams());
            }

            icebergScanNode.setScanOptimzeOption(node.getScanOptimzeOption());
            icebergScanNode.computeStatistics(expression.getStatistics());
            currentExecGroup.add(icebergScanNode, true);
            try {
                // set predicate
                ScalarOperatorToExpr.FormatterContext formatterContext =
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
                List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
                for (ScalarOperator predicate : predicates) {
                    icebergScanNode.getConjuncts()
                            .add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
                }

                ScalarOperator icebergPredicate = !isEqDeleteScan ? node.getPredicate() :
                        ((PhysicalIcebergEqualityDeleteScanOperator) node).getOriginPredicate();
                icebergScanNode.preProcessIcebergPredicate(icebergPredicate);
                icebergScanNode.setSnapshotId(node.getTableVersionRange().end());
                icebergScanNode.setupScanRangeLocations(
                        context.getConnectContext().getSessionVariable().isEnableConnectorIncrementalScanRanges());
                if (!isEqDeleteScan) {
                    HDFSScanNodePredicates scanNodePredicates = icebergScanNode.getScanNodePredicates();
                    prepareMinMaxExpr(scanNodePredicates, node.getScanOperatorPredicates(), context, referenceTable);
                }
            } catch (StarRocksException e) {
                LOG.warn("Iceberg scan node get scan range locations failed : ", e);
                throw new StarRocksPlannerException(e.getMessage(), INTERNAL_ERROR);
            }

            icebergScanNode.setLimit(node.getLimit());
            icebergScanNode.setDataCacheOptions(node.getDataCacheOptions());

            tupleDescriptor.computeMemLayout();
            context.getScanNodes().add(icebergScanNode);

            PlanFragment fragment =
                    new PlanFragment(context.getNextFragmentId(), icebergScanNode, DataPartition.RANDOM);
            if (!isEqDeleteScan) {
                fragment.setQueryGlobalDicts(((PhysicalIcebergScanOperator) node).getGlobalDicts());
                fragment.setQueryGlobalDictExprs(
                        getGlobalDictsExprs(((PhysicalIcebergScanOperator) node).getGlobalDictsExpr(), context));
            }
            context.getFragments().add(fragment);
            return fragment;
        }

        @Override
        public PlanFragment visitPhysicalIcebergMetadataScan(OptExpression optExpression, ExecPlan context) {
            PhysicalIcebergMetadataScanOperator node = (PhysicalIcebergMetadataScanOperator) optExpression.getOp();

            MetadataTable table = (MetadataTable) node.getTable();
            context.getDescTbl().addReferencedTable(table);
            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            tupleDescriptor.setTable(table);

            Column phCol = table.getPlaceHolderColumns().stream().findFirst().get();
            for (Map.Entry<ColumnRefOperator, Column> entry : node.getColRefToColumnMetaMap().entrySet()) {
                if (entry.getValue().getName().equalsIgnoreCase(phCol.getName())) {
                    continue;
                }
                SlotDescriptor slotDescriptor =
                        context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(entry.getKey().getId()));
                slotDescriptor.setColumn(entry.getValue());
                slotDescriptor.setIsNullable(entry.getValue().isAllowNull());
                slotDescriptor.setIsMaterialized(true);
                slotDescriptor.setIsOutputColumn(node.getOutputColumns().contains(entry.getKey()));
                if (slotDescriptor.getOriginType().isComplexType()) {
                    slotDescriptor.setOriginType(entry.getKey().getType());
                    slotDescriptor.setType(entry.getKey().getType());
                }
                context.getColRefToExpr().put(entry.getKey(), new SlotRef(entry.getKey().toString(), slotDescriptor));
            }

            IcebergMetadataScanNode metadataScanNode =
                    new IcebergMetadataScanNode(context.getNextNodeId(), tupleDescriptor,
                            "IcebergMetadataScanNode", node.getTableVersionRange());
            try {
                ScalarOperatorToExpr.FormatterContext formatterContext =
                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
                List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());

                ColumnRefOperator placeHolderOp = new ColumnRefOperator(
                        phCol.getUniqueId(), phCol.getType(), phCol.getName(), phCol.isAllowNull());
                String icebergPredicate = "";

                for (ScalarOperator predicate : predicates) {
                    // exclude iceberg predicate
                    if (isColumnEqualConstant(predicate) && predicate.getChild(0).equivalent(placeHolderOp)) {
                        icebergPredicate = ((ConstantOperator) predicate.getChild(1)).getVarchar();
                    } else {
                        metadataScanNode.getConjuncts().add(
                                ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
                    }
                }

                metadataScanNode.preProcessIcebergPredicate(icebergPredicate);
                metadataScanNode.setupScanRangeLocations();

            } catch (Exception e) {
                LOG.warn("Iceberg metadata scan node get scan range locations failed", e);
                throw new StarRocksPlannerException(e.getMessage(), INTERNAL_ERROR);
            }

            metadataScanNode.setLimit(node.getLimit());

            tupleDescriptor.computeMemLayout();
            context.getScanNodes().add(metadataScanNode);

            PlanFragment fragment =
                    new PlanFragment(context.getNextFragmentId(), metadataScanNode, DataPartition.RANDOM);
            context.getFragments().add(fragment);
            return fragment;
        }

        @Override
        public PlanFragment visitPhysicalSchemaScan(OptExpression optExpression, ExecPlan context) {
            PhysicalSchemaScanOperator node = (PhysicalSchemaScanOperator) optExpression.getOp();

            SystemTable table = (SystemTable) node.getTable();
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

            SchemaScanNode scanNode = new SchemaScanNode(context.getNextNodeId(), tupleDescriptor);

            scanNode.setCatalogName(table.getCatalogName());
            scanNode.setFrontendIP(FrontendOptions.getLocalHostAddress());
            scanNode.setFrontendPort(Config.rpc_port);
            scanNode.setUser(context.getConnectContext().getQualifiedUser());
            scanNode.setUserIp(context.getConnectContext().getRemoteIP());
            scanNode.setLimit(node.getLimit());
            scanNode.setScanOptimzeOption(node.getScanOptimzeOption());
            currentExecGroup.add(scanNode, true);

            // set predicate
            List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());

            for (ScalarOperator origPredicate : predicates) {
                scanNode.getConjuncts().add(ScalarOperatorToExpr.buildExecExpression(origPredicate, formatterContext));
                // if user set table_schema or table_name in where condition and is
                // binary predicate operator, we can set table_schema and table_name
                // into scan-node, which can reduce time from be to fe
                if (origPredicate.getChildren().size() != 2) {
                    continue;
                }
                ScalarOperator predicate = origPredicate.clone();
                ScalarOperator operator0 = origPredicate.getChild(0);
                ScalarOperator operator1 = origPredicate.getChild(1);
                if (operator0 instanceof CastOperator && operator0.getChild(0) instanceof ColumnRefOperator &&
                        operator1 instanceof ConstantOperator) {
                    // select * from information_schema.load_tracking_logs where job_id = "123", job_id is bigint type.
                    // analyzer generates where predicate as [cast (job_id as varchar) = "123"] when
                    // session variable cbo_eq_base_type is varchar, so rewrite the predicate for compatibility.
                    ColumnRefOperator columnRefOperator = (ColumnRefOperator) operator0.getChild(0);
                    if (columnRefOperator.getType().isNumericType() && operator0.getType().isStringType() &&
                            operator1.getType().isStringType()) {
                        predicate.setChild(0, columnRefOperator);
                        try {
                            LiteralExpr literalExpr =
                                    LiteralExpr.create(((ConstantOperator) operator1).getVarchar(),
                                            columnRefOperator.getType());
                            predicate.setChild(1,
                                    ConstantOperator.createObject(literalExpr.getRealObjectValue(),
                                            columnRefOperator.getType()));
                        } catch (AnalysisException e) {
                            throw new SemanticException(
                                    ((ConstantOperator) operator1).getVarchar() + " is not a number");
                        }
                    } else {
                        continue;
                    }
                } else if (!(origPredicate.getChild(0) instanceof ColumnRefOperator &&
                        origPredicate.getChild(1) instanceof ConstantOperator)) {
                    continue;
                }

                ColumnRefOperator columnRefOperator = (ColumnRefOperator) predicate.getChild(0);
                ConstantOperator constantOperator = (ConstantOperator) predicate.getChild(1);
                if (predicate instanceof BinaryPredicateOperator) {
                    BinaryPredicateOperator binaryPredicateOperator = (BinaryPredicateOperator) predicate;
                    if (binaryPredicateOperator.getBinaryType() == BinaryType.EQ) {
                        switch (columnRefOperator.getName()) {
                            case "TABLE_SCHEMA":
                            case "DATABASE_NAME":
                                scanNode.setSchemaDb(constantOperator.getVarchar());
                                break;
                            case "TABLE_NAME":
                                scanNode.setSchemaTable(constantOperator.getVarchar());
                                break;
                            case "BE_ID":
                                scanNode.setBeId(constantOperator.getBigint());
                                break;
                            case "TABLE_ID":
                                scanNode.setTableId(constantOperator.getBigint());
                                break;
                            case "PARTITION_ID":
                                scanNode.setPartitionId(constantOperator.getBigint());
                                break;
                            case "TABLET_ID":
                                scanNode.setTabletId(constantOperator.getBigint());
                                break;
                            case "TXN_ID":
                                scanNode.setTxnId(constantOperator.getBigint());
                                break;
                            case "LABEL":
                                scanNode.setLabel(constantOperator.getVarchar());
                                break;
                            case "JOB_ID":
                                scanNode.setJobId(constantOperator.getBigint());
                                break;
                            case "ID":
                                if (scanNode.getTableName().equalsIgnoreCase(LoadTrackingLogsSystemTable.NAME)
                                        || scanNode.getTableName().equalsIgnoreCase(LoadsSystemTable.NAME)) {
                                    scanNode.setJobId(constantOperator.getBigint());
                                }
                                break;
                            case "TYPE":
                                scanNode.setType(constantOperator.getVarchar());
                                break;
                            case "STATE":
                                scanNode.setState(constantOperator.getVarchar());
                                break;
                            case "LOG":
                                // support full match, `log = 'xxxx'`
                                // TODO: to be fully accurate, need to escape parameter
                                scanNode.setLogPattern("^" + constantOperator.getVarchar() + "$");
                                break;
                            case "LEVEL":
                                scanNode.setLogLevel(constantOperator.getVarchar());
                                break;
                            default:
                                break;
                        }
                    }
                    // support be_logs.timestamp filter
                    if (columnRefOperator.getName().equals("TIMESTAMP")) {
                        BinaryType opType = binaryPredicateOperator.getBinaryType();
                        if (opType == BinaryType.EQ) {
                            scanNode.setLogStartTs(constantOperator.getBigint());
                            scanNode.setLogEndTs(constantOperator.getBigint() + 1);
                        } else if (opType == BinaryType.GT) {
                            scanNode.setLogStartTs(constantOperator.getBigint() + 1);
                        } else if (opType == BinaryType.GE) {
                            scanNode.setLogStartTs(constantOperator.getBigint());
                        } else if (opType == BinaryType.LT) {
                            scanNode.setLogEndTs(constantOperator.getBigint());
                        } else if (opType == BinaryType.LE) {
                            scanNode.setLogEndTs(constantOperator.getBigint() + 1);
                        }
                    }
                } else if (predicate instanceof LikePredicateOperator) {
                    LikePredicateOperator like = (LikePredicateOperator) predicate;
                    // currently, we only optimize `log rlike xxx` or `log regexp xxx`, raise an error if using `like`
                    if (columnRefOperator.getName().equals("LOG")) {
                        if (like.getLikeType() == LikePredicateOperator.LikeType.REGEXP) {
                            scanNode.setLogPattern(((ConstantOperator) like.getChild(1)).getVarchar());
                        } else {
                            throw UnsupportedException.unsupportedException(
                                    "only support `regexp` or `rlike` for log grep");
                        }
                    }
                }
            }

            if (scanNode.getTableName().equalsIgnoreCase(LoadTrackingLogsSystemTable.NAME) && scanNode.getLabel() == null
                    && scanNode.getJobId() == null) {
                throw UnsupportedException.unsupportedException("load_tracking_logs must specify label or job_id");
            }

            if (SystemTable.needQueryFromLeader(scanNode.getTableName())) {
                Pair<String, Integer> ipPort = GlobalStateMgr.getCurrentState().getNodeMgr().getLeaderIpAndRpcPort();
                scanNode.setFrontendIP(ipPort.first);
                scanNode.setFrontendPort(ipPort.second.intValue());
            }

            if (scanNode.getTableName().equalsIgnoreCase(FeMetricsSystemTable.NAME)) {
                scanNode.computeFeNodes();
            }

            if (scanNode.isBeSchemaTable()) {
                scanNode.computeBeScanRanges();
            }

            // set a per node log scan limit to prevent BE/CN OOM
            if (scanNode.getLimit() > 0 && predicates.isEmpty()) {
                scanNode.setLogLimit(Math.min(scanNode.getLimit(), Config.max_per_node_grep_log_limit));
            } else {
                scanNode.setLogLimit(Config.max_per_node_grep_log_limit);
            }

            context.getScanNodes().add(scanNode);
            PlanFragment fragment = new PlanFragment(context.getNextFragmentId(), scanNode,
                    scanNode.isBeSchemaTable() ? DataPartition.RANDOM : DataPartition.UNPARTITIONED);
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

            MysqlScanNode scanNode = new MysqlScanNode(context.getNextNodeId(), tupleDescriptor,
                    (MysqlTable) node.getTable());
            currentExecGroup.add(scanNode, true);

            if (node.getTemporalClause() != null) {
                scanNode.setTemporalClause(node.getTemporalClause());
            }

            // set predicate
            List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
            for (ScalarOperator predicate : predicates) {
                scanNode.getConjuncts().add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
            }

            scanNode.setLimit(node.getLimit());
            scanNode.computeColumnsAndFilters();
            scanNode.computeStatistics(optExpression.getStatistics());
            scanNode.setScanOptimzeOption(node.getScanOptimzeOption());

            context.getScanNodes().add(scanNode);
            PlanFragment fragment =
                    new PlanFragment(context.getNextFragmentId(), scanNode, DataPartition.UNPARTITIONED);
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

            EsScanNode scanNode = new EsScanNode(context.getNextNodeId(), tupleDescriptor, "EsScanNode");
            currentExecGroup.add(scanNode, true);
            // set predicate
            List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());

            for (ScalarOperator predicate : predicates) {
                scanNode.getConjuncts().add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
            }
            scanNode.setLimit(node.getLimit());
            scanNode.computeStatistics(optExpression.getStatistics());
            scanNode.setScanOptimzeOption(node.getScanOptimzeOption());
            try {
                scanNode.assignNodes();
            } catch (StarRocksException e) {
                throw new StarRocksPlannerException(e.getMessage(), INTERNAL_ERROR);
            }
            scanNode.setShardScanRanges(scanNode.computeShardLocations(node.getSelectedIndex()));

            context.getScanNodes().add(scanNode);
            PlanFragment fragment =
                    new PlanFragment(context.getNextFragmentId(), scanNode, DataPartition.RANDOM);
            context.getFragments().add(fragment);
            return fragment;
        }

        @Override
        public PlanFragment visitPhysicalJDBCScan(OptExpression optExpression, ExecPlan context) {
            PhysicalJDBCScanOperator node = (PhysicalJDBCScanOperator) optExpression.getOp();

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

            JDBCScanNode scanNode = new JDBCScanNode(context.getNextNodeId(), tupleDescriptor,
                    (JDBCTable) node.getTable());
            currentExecGroup.add(scanNode, true);

            // set predicate
            List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
            for (ScalarOperator predicate : predicates) {
                scanNode.getConjuncts().add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
            }

            scanNode.setLimit(node.getLimit());
            scanNode.computeColumnsAndFilters();
            scanNode.computeStatistics(optExpression.getStatistics());
            scanNode.setScanOptimzeOption(node.getScanOptimzeOption());
            context.getScanNodes().add(scanNode);
            PlanFragment fragment =
                    new PlanFragment(context.getNextFragmentId(), scanNode, DataPartition.UNPARTITIONED);
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
                EmptySetNode emptyNode = new EmptySetNode(context.getNextNodeId(),
                        Lists.newArrayList(tupleDescriptor.getId()));
                emptyNode.computeStatistics(optExpr.getStatistics());
                currentExecGroup.add(emptyNode, true);
                PlanFragment fragment = new PlanFragment(context.getNextFragmentId(), emptyNode,
                        DataPartition.UNPARTITIONED);
                context.getFragments().add(fragment);
                return fragment;
            } else {
                UnionNode unionNode = new UnionNode(context.getNextNodeId(), tupleDescriptor.getId());
                unionNode.setLimit(valuesOperator.getLimit());
                currentExecGroup.add(unionNode, true);

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

                PlanFragment fragment = new PlanFragment(context.getNextFragmentId(), unionNode,
                        DataPartition.UNPARTITIONED);
                context.getFragments().add(fragment);
                return fragment;
            }
        }

        // return true if all leaf offspring are not ExchangeNode
        public static boolean hasNoExchangeNodes(PlanNode root) {
            if (root instanceof ExchangeNode) {
                return false;
            }
            for (PlanNode childNode : root.getChildren()) {
                if (!hasNoExchangeNodes(childNode)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Whether all the nodes of the plan tree only contain the specific node types.
         *
         * @param root              The plan tree root.
         * @param requiredNodeTypes The specific node type.
         * @return true if all the nodes belong to the node types, otherwise false.
         */
        private boolean onlyContainNodeTypes(PlanNode root, List<Class<? extends PlanNode>> requiredNodeTypes) {
            boolean rootMatched = requiredNodeTypes.stream().anyMatch(type -> type.isInstance(root));
            if (!rootMatched) {
                return false;
            }

            for (PlanNode child : root.getChildren()) {
                if (!onlyContainNodeTypes(child, requiredNodeTypes)) {
                    return false;
                }
            }

            return true;
        }

        /**
         * Remove ExchangeNode between AggNode and ScanNode for the single backend.
         * <p>
         * This is used to generate "ScanNode->LocalShuffle->OnePhaseLocalAgg" for the single backend,
         * which contains two steps:
         * 1. Ignore the network cost for ExchangeNode when estimating cost model.
         * 2. Remove ExchangeNode between AggNode and ScanNode when building fragments.
         * <p>
         * Specifically, transfer
         * (AggNode->ExchangeNode)->([ProjectNode->]ScanNode)
         * -      *inputFragment         sourceFragment
         * to
         * (AggNode->[ProjectNode->]ScanNode)
         * -      *sourceFragment
         * That is, when matching this fragment pattern, remove inputFragment and return sourceFragment.
         *
         * @param inputFragment The input fragment to match the above pattern.
         * @param context       The context of building fragment, which contains all the fragments.
         * @return SourceFragment if it matches th pattern, otherwise the original inputFragment.
         */
        private PlanFragment removeExchangeNodeForLocalShuffleAgg(PlanFragment inputFragment, ExecPlan context) {
            if (ConnectContext.get() == null) {
                return inputFragment;
            }
            if (!canUseLocalShuffleAgg) {
                return inputFragment;
            }
            SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
            boolean enableLocalShuffleAgg = sessionVariable.isEnableLocalShuffleAgg()
                    && sessionVariable.isEnablePipelineEngine()
                    && GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().isSingleBackendAndComputeNode();
            if (!enableLocalShuffleAgg) {
                return inputFragment;
            }

            // InputFragment should match "AggNode->ExchangeNode" pattern, where AggNode is the caller of this method.
            if (!(inputFragment.getPlanRoot() instanceof ExchangeNode)) {
                return inputFragment;
            }

            ExchangeNode node = (ExchangeNode) inputFragment.getPlanRoot();
            if (node.isMerge()) {
                return inputFragment;
            }

            // SourceFragment should match "[ProjectNode->]ScanNode" pattern.
            PlanNode sourceFragmentRoot = inputFragment.getPlanRoot().getChild(0);
            if (!onlyContainNodeTypes(sourceFragmentRoot, ImmutableList.of(ScanNode.class, ProjectNode.class))) {
                return inputFragment;
            }

            // If ExchangeSink is CTE MultiCastPlanFragment, we cannot remove this ExchangeNode.
            PlanFragment sourceFragment = sourceFragmentRoot.getFragment();
            if (sourceFragment instanceof MultiCastPlanFragment) {
                return inputFragment;
            }

            // Traverse fragment in reverse to delete inputFragment,
            // because the last fragment is inputFragment for the most cases.
            ArrayList<PlanFragment> fragments = context.getFragments();
            for (int i = fragments.size() - 1; i >= 0; --i) {
                if (fragments.get(i).equals(inputFragment)) {
                    fragments.remove(i);
                    break;
                }
            }

            sourceFragment.clearDestination();
            sourceFragment.clearOutputPartition();
            return sourceFragment;
        }

        private static class AggregateExprInfo {
            public final ArrayList<Expr> groupExpr;
            public final ArrayList<FunctionCallExpr> aggregateExpr;
            public final ArrayList<Expr> partitionExpr;
            public final ArrayList<Expr> intermediateExpr;

            public final List<Boolean> removeDistinctFlags;

            public AggregateExprInfo(ArrayList<Expr> groupExpr, ArrayList<FunctionCallExpr> aggregateExpr,
                                     ArrayList<Expr> partitionExpr,
                                     ArrayList<Expr> intermediateExpr,
                                     List<Boolean> removeDistinctFlags) {
                this.groupExpr = groupExpr;
                this.aggregateExpr = aggregateExpr;
                this.partitionExpr = partitionExpr;
                this.intermediateExpr = intermediateExpr;
                this.removeDistinctFlags = removeDistinctFlags;
            }
        }

        private AggregateExprInfo buildAggregateTuple(
                Map<ColumnRefOperator, CallOperator> aggregations,
                List<ColumnRefOperator> groupBys,
                List<ColumnRefOperator> partitionBys,
                TupleDescriptor outputTupleDesc,
                ExecPlan context) {
            ArrayList<Expr> groupingExpressions = Lists.newArrayList();
            // EXCHANGE_BYTES/_SPEED aggregate the total bytes/ratio on a node, without grouping, remove group-by here.
            // the group-by expressions just denote the hash distribution of an exchange operator.
            boolean forExchangePerf = aggregations.values().stream().anyMatch(aggFunc ->
                    aggFunc.getFnName().equals(FunctionSet.EXCHANGE_BYTES) ||
                            aggFunc.getFnName().equals(FunctionSet.EXCHANGE_SPEED)) &&
                    ConnectContext.get().getSessionVariable().getNewPlannerAggStage() == 1;
            if (!forExchangePerf) {
                for (ColumnRefOperator grouping : CollectionUtils.emptyIfNull(groupBys)) {
                    Expr groupingExpr = ScalarOperatorToExpr.buildExecExpression(grouping,
                            new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr()));

                    groupingExpressions.add(groupingExpr);

                    SlotDescriptor slotDesc =
                            context.getDescTbl().addSlotDescriptor(outputTupleDesc, new SlotId(grouping.getId()));
                    slotDesc.setType(groupingExpr.getType());
                    slotDesc.setIsNullable(groupingExpr.isNullable());
                    slotDesc.setIsMaterialized(true);
                }
            }

            ArrayList<FunctionCallExpr> aggregateExprList = Lists.newArrayList();
            ArrayList<Expr> intermediateAggrExprs = Lists.newArrayList();
            List<Boolean> removeDistinctFlags = Lists.newArrayList();
            for (Map.Entry<ColumnRefOperator, CallOperator> aggregation : aggregations.entrySet()) {
                FunctionCallExpr aggExpr = (FunctionCallExpr) ScalarOperatorToExpr.buildExecExpression(
                        aggregation.getValue(), new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr()));

                aggregateExprList.add(aggExpr);
                removeDistinctFlags.add(aggregation.getValue().isRemovedDistinct());
                SlotDescriptor slotDesc = context.getDescTbl()
                        .addSlotDescriptor(outputTupleDesc, new SlotId(aggregation.getKey().getId()));
                slotDesc.setType(aggregation.getValue().getType());
                slotDesc.setIsNullable(aggExpr.isNullable());
                slotDesc.setIsMaterialized(true);
                context.getColRefToExpr()
                        .put(aggregation.getKey(), new SlotRef(aggregation.getKey().toString(), slotDesc));

                SlotDescriptor intermediateSlotDesc = new SlotDescriptor(slotDesc.getId(), slotDesc.getParent());
                AggregateFunction aggrFn = (AggregateFunction) aggExpr.getFn();
                Type intermediateType = aggrFn.getIntermediateType() != null ?
                        aggrFn.getIntermediateType() : aggrFn.getReturnType();
                intermediateSlotDesc.setType(intermediateType);
                intermediateSlotDesc.setIsNullable(aggrFn.isNullable());
                intermediateSlotDesc.setIsMaterialized(true);
                SlotRef intermediateSlotRef = new SlotRef(aggregation.getKey().toString(), intermediateSlotDesc);
                intermediateAggrExprs.add(intermediateSlotRef);
            }

            ArrayList<Expr> partitionExpressions = Lists.newArrayList();
            for (ColumnRefOperator column : CollectionUtils.emptyIfNull(partitionBys)) {
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

            return new AggregateExprInfo(groupingExpressions, aggregateExprList, partitionExpressions,
                    intermediateAggrExprs, removeDistinctFlags);
        }

        @Override
        public PlanFragment visitPhysicalHashAggregate(OptExpression optExpr, ExecPlan context) {
            PhysicalHashAggregateOperator node = (PhysicalHashAggregateOperator) optExpr.getOp();
            PlanFragment originalInputFragment = visit(optExpr.inputAt(0), context);

            PlanFragment inputFragment = removeExchangeNodeForLocalShuffleAgg(originalInputFragment, context);
            boolean withLocalShuffle = inputFragment != originalInputFragment || inputFragment.isWithLocalShuffle();

            Map<ColumnRefOperator, CallOperator> aggregations = node.getAggregations();
            List<ColumnRefOperator> groupBys = node.getGroupBys();
            if (MapUtils.isEmpty(aggregations) && CollectionUtils.isEmpty(groupBys)) {
                throw new StarRocksPlannerException(INTERNAL_ERROR, "invalid agg operator " +
                        "without any group by key or agg function. OptExpression:\n%s", optExpr.debugString(5));
            }
            List<ColumnRefOperator> partitionBys = node.getPartitionByColumns();
            boolean hasRemovedDistinct = node.hasRemovedDistinctFunc();

            TupleDescriptor outputTupleDesc = context.getDescTbl().createTupleDescriptor();
            AggregateExprInfo aggExpr =
                    buildAggregateTuple(aggregations, groupBys, partitionBys, outputTupleDesc, context);
            ArrayList<Expr> groupingExpressions = aggExpr.groupExpr;
            ArrayList<FunctionCallExpr> aggregateExprList = aggExpr.aggregateExpr;
            ArrayList<Expr> partitionExpressions = aggExpr.partitionExpr;
            ArrayList<Expr> intermediateAggrExprs = aggExpr.intermediateExpr;

            AggregationNode aggregationNode;
            if (node.getType().isLocal() && node.isSplit()) {
                if (hasRemovedDistinct) {
                    setMergeAggFn(aggregateExprList, aggExpr.removeDistinctFlags);
                }
                AggregateInfo aggInfo = AggregateInfo.create(
                        groupingExpressions,
                        aggregateExprList,
                        outputTupleDesc, outputTupleDesc,
                        AggregateInfo.AggPhase.FIRST);
                aggregationNode =
                        new AggregationNode(context.getNextNodeId(), inputFragment.getPlanRoot(), aggInfo);
                aggregationNode.unsetNeedsFinalize();
                aggregationNode.setIsPreagg(node.canUseStreamingPreAgg());
                aggregationNode.setIntermediateTuple();
                if (!partitionExpressions.isEmpty()) {
                    inputFragment.setOutputPartition(DataPartition.hashPartitioned(partitionExpressions));
                }

                // Check colocate for the first phase in three/four-phase agg whose second phase is pruned.
                if (!withLocalShuffle && node.isMergedLocalAgg() &&
                        hasColocateOlapScanChildInFragment(aggregationNode)) {
                    aggregationNode.setColocate(!node.isWithoutColocateRequirement());
                }
                aggregationNode.setLimit(node.getLimit());
            } else if (node.getType().isGlobal() || (node.getType().isLocal() && !node.isSplit())) {
                // Local && un-split aggregate meanings only execute local pre-aggregation, we need promise
                // output type match other node, so must use `update finalized` phase
                if (hasRemovedDistinct) {
                    setMergeAggFn(aggregateExprList, aggExpr.removeDistinctFlags);
                    AggregateInfo aggInfo = AggregateInfo.create(
                            groupingExpressions,
                            aggregateExprList,
                            outputTupleDesc, outputTupleDesc,
                            AggregateInfo.AggPhase.SECOND);
                    aggregationNode =
                            new AggregationNode(context.getNextNodeId(), inputFragment.getPlanRoot(),
                                    aggInfo);
                } else if (!node.isSplit()) {
                    rewriteAggDistinctFirstStageFunction(aggregateExprList);
                    AggregateInfo aggInfo = AggregateInfo.create(
                            groupingExpressions,
                            aggregateExprList,
                            outputTupleDesc, outputTupleDesc,
                            AggregateInfo.AggPhase.FIRST);
                    aggregationNode =
                            new AggregationNode(context.getNextNodeId(), inputFragment.getPlanRoot(),
                                    aggInfo);
                } else {
                    aggregateExprList.forEach(FunctionCallExpr::setMergeAggFn);
                    AggregateInfo aggInfo = AggregateInfo.create(
                            groupingExpressions,
                            aggregateExprList,
                            outputTupleDesc, outputTupleDesc,
                            AggregateInfo.AggPhase.SECOND_MERGE);
                    aggregationNode =
                            new AggregationNode(context.getNextNodeId(), inputFragment.getPlanRoot(),
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

                // Check colocate for one-phase local agg.
                if (!withLocalShuffle && hasColocateOlapScanChildInFragment(aggregationNode)) {
                    aggregationNode.setColocate(!node.isWithoutColocateRequirement());
                }
            } else if (node.getType().isDistinctGlobal()) {
                aggregateExprList.forEach(FunctionCallExpr::setMergeAggFn);
                AggregateInfo aggInfo = AggregateInfo.create(
                        groupingExpressions,
                        aggregateExprList,
                        outputTupleDesc, outputTupleDesc,
                        AggregateInfo.AggPhase.FIRST_MERGE);
                aggregationNode =
                        new AggregationNode(context.getNextNodeId(), inputFragment.getPlanRoot(), aggInfo);
                aggregationNode.unsetNeedsFinalize();
                aggregationNode.setIntermediateTuple();

                if (!withLocalShuffle && hasColocateOlapScanChildInFragment(aggregationNode)) {
                    aggregationNode.setColocate(!node.isWithoutColocateRequirement());
                }
            } else if (node.getType().isDistinctLocal()) {
                // For SQL: select count(distinct id_bigint), sum(id_int) from test_basic;
                // count function is update function, but sum is merge function
                if (hasRemovedDistinct) {
                    setMergeAggFn(aggregateExprList, aggExpr.removeDistinctFlags);
                }
                AggregateInfo aggInfo = AggregateInfo.create(
                        groupingExpressions,
                        aggregateExprList,
                        outputTupleDesc, outputTupleDesc,
                        AggregateInfo.AggPhase.SECOND);
                aggregationNode =
                        new AggregationNode(context.getNextNodeId(), inputFragment.getPlanRoot(), aggInfo);
                aggregationNode.unsetNeedsFinalize();
                aggregationNode.setIsPreagg(node.canUseStreamingPreAgg());
                aggregationNode.setIntermediateTuple();
            } else {
                throw unsupportedException("Not support aggregate type : " + node.getType());
            }

            aggregationNode.setUseSortAgg(node.isUseSortAgg());
            aggregationNode.setUsePerBucketOptimize(node.isUsePerBucketOptmize());
            aggregationNode.setStreamingPreaggregationMode(node.getNeededPreaggregationMode());
            aggregationNode.setHasNullableGenerateChild();
            aggregationNode.computeStatistics(optExpr.getStatistics());

            if (node.isOnePhaseAgg() || node.isMergedLocalAgg() || node.getType().isDistinctGlobal()) {
                // For ScanNode->LocalShuffle->AggNode, we needn't assign scan ranges per driver sequence.
                inputFragment.setAssignScanRangesPerDriverSeq(!withLocalShuffle);
                inputFragment.setWithLocalShuffleIfTrue(withLocalShuffle);
                aggregationNode.setWithLocalShuffle(withLocalShuffle);
                aggregationNode.setIdenticallyDistributed(true);
            }

            aggregationNode.getAggInfo().setIntermediateAggrExprs(intermediateAggrExprs);
            // enable group execution for:
            // any aggregate stage
            // disable group execution for local shuffle Agg
            boolean useGroupExecution =
                    ConnectContext.get().getSessionVariable().isEnableGroupExecution() && !groupBys.isEmpty() &&
                            !withLocalShuffle;
            if (useGroupExecution) {
                currentExecGroup.setColocateGroup();
            }
            currentExecGroup.add(aggregationNode);
            inputFragment.setPlanRoot(aggregationNode);
            inputFragment.getPlanRoot().forceCollectExecStats();
            inputFragment.mergeQueryDictExprs(originalInputFragment.getQueryGlobalDictExprs());
            inputFragment.mergeQueryGlobalDicts(originalInputFragment.getQueryGlobalDicts());
            return inputFragment;
        }

        // Check whether colocate Table exists in the same Fragment
        public boolean hasColocateOlapScanChildInFragment(PlanNode node) {
            if (node instanceof OlapScanNode) {
                ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
                OlapScanNode scanNode = (OlapScanNode) node;
                if (colocateIndex.isColocateTable(scanNode.getOlapTable().getId())) {
                    return true;
                }
            }
            if (node instanceof ExchangeNode) {
                return false;
            }
            boolean hasOlapScanChild = false;
            for (PlanNode child : node.getChildren()) {
                hasOlapScanChild |= hasColocateOlapScanChildInFragment(child);
            }
            return hasOlapScanChild;
        }

        public void rewriteAggDistinctFirstStageFunction(List<FunctionCallExpr> aggregateExprList) {
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
                    replaceExpr = new FunctionCallExpr(FunctionSet.MULTI_DISTINCT_COUNT, functionCallExpr.getParams());
                    replaceExpr.setFn(Expr.getBuiltinFunction(FunctionSet.MULTI_DISTINCT_COUNT,
                            functionCallExpr.getFn().getArgs(),
                            IS_NONSTRICT_SUPERTYPE_OF));
                    replaceExpr.getParams().setIsDistinct(false);
                    replaceExpr.setType(functionCallExpr.getType());
                } else if (functionName.equalsIgnoreCase(FunctionSet.SUM)) {
                    replaceExpr = new FunctionCallExpr(FunctionSet.MULTI_DISTINCT_SUM, functionCallExpr.getParams());
                    Function multiDistinctSum = DecimalV3FunctionAnalyzer.convertSumToMultiDistinctSum(
                            functionCallExpr.getFn(), functionCallExpr.getChild(0).getType());
                    replaceExpr.setFn(multiDistinctSum);
                    replaceExpr.getParams().setIsDistinct(false);
                    replaceExpr.setType(functionCallExpr.getType());
                } else if (functionName.equals(FunctionSet.ARRAY_AGG)) {
                    replaceExpr = new FunctionCallExpr(FunctionSet.ARRAY_AGG_DISTINCT, functionCallExpr.getParams());
                    replaceExpr.setFn(Expr.getBuiltinFunction(FunctionSet.ARRAY_AGG_DISTINCT,
                            functionCallExpr.getFn().getArgs(),
                            IS_NONSTRICT_SUPERTYPE_OF));
                    replaceExpr.getParams().setIsDistinct(false);
                    replaceExpr.setType(functionCallExpr.getType());
                }

                Preconditions.checkState(replaceExpr != null, functionName + " does not support distinct");
                aggregateExprList.set(singleDistinctIndex, replaceExpr);
            }
        }

        // For SQL: select count(id_int) as a, sum(DISTINCT id_bigint) as b from test_basic group by id_int;
        // sum function is update function, but count is merge function
        private void setMergeAggFn(List<FunctionCallExpr> aggregateExprList, List<Boolean> removeDistinctFlags) {
            for (int i = 0; i < aggregateExprList.size(); i++) {
                if (!removeDistinctFlags.get(i)) {
                    aggregateExprList.get(i).setMergeAggFn();
                }
            }
        }

        @Override
        public PlanFragment visitPhysicalDistribution(OptExpression optExpr, ExecPlan context) {
            ExecGroup lastExecGroup = this.currentExecGroup;
            currentExecGroup = execGroups.newExecGroup();
            PlanFragment inputFragment = visit(optExpr.inputAt(0), context);
            currentExecGroup = lastExecGroup;

            PhysicalDistributionOperator distribution = (PhysicalDistributionOperator) optExpr.getOp();
            ExchangeNode exchangeNode = new ExchangeNode(context.getNextNodeId(),
                    inputFragment.getPlanRoot(), distribution.getDistributionSpec().getType());
            currentExecGroup.add(exchangeNode, true);
            DataPartition dataPartition =
                    translateDistributionToDataPartition(distribution.getDistributionSpec(), context);
            exchangeNode.setDataPartition(dataPartition);

            PlanFragment fragment =
                    new PlanFragment(context.getNextFragmentId(), exchangeNode, dataPartition);
            fragment.setQueryGlobalDicts(distribution.getGlobalDicts());
            fragment.setQueryGlobalDictExprs(getGlobalDictsExprs(distribution.getGlobalDictsExpr(), context));
            inputFragment.setDestination(exchangeNode);
            inputFragment.setOutputPartition(dataPartition);

            context.getFragments().add(fragment);
            return fragment;
        }

        @Override
        public PlanFragment visitPhysicalTopN(OptExpression optExpr, ExecPlan context) {
            ExecGroup lastExecGroup = currentExecGroup;
            currentExecGroup = execGroups.newExecGroup();
            PlanFragment inputFragment = visit(optExpr.inputAt(0), context);
            currentExecGroup = lastExecGroup;

            PhysicalTopNOperator topN = (PhysicalTopNOperator) optExpr.getOp();
            Preconditions.checkState(topN.getOffset() >= 0);
            if (!topN.isSplit()) {
                return buildPartialTopNFragment(optExpr, context, topN.getPartitionByColumns(),
                        topN.getPartitionLimit(), topN.getOrderSpec(),
                        topN.getTopNType(), topN.getLimit(), topN.getOffset(), topN.getPreAggCall(), inputFragment);
            } else {
                return buildFinalTopNFragment(context, topN.getTopNType(), topN.getLimit(), topN.getOffset(),
                        inputFragment, optExpr);
            }
        }

        private PlanFragment buildFinalTopNFragment(ExecPlan context, TopNType topNType, long limit, long offset,
                                                    PlanFragment inputFragment,
                                                    OptExpression optExpr) {
            ExchangeNode exchangeNode = new ExchangeNode(context.getNextNodeId(),
                    inputFragment.getPlanRoot(),
                    DistributionSpec.DistributionType.GATHER);

            DataPartition dataPartition = DataPartition.UNPARTITIONED;
            exchangeNode.setDataPartition(dataPartition);

            Preconditions.checkState(inputFragment.getPlanRoot() instanceof SortNode);
            SortNode sortNode = (SortNode) inputFragment.getPlanRoot();
            sortNode.setTopNType(topNType);
            exchangeNode.setMergeInfo(sortNode.getSortInfo(), offset);
            exchangeNode.computeStatistics(optExpr.getStatistics());
            currentExecGroup.add(exchangeNode, true);

            if (TopNType.ROW_NUMBER.equals(topNType)) {
                exchangeNode.setLimit(limit);
            } else {
                // if TopNType is RANK or DENSE_RANK, the number of rows may be greater than limit
                // So we cannot set limit at exchange
                exchangeNode.unsetLimit();
            }

            PlanFragment fragment =
                    new PlanFragment(context.getNextFragmentId(), exchangeNode, dataPartition);
            inputFragment.setDestination(exchangeNode);
            inputFragment.setOutputPartition(dataPartition);
            fragment.setQueryGlobalDicts(inputFragment.getQueryGlobalDicts());
            fragment.setQueryGlobalDictExprs(inputFragment.getQueryGlobalDictExprs());

            context.getFragments().add(fragment);
            return fragment;
        }

        private PlanFragment buildPartialTopNFragment(OptExpression optExpr, ExecPlan context,
                                                      List<ColumnRefOperator> partitionByColumns, long partitionLimit,
                                                      OrderSpec orderSpec, TopNType topNType, long limit, long offset,
                                                      Map<ColumnRefOperator, CallOperator> preAggFnCalls,
                                                      PlanFragment inputFragment) {
            List<Expr> resolvedTupleExprs = Lists.newArrayList();
            List<Expr> partitionExprs = Lists.newArrayList();
            List<Expr> sortExprs = Lists.newArrayList();
            TupleDescriptor sortTuple = context.getDescTbl().createTupleDescriptor();

            ColumnRefSet outputColumnRefSet = optExpr.inputAt(0).getLogicalProperty().getOutputColumns().clone();

            if (CollectionUtils.isNotEmpty(partitionByColumns)) {
                for (ColumnRefOperator partitionByColumn : partitionByColumns) {
                    Expr expr = ScalarOperatorToExpr.buildExecExpression(partitionByColumn,
                            new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr()));
                    partitionExprs.add(expr);
                }
            }

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

                outputColumnRefSet.except(List.of(ordering.getColumnRef()));
            }

            List<Expr> preAggFnCallExprs = new ArrayList<>();
            List<SlotId> preAggOutputColumnIds = new ArrayList<>();
            TupleDescriptor preAggTuple = context.getDescTbl().createTupleDescriptor();
            if (preAggFnCalls != null) {
                for (Map.Entry<ColumnRefOperator, CallOperator> entry : preAggFnCalls.entrySet()) {
                    Expr preAggFunction = ScalarOperatorToExpr.buildExecExpression(entry.getValue(),
                            new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr()));

                    preAggFnCallExprs.add(preAggFunction);

                    SlotId outputColumnId = new SlotId(entry.getKey().getId());
                    SlotDescriptor slotDesc =
                            context.getDescTbl().addSlotDescriptor(preAggTuple, outputColumnId);
                    slotDesc.initFromExpr(preAggFunction);
                    slotDesc.setIsMaterialized(true);
                    slotDesc.setIsNullable(preAggFunction.isNullable());
                    slotDesc.setType(preAggFunction.getType());
                    context.getColRefToExpr().put(entry.getKey(), new SlotRef(entry.getKey().toString(), slotDesc));

                    //                    resolvedTupleExprs.add(preAggFunction);
                    outputColumnRefSet.except(List.of(entry.getKey()));

                    preAggOutputColumnIds.add(outputColumnId);
                }
            }

            for (int i = 0; i < outputColumnRefSet.getColumnIds().length; ++i) {
                /*
                 * Add column not be used in ordering
                 */
                ColumnRefOperator columnRef = columnRefFactory.getColumnRef(outputColumnRefSet.getColumnIds()[i]);
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

            sortTuple.computeMemLayout();
            SortInfo sortInfo = new SortInfo(partitionExprs, partitionLimit, sortExprs,
                    orderSpec.getOrderDescs().stream().map(Ordering::isAscending).collect(Collectors.toList()),
                    orderSpec.getOrderDescs().stream().map(Ordering::isNullsFirst).collect(Collectors.toList()));
            sortInfo.setMaterializedTupleInfo(sortTuple, resolvedTupleExprs);
            if (preAggFnCalls != null) {
                sortInfo.setPreAggTupleDesc_(preAggTuple);
            }

            SortNode sortNode = new SortNode(
                    context.getNextNodeId(),
                    inputFragment.getPlanRoot(),
                    sortInfo,
                    limit != Operator.DEFAULT_LIMIT,
                    limit == Operator.DEFAULT_LIMIT,
                    0);
            sortNode.setTopNType(topNType);
            sortNode.setPreAggFnCalls(preAggFnCallExprs);
            sortNode.setPreAggOutputColumnId(preAggOutputColumnIds);
            sortNode.setLimit(limit);
            sortNode.setOffset(offset);
            sortNode.resolvedTupleExprs = resolvedTupleExprs;
            sortNode.setHasNullableGenerateChild();
            sortNode.computeStatistics(optExpr.getStatistics());
            currentExecGroup.add(sortNode, true);
            if (shouldBuildGlobalRuntimeFilter()) {
                sortNode.buildRuntimeFilters(runtimeFilterIdIdGenerator, context.getDescTbl(), execGroups);
            }

            inputFragment.setPlanRoot(sortNode);
            return inputFragment;
        }

        private void setJoinPushDown(JoinNode node) {
            // Push down the predicates constructed by the right child when the
            // join op is inner join or left semi join or right join(semi, outer, anti)
            node.setIsPushDown(ConnectContext.get().getSessionVariable().isHashJoinPushDownRightTable()
                    && (node.getJoinOp().isInnerJoin() || node.getJoinOp().isLeftSemiJoin() ||
                    node.getJoinOp().isRightJoin()));
        }

        // when enable_pipeline_engine=true and enable_gloal_runtime_filter=false, global runtime filter
        // also needs be planned, because in pipeline engine, operators need local_rf_waiting_set constructed
        // from global runtime filters to determine local runtime filters generated by which HashJoinNode
        // to be waited to be completed. in this scenario, global runtime filters are built just for
        // obtaining local_rf_waiting_set, so they are cleaned before deliver fragment instances to BEs.
        private boolean shouldBuildGlobalRuntimeFilter() {
            return ConnectContext.get() != null &&
                    (ConnectContext.get().getSessionVariable().getEnableGlobalRuntimeFilter() ||
                            ConnectContext.get().getSessionVariable().isEnablePipelineEngine());
        }

        @Override
        public PlanFragment visitPhysicalHashJoin(OptExpression optExpr, ExecPlan context) {
            PlanFragment leftFragment = visit(optExpr.inputAt(0), context);
            leftFragment.getPlanRoot().forceCollectExecStats();
            ExecGroup leftExecGroup = this.currentExecGroup;
            this.currentExecGroup = execGroups.newExecGroup();
            PlanFragment rightFragment = visit(optExpr.inputAt(1), context);
            rightFragment.getPlanRoot().forceCollectExecStats();
            return visitPhysicalJoin(leftFragment, rightFragment, leftExecGroup, currentExecGroup, optExpr, context);
        }

        private List<Expr> extractConjuncts(ScalarOperator predicate, ExecPlan context) {
            return Utils.extractConjuncts(predicate).stream()
                    .map(e -> ScalarOperatorToExpr.buildExecExpression(e,
                            new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())))
                    .collect(Collectors.toList());
        }

        private void setNullableForJoin(JoinOperator joinOperator,
                                        PlanFragment leftFragment, PlanFragment rightFragment, ExecPlan context) {
            Set<TupleId> nullableTupleIds = new HashSet<>();
            nullableTupleIds.addAll(leftFragment.getPlanRoot().getNullableTupleIds());
            nullableTupleIds.addAll(rightFragment.getPlanRoot().getNullableTupleIds());
            if (joinOperator.isLeftOuterJoin()) {
                nullableTupleIds.addAll(rightFragment.getPlanRoot().getTupleIds());
            } else if (joinOperator.isRightOuterJoin()) {
                nullableTupleIds.addAll(leftFragment.getPlanRoot().getTupleIds());
            } else if (joinOperator.isFullOuterJoin()) {
                nullableTupleIds.addAll(leftFragment.getPlanRoot().getTupleIds());
                nullableTupleIds.addAll(rightFragment.getPlanRoot().getTupleIds());
            }
            for (TupleId tupleId : nullableTupleIds) {
                TupleDescriptor tupleDescriptor = context.getDescTbl().getTupleDesc(tupleId);
                tupleDescriptor.getSlots().forEach(slot -> slot.setIsNullable(true));
                tupleDescriptor.computeMemLayout();
            }
        }

        @Override
        public PlanFragment visitPhysicalNestLoopJoin(OptExpression optExpr, ExecPlan context) {
            PhysicalJoinOperator node = (PhysicalJoinOperator) optExpr.getOp();
            PlanFragment leftFragment = visit(optExpr.inputAt(0), context);
            leftFragment.getPlanRoot().forceCollectExecStats();
            ExecGroup leftExecGroup = this.currentExecGroup;
            this.currentExecGroup = execGroups.newExecGroup();
            PlanFragment rightFragment = visit(optExpr.inputAt(1), context);
            rightFragment.getPlanRoot().forceCollectExecStats();
            this.currentExecGroup = leftExecGroup;

            List<Expr> conjuncts = extractConjuncts(node.getPredicate(), context);
            List<Expr> joinOnConjuncts = extractConjuncts(node.getOnPredicate(), context);
            List<Expr> probePartitionByExprs = Lists.newArrayList();
            DistributionSpec leftDistributionSpec =
                    optExpr.getRequiredProperties().get(0).getDistributionProperty().getSpec();
            DistributionSpec rightDistributionSpec =
                    optExpr.getRequiredProperties().get(1).getDistributionProperty().getSpec();
            if (leftDistributionSpec instanceof HashDistributionSpec &&
                    rightDistributionSpec instanceof HashDistributionSpec) {
                probePartitionByExprs = getShuffleExprs((HashDistributionSpec) leftDistributionSpec, context);
            }

            setNullableForJoin(node.getJoinType(), leftFragment, rightFragment, context);

            NestLoopJoinNode joinNode = new NestLoopJoinNode(context.getNextNodeId(),
                    leftFragment.getPlanRoot(), rightFragment.getPlanRoot(),
                    null, node.getJoinType(), Lists.newArrayList(), joinOnConjuncts);

            joinNode.setLimit(node.getLimit());
            joinNode.computeStatistics(optExpr.getStatistics());
            joinNode.addConjuncts(conjuncts);
            joinNode.setProbePartitionByExprs(probePartitionByExprs);
            currentExecGroup.add(joinNode);
            // Connect parent and child fragment
            rightFragment.getPlanRoot().setFragment(leftFragment);

            // Currently, we always generate new fragment for PhysicalDistribution.
            // So we need to remove exchange node only fragment for Join.
            context.getFragments().remove(rightFragment);

            // Move leftFragment to end, it depends on all of its children
            context.getFragments().remove(leftFragment);
            context.getFragments().add(leftFragment);

            leftFragment.setPlanRoot(joinNode);
            leftFragment.addChildren(rightFragment.getChildren());

            if (!(joinNode.getChild(1) instanceof ExchangeNode)) {
                joinNode.setReplicated(true);
            }

            if (shouldBuildGlobalRuntimeFilter()) {
                joinNode.buildRuntimeFilters(runtimeFilterIdIdGenerator, context.getDescTbl(), execGroups);
            }

            leftFragment.mergeQueryGlobalDicts(rightFragment.getQueryGlobalDicts());
            leftFragment.mergeQueryDictExprs(rightFragment.getQueryGlobalDictExprs());
            return leftFragment;
        }

        @Override
        public PlanFragment visitPhysicalMergeJoin(OptExpression optExpr, ExecPlan context) {
            PlanFragment leftFragment = visit(optExpr.inputAt(0), context);
            ExecGroup leftExecGroup = this.currentExecGroup;
            this.currentExecGroup = execGroups.newExecGroup();
            PlanFragment rightFragment = visit(optExpr.inputAt(1), context);
            PlanNode leftPlanRoot = leftFragment.getPlanRoot();
            PlanNode rightPlanRoot = rightFragment.getPlanRoot();

            OptExpression leftExpression = optExpr.inputAt(0);
            OptExpression rightExpression = optExpr.inputAt(1);

            boolean needDealSort = leftExpression.getInputs().size() > 0 && rightExpression.getInputs().size() > 0;
            if (needDealSort) {
                optExpr.setChild(0, leftExpression.inputAt(0));
                optExpr.setChild(1, rightExpression.inputAt(0));
                leftFragment.setPlanRoot(leftPlanRoot.getChild(0));
                rightFragment.setPlanRoot(rightPlanRoot.getChild(0));
            }

            PlanFragment planFragment =
                    visitPhysicalJoin(leftFragment, rightFragment, leftExecGroup, currentExecGroup, optExpr, context);
            if (needDealSort) {
                leftExpression.setChild(0, optExpr.inputAt(0));
                rightExpression.setChild(0, optExpr.inputAt(1));
                optExpr.setChild(0, leftExpression);
                optExpr.setChild(1, rightExpression);
                planFragment.getPlanRoot().setChild(0, leftPlanRoot);
                planFragment.getPlanRoot().setChild(1, rightPlanRoot);
            }
            return planFragment;
        }

        private List<Expr> getShuffleExprs(HashDistributionSpec hashDistributionSpec, ExecPlan context) {
            List<ColumnRefOperator> shuffleColumns = getShuffleColumns(hashDistributionSpec, columnRefFactory);
            return shuffleColumns.stream().map(e -> ScalarOperatorToExpr.buildExecExpression(e,
                            new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())))
                    .collect(Collectors.toList());
        }

        private PlanFragment visitPhysicalJoin(PlanFragment leftFragment, PlanFragment rightFragment,
                                               ExecGroup leftExecGroup, ExecGroup rightExecGroup,
                                               OptExpression optExpr, ExecPlan context) {
            PhysicalJoinOperator node = (PhysicalJoinOperator) optExpr.getOp();

            JoinOperator joinOperator = node.getJoinType();
            Preconditions.checkState(!joinOperator.isCrossJoin(), "should not be cross join");

            PlanNode leftFragmentPlanRoot = leftFragment.getPlanRoot();
            PlanNode rightFragmentPlanRoot = rightFragment.getPlanRoot();
            // skip decode node
            if (leftFragmentPlanRoot instanceof DecodeNode) {
                leftFragmentPlanRoot = leftFragmentPlanRoot.getChild(0);
            }
            if (rightFragmentPlanRoot instanceof DecodeNode) {
                rightFragmentPlanRoot = rightFragmentPlanRoot.getChild(0);
            }

            // 1. Get distributionMode

            // When left table's distribution is HashPartition, need to record probe's partitionByExprs to
            // compute hash, and then check whether GRF can push down ExchangeNode.
            // TODO(by LiShuMing): Multicolumn-grf generated by colocate HJ and bucket_shuffle HJ also need
            //  be tackled with, because a broadcast HJ can be interpolated between the left-deepmost
            //  OlapScanNode and its ancestor of HJ.
            List<Expr> probePartitionByExprs = Lists.newArrayList();
            DistributionSpec leftDistributionSpec =
                    optExpr.getRequiredProperties().get(0).getDistributionProperty().getSpec();
            DistributionSpec rightDistributionSpec =
                    optExpr.getRequiredProperties().get(1).getDistributionProperty().getSpec();
            if (leftDistributionSpec instanceof HashDistributionSpec &&
                    rightDistributionSpec instanceof HashDistributionSpec) {
                probePartitionByExprs = getShuffleExprs((HashDistributionSpec) leftDistributionSpec, context);
            }

            JoinNode.DistributionMode distributionMode =
                    inferDistributionMode(optExpr, leftFragmentPlanRoot, rightFragmentPlanRoot);
            JoinExprInfo joinExpr = buildJoinExpr(optExpr, context);
            List<Expr> eqJoinConjuncts = joinExpr.eqJoinConjuncts;
            List<Expr> otherJoinConjuncts = joinExpr.otherJoin;
            List<Expr> conjuncts = joinExpr.conjuncts;

            setNullableForJoin(joinOperator, leftFragment, rightFragment, context);

            JoinNode joinNode;
            if (node instanceof PhysicalHashJoinOperator) {
                joinNode = new HashJoinNode(
                        context.getNextNodeId(),
                        leftFragment.getPlanRoot(), rightFragment.getPlanRoot(),
                        joinOperator, eqJoinConjuncts, otherJoinConjuncts);
                UKFKConstraints constraints = optExpr.getConstraints();
                if (constraints != null) {
                    UKFKConstraints.JoinProperty joinProperty = constraints.getJoinProperty();
                    if (joinProperty != null) {
                        joinProperty.buildExpr(context);
                        joinNode.setUkfkProperty(joinProperty);
                    }
                }
                // set skew join, this is used by runtime filter
                PhysicalHashJoinOperator physicalHashJoinOperator = (PhysicalHashJoinOperator) node;
                boolean isSkewJoin = physicalHashJoinOperator.getSkewColumn() != null;
                if (isSkewJoin) {
                    HashJoinNode hashJoinNode = (HashJoinNode) joinNode;
                    hashJoinNode.setSkewJoin(isSkewJoin);
                    if (physicalHashJoinOperator.getSkewJoinFriend().isPresent()) {
                        PhysicalHashJoinOperator skewJoinFriend = physicalHashJoinOperator.getSkewJoinFriend().get();
                        if (distributionMode == JoinNode.DistributionMode.BROADCAST &&
                                context.getJoinNodeMap().containsKey(skewJoinFriend)) {
                            HashJoinNode skewJoinFriendNode = context.getJoinNodeMap().get(skewJoinFriend);
                            // let them know each other
                            hashJoinNode.setSkewJoinFriend(skewJoinFriendNode);
                            skewJoinFriendNode.setSkewJoinFriend(hashJoinNode);
                        }
                    }
                    // right now skew broadcast join's hash code is same as skew shuffle join's
                    // But we first visit shuffle join then visit broadcast join, so using JoinNodeMap here is safe
                    context.getJoinNodeMap().put(physicalHashJoinOperator, hashJoinNode);
                }
            } else if (node instanceof PhysicalMergeJoinOperator) {
                joinNode = new MergeJoinNode(
                        context.getNextNodeId(),
                        leftFragment.getPlanRoot(), rightFragment.getPlanRoot(),
                        joinOperator, eqJoinConjuncts, otherJoinConjuncts);
            } else {
                throw new StarRocksPlannerException("unknown join operator: " + node, INTERNAL_ERROR);
            }

            if (!node.getOutputRequireHashPartition()) {
                joinNode.setCanLocalShuffle(true);
            }

            // Build outputColumns
            fillSlotsInfo(node.getProjection(), joinNode, optExpr);

            joinNode.setDistributionMode(distributionMode);
            joinNode.getConjuncts().addAll(conjuncts);
            joinNode.setLimit(node.getLimit());
            joinNode.computeStatistics(optExpr.getStatistics());
            joinNode.setProbePartitionByExprs(probePartitionByExprs);
            joinNode.setEnableLateMaterialization(ConnectContext.get().getSessionVariable().isJoinLateMaterialization());
            // enable group execution for colocate join
            currentExecGroup = leftExecGroup;
            if (ConnectContext.get().getSessionVariable().isEnableGroupExecution()) {
                if (distributionMode.equals(JoinNode.DistributionMode.COLOCATE) &&
                        !leftExecGroup.isDisableColocateGroup() && !rightExecGroup.isDisableColocateGroup()) {
                    currentExecGroup.setColocateGroup();
                    currentExecGroup.merge(rightExecGroup);
                    execGroups.remove(rightExecGroup);
                } else if (!leftExecGroup.isDisableColocateGroup() &&
                        (distributionMode.equals(JoinNode.DistributionMode.BROADCAST) ||
                                (distributionMode.equals(JoinNode.DistributionMode.LOCAL_HASH_BUCKET) &&
                                        !joinOperator.isRightJoin()))) {
                    // case 1: broadcast join
                    // case 2: local bucket shuffle (no-right) join
                    // do nothing
                } else {
                    // we don't support group execution for other join
                    currentExecGroup.disableColocateGroup(leftFragmentPlanRoot);
                    rightExecGroup.disableColocateGroup(rightFragmentPlanRoot);
                    currentExecGroup.merge(rightExecGroup);
                    execGroups.remove(rightExecGroup);
                }
            }
            currentExecGroup.add(joinNode);

            if (shouldBuildGlobalRuntimeFilter()) {
                joinNode.buildRuntimeFilters(runtimeFilterIdIdGenerator, context.getDescTbl(), execGroups);
            }

            return buildJoinFragment(context, leftFragment, rightFragment, distributionMode, joinNode);
        }

        private boolean isExchangeWithDistributionType(PlanNode node, DistributionSpec.DistributionType expectedType) {
            if (!(node instanceof ExchangeNode)) {
                return false;
            }
            ExchangeNode exchangeNode = (ExchangeNode) node;
            return Objects.equals(exchangeNode.getDistributionType(), expectedType);
        }

        private boolean isColocateJoin(OptExpression optExpression) {
            // through the required properties type check if it is colocate join
            return optExpression.getRequiredProperties().stream().allMatch(
                    physicalPropertySet -> {
                        if (!physicalPropertySet.getDistributionProperty().isShuffle()) {
                            return false;
                        }
                        HashDistributionDesc.SourceType hashSourceType =
                                ((HashDistributionSpec) (physicalPropertySet.getDistributionProperty().getSpec()))
                                        .getHashDistributionDesc().getSourceType();
                        return hashSourceType.equals(HashDistributionDesc.SourceType.LOCAL);
                    });
        }

        public boolean isShuffleJoin(OptExpression optExpression) {
            // through the required properties type check if it is shuffle join
            return optExpression.getRequiredProperties().stream().allMatch(
                    physicalPropertySet -> {
                        if (!physicalPropertySet.getDistributionProperty().isShuffle()) {
                            return false;
                        }
                        HashDistributionDesc.SourceType hashSourceType =
                                ((HashDistributionSpec) (physicalPropertySet.getDistributionProperty().getSpec()))
                                        .getHashDistributionDesc().getSourceType();
                        return hashSourceType.equals(HashDistributionDesc.SourceType.SHUFFLE_JOIN) ||
                                hashSourceType.equals(HashDistributionDesc.SourceType.SHUFFLE_ENFORCE) ||
                                hashSourceType.equals(HashDistributionDesc.SourceType.SHUFFLE_AGG);
                    });
        }

        public PlanFragment computeBucketShufflePlanFragment(ExecPlan context,
                                                             PlanFragment stayFragment,
                                                             PlanFragment removeFragment, JoinNode hashJoinNode) {
            hashJoinNode.setLocalHashBucket(true);
            hashJoinNode.setPartitionExprs(removeFragment.getDataPartition().getPartitionExprs());
            removeFragment.getChild(0)
                    .setOutputPartition(new DataPartition(TPartitionType.BUCKET_SHUFFLE_HASH_PARTITIONED,
                            removeFragment.getDataPartition().getPartitionExprs()));

            // Currently, we always generate new fragment for PhysicalDistribution.
            // So we need to remove exchange node only fragment for Join.
            context.getFragments().remove(removeFragment);

            context.getFragments().remove(stayFragment);
            context.getFragments().add(stayFragment);

            stayFragment.setPlanRoot(hashJoinNode);
            stayFragment.addChildren(removeFragment.getChildren());
            stayFragment.mergeQueryGlobalDicts(removeFragment.getQueryGlobalDicts());
            stayFragment.mergeQueryDictExprs(removeFragment.getQueryGlobalDictExprs());
            return stayFragment;
        }

        public PlanFragment computeShuffleHashBucketPlanFragment(ExecPlan context,
                                                                 PlanFragment stayFragment,
                                                                 PlanFragment removeFragment,
                                                                 JoinNode hashJoinNode) {
            hashJoinNode.setPartitionExprs(removeFragment.getDataPartition().getPartitionExprs());
            DataPartition dataPartition = new DataPartition(TPartitionType.HASH_PARTITIONED,
                    removeFragment.getDataPartition().getPartitionExprs());
            removeFragment.getChild(0).setOutputPartition(dataPartition);

            // Currently, we always generate new fragment for PhysicalDistribution.
            // So we need to remove exchange node only fragment for Join.
            context.getFragments().remove(removeFragment);

            context.getFragments().remove(stayFragment);
            context.getFragments().add(stayFragment);

            stayFragment.setPlanRoot(hashJoinNode);
            stayFragment.addChildren(removeFragment.getChildren());
            stayFragment.mergeQueryGlobalDicts(removeFragment.getQueryGlobalDicts());
            stayFragment.mergeQueryDictExprs(removeFragment.getQueryGlobalDictExprs());
            return stayFragment;
        }

        @Override
        public PlanFragment visitPhysicalAssertOneRow(OptExpression optExpression, ExecPlan context) {
            PlanFragment inputFragment = visit(optExpression.inputAt(0), context);

            // AssertNode will fill null row if child result is empty, should create new tuple use null type column
            for (TupleId id : inputFragment.getPlanRoot().getTupleIds()) {
                context.getDescTbl().getTupleDesc(id).getSlots().forEach(s -> s.setIsNullable(true));
            }

            PhysicalAssertOneRowOperator assertOneRow = (PhysicalAssertOneRowOperator) optExpression.getOp();
            AssertNumRowsNode node =
                    new AssertNumRowsNode(context.getNextNodeId(), inputFragment.getPlanRoot(),
                            new AssertNumRowsElement(assertOneRow.getCheckRows(), assertOneRow.getTips(),
                                    assertOneRow.getAssertion()));
            node.computeStatistics(optExpression.getStatistics());
            currentExecGroup.add(node);
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
                // if local partition topn can preAgg, then it's output is binary format
                // which means analytic node should call function's merge method instead of update
                if (node.isInputIsBinary()) {
                    ((FunctionCallExpr) analyticFunction).setMergeAggFn();
                }
                analyticFnCalls.add(analyticFunction);

                SlotDescriptor slotDesc = context.getDescTbl()
                        .addSlotDescriptor(outputTupleDesc, new SlotId(analyticCall.getKey().getId()));
                slotDesc.setType(analyticFunction.getType());
                slotDesc.setIsNullable(analyticFunction.isNullable());
                slotDesc.setIsMaterialized(true);
                context.getColRefToExpr()
                        .put(analyticCall.getKey(), new SlotRef(analyticCall.getKey().toString(), slotDesc));
            }
            outputTupleDesc.computeMemLayout();

            List<Expr> partitionExprs =
                    node.getPartitionExpressions().stream().map(e -> ScalarOperatorToExpr.buildExecExpression(e,
                                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())))
                            .collect(Collectors.toList());

            List<OrderByElement> orderByElements = node.getOrderByElements().stream().map(e -> new OrderByElement(
                    ScalarOperatorToExpr.buildExecExpression(e.getColumnRef(),
                            new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())),
                    e.isAscending(), e.isNullsFirst())).collect(Collectors.toList());

            AnalyticEvalNode analyticEvalNode = new AnalyticEvalNode(
                    context.getNextNodeId(),
                    inputFragment.getPlanRoot(),
                    analyticFnCalls,
                    partitionExprs,
                    orderByElements,
                    node.getAnalyticWindow(),
                    node.isUseHashBasedPartition(),
                    node.isSkewed(),
                    null, outputTupleDesc, null, null,
                    context.getDescTbl().createTupleDescriptor());
            analyticEvalNode.setSubstitutedPartitionExprs(partitionExprs);
            analyticEvalNode.setLimit(node.getLimit());
            analyticEvalNode.setHasNullableGenerateChild();
            analyticEvalNode.computeStatistics(optExpr.getStatistics());
            currentExecGroup.add(analyticEvalNode);
            if (hasColocateOlapScanChildInFragment(analyticEvalNode)) {
                analyticEvalNode.setColocate(true);
            }

            // set predicate
            List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
            for (ScalarOperator predicate : predicates) {
                analyticEvalNode.getConjuncts()
                        .add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
            }
            passPartitionByToSortNode(context, inputFragment.getPlanRoot(), analyticEvalNode.getPartitionExprs(),
                    node.isSkewed());

            inputFragment.setPlanRoot(analyticEvalNode);
            return inputFragment;
        }

        /**
         * In new planner.
         * Add partition exprs of AnalyticEvalNode to SortNode, it is used in pipeline execution engine
         * to eliminate time-consuming LocalMergeSortSourceOperator and parallelize AnalyticNode.
         */
        private static void passPartitionByToSortNode(ExecPlan context, PlanNode childRoot, List<Expr> partitionExprs,
                                                      boolean isSkewed) {
            SortNode sortNode = null;
            if (childRoot instanceof SortNode) {
                sortNode = (SortNode) childRoot;
                sortNode.setAnalyticPartitionExprs(partitionExprs);
            } else if (childRoot instanceof DecodeNode && childRoot.getChild(0) instanceof SortNode) {
                DecodeNode decodeNode = (DecodeNode) childRoot;
                sortNode = (SortNode) childRoot.getChild(0);

                Map<Integer, Integer> stringIdToDictId = decodeNode.getDictIdToStringIds()
                        .entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
                List<Expr> partitionExprsBeforeDecode = partitionExprs.stream().map(expr -> {
                    if (!(expr instanceof SlotRef)) {
                        return expr;
                    }

                    SlotRef slotRef = (SlotRef) expr;
                    Integer dictSlotId = stringIdToDictId.get(slotRef.getDesc().getId().asInt());
                    if (dictSlotId == null) {
                        return expr;
                    }

                    SlotDescriptor slotDesc = context.getDescTbl().getSlotDesc(new SlotId(dictSlotId));
                    return new SlotRef(slotDesc);
                }).collect(Collectors.toList());

                sortNode.setAnalyticPartitionExprs(partitionExprsBeforeDecode);
            }

            if (sortNode != null) {
                // If the data is skewed, we prefer to perform the standard sort-merge process to enhance performance.
                sortNode.setAnalyticPartitionSkewed(isSkewed);
            }
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
            boolean isUnion = false;
            if (operatorType.equals(OperatorType.PHYSICAL_UNION)) {
                isUnion = true;
                setOperationNode = new UnionNode(context.getNextNodeId(), setOperationTuple.getId());
                setOperationNode.setFirstMaterializedChildIdx_(optExpr.arity());
            } else if (operatorType.equals(OperatorType.PHYSICAL_EXCEPT)) {
                setOperationNode = new ExceptNode(context.getNextNodeId(), setOperationTuple.getId());
            } else if (operatorType.equals(OperatorType.PHYSICAL_INTERSECT)) {
                setOperationNode = new IntersectNode(context.getNextNodeId(), setOperationTuple.getId());
            } else {
                throw new StarRocksPlannerException("Unsupported set operation", INTERNAL_ERROR);
            }
            currentExecGroup.add(setOperationNode, true);

            List<Map<Integer, Integer>> outputSlotIdToChildSlotIdMaps = new ArrayList<>();
            for (int childIdx = 0; childIdx < optExpr.arity(); ++childIdx) {
                Map<Integer, Integer> slotIdMap = new HashMap<>();
                List<ColumnRefOperator> childOutput = setOperation.getChildOutputColumns().get(childIdx);
                Preconditions.checkState(childOutput.size() == setOperation.getOutputColumnRefOp().size());
                for (int columnIdx = 0; columnIdx < setOperation.getOutputColumnRefOp().size(); ++columnIdx) {
                    Integer resultColumnIdx = setOperation.getOutputColumnRefOp().get(columnIdx).getId();
                    slotIdMap.put(resultColumnIdx, childOutput.get(columnIdx).getId());
                }
                outputSlotIdToChildSlotIdMaps.add(slotIdMap);
                Preconditions.checkState(slotIdMap.size() == setOperation.getOutputColumnRefOp().size());
            }
            setOperationNode.setOutputSlotIdToChildSlotIdMaps(outputSlotIdToChildSlotIdMaps);

            Preconditions.checkState(optExpr.getInputs().size() == setOperation.getChildOutputColumns().size());

            PlanFragment setOperationFragment =
                    new PlanFragment(context.getNextFragmentId(), setOperationNode, DataPartition.RANDOM);
            List<List<Expr>> materializedResultExprLists = Lists.newArrayList();

            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
            List<PlanFragment> childFragments = Lists.newArrayList();
            for (int i = 0; i < optExpr.getInputs().size(); i++) {
                List<ColumnRefOperator> childOutput = setOperation.getChildOutputColumns().get(i);
                ExecGroup setExecGroup = this.currentExecGroup;
                this.currentExecGroup = execGroups.newExecGroup();
                PlanFragment fragment = visit(optExpr.getInputs().get(i), context);
                this.currentExecGroup = setExecGroup;

                List<Expr> materializedExpressions = Lists.newArrayList();

                // keep output column order
                for (ColumnRefOperator ref : childOutput) {
                    materializedExpressions.add(ScalarOperatorToExpr.buildExecExpression(ref, formatterContext));
                }

                materializedResultExprLists.add(materializedExpressions);

                if (isUnion) {
                    fragment.setOutputPartition(DataPartition.RANDOM);
                } else {
                    fragment.setOutputPartition(DataPartition.hashPartitioned(materializedExpressions));
                }

                // nothing distribute can satisfy set-operator, must shuffle data
                ExchangeNode exchangeNode =
                        new ExchangeNode(context.getNextNodeId(), fragment.getPlanRoot(), fragment.getDataPartition());
                this.currentExecGroup.add(exchangeNode, true);

                childFragments.add(fragment);
                exchangeNode.setFragment(setOperationFragment);
                setOperationNode.addChild(exchangeNode);
            }

            // keep the fragment child order with execution order
            // for intersect and except. child(0) is build node.
            for (int i = setOperationNode.getChildren().size() - 1; i >= 0; i--) {
                final PlanFragment planFragment = childFragments.get(i);
                planFragment.setDestination((ExchangeNode) setOperationNode.getChild(i));
            }

            // reset column is nullable, for handle union select xx join select xxx...
            setOperationNode.setHasNullableGenerateChild();
            List<Expr> setOutputList = Lists.newArrayList();
            for (int index = 0; index < setOperation.getOutputColumnRefOp().size(); index++) {
                ColumnRefOperator columnRefOperator = setOperation.getOutputColumnRefOp().get(index);
                SlotDescriptor slotDesc = context.getDescTbl().getSlotDesc(new SlotId(columnRefOperator.getId()));
                boolean isNullable = slotDesc.getIsNullable() | setOperationNode.isHasNullableGenerateChild();
                for (List<ColumnRefOperator> childOutputColumn : setOperation.getChildOutputColumns()) {
                    ColumnRefOperator childRef = childOutputColumn.get(index);
                    Expr childExpr = ScalarOperatorToExpr.buildExecExpression(childRef, formatterContext);
                    isNullable |= childExpr.isNullable();
                }
                slotDesc.setIsNullable(isNullable);
                setOutputList.add(new SlotRef(String.valueOf(columnRefOperator.getId()), slotDesc));
            }
            setOperationTuple.computeMemLayout();

            setOperationNode.setSetOperationOutputList(setOutputList);
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

            // RepeatSlotIdList
            List<Set<Integer>> repeatSlotIdList = new ArrayList<>();
            for (List<ColumnRefOperator> repeat : repeatOperator.getRepeatColumnRef()) {
                repeatSlotIdList.add(
                        repeat.stream().map(ColumnRefOperator::getId).collect(Collectors.toSet()));
            }

            RepeatNode repeatNode = new RepeatNode(
                    context.getNextNodeId(),
                    inputFragment.getPlanRoot(),
                    outputGroupingTuple,
                    repeatSlotIdList,
                    repeatOperator.getGroupingIds());
            List<ScalarOperator> predicates = Utils.extractConjuncts(repeatOperator.getPredicate());
            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
            currentExecGroup.add(repeatNode);

            for (ScalarOperator predicate : predicates) {
                repeatNode.getConjuncts().add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
            }
            repeatNode.computeStatistics(optExpr.getStatistics());

            inputFragment.setPlanRoot(repeatNode);
            return inputFragment;
        }

        @Override
        public PlanFragment visitPhysicalFilter(OptExpression optExpr, ExecPlan context) {
            PlanFragment inputFragment = visit(optExpr.inputAt(0), context);
            PhysicalFilterOperator filter = (PhysicalFilterOperator) optExpr.getOp();

            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();

            Map<SlotId, Expr> commonSubOperatorMap = Maps.newHashMap();
            if (filter.getPredicateCommonOperators() != null) {
                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : filter.getPredicateCommonOperators().entrySet()) {
                    Expr expr = ScalarOperatorToExpr.buildExecExpression(entry.getValue(),
                            new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr(),
                                    filter.getPredicateCommonOperators()));

                    commonSubOperatorMap.put(new SlotId(entry.getKey().getId()), expr);

                    SlotDescriptor slotDescriptor =
                            context.getDescTbl().addSlotDescriptor(tupleDescriptor, new SlotId(entry.getKey().getId()));
                    slotDescriptor.setIsNullable(expr.isNullable());
                    slotDescriptor.setIsMaterialized(false);
                    slotDescriptor.setType(expr.getType());
                    context.getColRefToExpr().put(entry.getKey(), new SlotRef(entry.getKey().toString(), slotDescriptor));
                }
            }

            List<Expr> predicates = Utils.extractConjuncts(filter.getPredicate()).stream()
                    .map(d -> ScalarOperatorToExpr.buildExecExpression(d,
                            new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())))
                    .collect(Collectors.toList());

            SelectNode selectNode =
                    new SelectNode(context.getNextNodeId(), inputFragment.getPlanRoot(), predicates);
            selectNode.setLimit(filter.getLimit());
            selectNode.computeStatistics(optExpr.getStatistics());
            selectNode.setCommonSlotMap(commonSubOperatorMap);
            currentExecGroup.add(selectNode);
            inputFragment.setPlanRoot(selectNode);
            return inputFragment;
        }

        @Override
        public PlanFragment visitPhysicalTableFunction(OptExpression optExpression, ExecPlan context) {
            PlanFragment inputFragment = visit(optExpression.inputAt(0), context);
            PhysicalTableFunctionOperator physicalTableFunction = (PhysicalTableFunctionOperator) optExpression.getOp();

            TupleDescriptor udtfOutputTuple = context.getDescTbl().createTupleDescriptor();
            for (ColumnRefOperator columnRefOperator : physicalTableFunction.getOutputColRefs()) {
                SlotDescriptor slotDesc =
                        context.getDescTbl().addSlotDescriptor(udtfOutputTuple, new SlotId(columnRefOperator.getId()));
                slotDesc.setType(columnRefOperator.getType());
                slotDesc.setIsMaterialized(true);
                slotDesc.setIsNullable(columnRefOperator.isNullable());

                context.getColRefToExpr().put(columnRefOperator, new SlotRef(columnRefOperator.toString(), slotDesc));
            }
            udtfOutputTuple.computeMemLayout();

            ColumnRefSet fnResultsRequired = ColumnRefSet.of();
            optExpression.getRowOutputInfo().getColumnRefMap().values()
                    .forEach(expr -> fnResultsRequired.union(expr.getUsedColumns()));
            Optional.ofNullable(physicalTableFunction.getPredicate())
                    .ifPresent(pred -> fnResultsRequired.union(pred.getUsedColumns()));

            // if projection exists, the table function's result may be used by both common sub-expressions and
            // column ref of projections, so all of them must be take into consideration.
            Map<ColumnRefOperator, ScalarOperator> commonSubMap =
                    Optional.ofNullable(physicalTableFunction.getProjection()).map(Projection::getCommonSubOperatorMap)
                            .orElseGet(Collections::emptyMap);
            Map<ColumnRefOperator, ScalarOperator> columnRefMap =
                    Optional.ofNullable(physicalTableFunction.getProjection()).map(Projection::getColumnRefMap)
                            .orElseGet(Collections::emptyMap);
            Stream.concat(commonSubMap.values().stream(), columnRefMap.values().stream())
                    .forEach(expr -> fnResultsRequired.union(expr.getUsedColumns()));

            fnResultsRequired.intersect(physicalTableFunction.getFnResultColRefs());
            TableFunctionNode tableFunctionNode = new TableFunctionNode(context.getNextNodeId(),
                    inputFragment.getPlanRoot(),
                    udtfOutputTuple,
                    physicalTableFunction.getFn(),
                    physicalTableFunction.getFnParamColumnRefs().stream().map(ColumnRefOperator::getId)
                            .collect(Collectors.toList()),
                    physicalTableFunction.getOuterColRefs().stream().map(ColumnRefOperator::getId)
                            .collect(Collectors.toList()),
                    physicalTableFunction.getFnResultColRefs().stream().map(ColumnRefOperator::getId)
                            .collect(Collectors.toList()),
                    !fnResultsRequired.isEmpty() || physicalTableFunction.getOuterColRefs().isEmpty()
            );

            tableFunctionNode.computeStatistics(optExpression.getStatistics());
            tableFunctionNode.setLimit(physicalTableFunction.getLimit());
            currentExecGroup.add(tableFunctionNode);
            inputFragment.setPlanRoot(tableFunctionNode);
            return inputFragment;
        }

        @Override
        public PlanFragment visitPhysicalLimit(OptExpression optExpression, ExecPlan context) {
            // PhysicalLimit use for enforce gather property, Enforcer will produce more PhysicalDistribution
            PlanFragment child = visit(optExpression.inputAt(0), context);
            PhysicalLimitOperator limit = optExpression.getOp().cast();

            // @Todo: support a special node to handle offset
            // now sr only support offset on merge-exchange node
            // 1. if child is exchange node, meanings child was enforced gather property
            //   a. only limit and no offset, only need set limit on child
            //   b. has offset
            //      b.1 not merge exchange should trans Exchange to Merge-Exchange node
            //      b.2 is merge exchange update offset
            // 2. if child isn't exchange node, meanings child satisfy gather property
            //   a. only limit and no offset, no need add exchange node, only need set limit on child
            //   b. has offset, need add exchange node, sr doesn't support a special node to handle offset

            if (limit.hasOffset()) {
                if (!(child.getPlanRoot() instanceof ExchangeNode)) {
                    // use merge-exchange
                    ExchangeNode exchangeNode = new ExchangeNode(context.getNextNodeId(), child.getPlanRoot(),
                            DistributionSpec.DistributionType.GATHER);
                    exchangeNode.setDataPartition(DataPartition.UNPARTITIONED);
                    this.currentExecGroup.add(exchangeNode, true);

                    PlanFragment fragment =
                            new PlanFragment(context.getNextFragmentId(), exchangeNode, DataPartition.UNPARTITIONED);
                    fragment.setQueryGlobalDicts(child.getQueryGlobalDicts());
                    fragment.setQueryGlobalDictExprs(child.getQueryGlobalDictExprs());
                    child.setDestination(exchangeNode);
                    child.setOutputPartition(DataPartition.UNPARTITIONED);

                    context.getFragments().add(fragment);
                    child = fragment;
                }
                ExchangeNode exchangeNode = (ExchangeNode) child.getPlanRoot();
                if (!exchangeNode.isMerge()) {
                    SortInfo sortInfo = new SortInfo(Lists.newArrayList(), Operator.DEFAULT_LIMIT,
                            Lists.newArrayList(new IntLiteral(1)), Lists.newArrayList(true), Lists.newArrayList(false));
                    exchangeNode.setMergeInfo(sortInfo, limit.getOffset());
                } else if (exchangeNode.getOffset() <= 0) {
                    exchangeNode.setOffset(limit.getOffset());
                } else if (exchangeNode.getOffset() > 0) {
                    exchangeNode.setOffset(limit.getOffset() + exchangeNode.getOffset());
                }

                exchangeNode.computeStatistics(optExpression.getStatistics());
            }

            if (limit.hasLimit()) {
                child.getPlanRoot().setLimit(limit.getLimit());
            }
            return child;
        }

        @Override
        public PlanFragment visitPhysicalCTEConsume(OptExpression optExpression, ExecPlan context) {
            PhysicalCTEConsumeOperator consume = (PhysicalCTEConsumeOperator) optExpression.getOp();
            int cteId = consume.getCteId();

            MultiCastPlanFragment cteFragment = (MultiCastPlanFragment) context.getCteProduceFragments().get(cteId);
            ExchangeNode exchangeNode = new ExchangeNode(context.getNextNodeId(),
                    cteFragment.getPlanRoot(), DistributionSpec.DistributionType.SHUFFLE);
            this.currentExecGroup.add(exchangeNode, true);
            exchangeNode.setReceiveColumns(consume.getCteOutputColumnRefMap().values().stream()
                    .map(ColumnRefOperator::getId).distinct().collect(Collectors.toList()));
            exchangeNode.setDataPartition(cteFragment.getDataPartition());
            exchangeNode.forceCollectExecStats();

            PlanFragment consumeFragment = new PlanFragment(context.getNextFragmentId(), exchangeNode,
                    cteFragment.getDataPartition());

            Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
            projectMap.putAll(consume.getCteOutputColumnRefMap());
            consumeFragment = buildProjectNode(optExpression, new Projection(projectMap), consumeFragment, context);
            consumeFragment.setQueryGlobalDicts(cteFragment.getQueryGlobalDicts());
            consumeFragment.setQueryGlobalDictExprs(cteFragment.getQueryGlobalDictExprs());
            consumeFragment.setLoadGlobalDicts(cteFragment.getLoadGlobalDicts());

            // add filter node
            if (consume.getPredicate() != null) {
                List<Expr> predicates = Utils.extractConjuncts(consume.getPredicate()).stream()
                        .map(d -> ScalarOperatorToExpr.buildExecExpression(d,
                                new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())))
                        .collect(Collectors.toList());
                SelectNode selectNode =
                        new SelectNode(context.getNextNodeId(), consumeFragment.getPlanRoot(), predicates);
                this.currentExecGroup.add(selectNode, true);
                selectNode.computeStatistics(optExpression.getStatistics());
                consumeFragment.setPlanRoot(selectNode);
            }

            // set limit
            if (consume.hasLimit()) {
                consumeFragment.getPlanRoot().setLimit(consume.getLimit());
            }

            cteFragment.getDestNodeList().add(exchangeNode);
            consumeFragment.addChild(cteFragment);
            context.getFragments().add(consumeFragment);
            return consumeFragment;
        }

        @Override
        public PlanFragment visitPhysicalCTEProduce(OptExpression optExpression, ExecPlan context) {
            PlanFragment child = visit(optExpression.inputAt(0), context);
            child.getPlanRoot().forceCollectExecStats();
            int cteId = ((PhysicalCTEProduceOperator) optExpression.getOp()).getCteId();
            context.getFragments().remove(child);
            MultiCastPlanFragment cteProduce = new MultiCastPlanFragment(child);

            List<Expr> outputs = Lists.newArrayList();
            optExpression.getOutputColumns().getStream()
                    .forEach(i -> outputs.add(context.getColRefToExpr().get(columnRefFactory.getColumnRef(i))));

            cteProduce.setOutputExprs(outputs);
            context.getCteProduceFragments().put(cteId, cteProduce);
            context.getFragments().add(cteProduce);
            return child;
        }

        @Override
        public PlanFragment visitPhysicalCTEAnchor(OptExpression optExpression, ExecPlan context) {
            visit(optExpression.inputAt(0), context);
            return visit(optExpression.inputAt(1), context);
        }

        @Override
        public PlanFragment visitPhysicalNoCTE(OptExpression optExpression, ExecPlan context) {
            return visit(optExpression.inputAt(0), context);
        }

        static class JoinExprInfo {
            public final List<Expr> eqJoinConjuncts;
            public final List<Expr> otherJoin;
            public final List<Expr> conjuncts;

            public JoinExprInfo(List<Expr> eqJoinConjuncts, List<Expr> otherJoin, List<Expr> conjuncts) {
                this.eqJoinConjuncts = eqJoinConjuncts;
                this.otherJoin = otherJoin;
                this.conjuncts = conjuncts;
            }
        }

        private JoinExprInfo buildJoinExpr(OptExpression optExpr, ExecPlan context) {
            ScalarOperator predicate = optExpr.getOp().getPredicate();
            ScalarOperator onPredicate;
            if (optExpr.getOp() instanceof PhysicalJoinOperator) {
                onPredicate = ((PhysicalJoinOperator) optExpr.getOp()).getOnPredicate();
            } else if (optExpr.getOp() instanceof PhysicalStreamJoinOperator) {
                onPredicate = ((PhysicalStreamJoinOperator) optExpr.getOp()).getOnPredicate();
            } else {
                throw new IllegalStateException("not supported join " + optExpr.getOp());
            }
            List<ScalarOperator> onPredicates = Utils.extractConjuncts(onPredicate);

            ColumnRefSet leftChildColumns = optExpr.inputAt(0).getOutputColumns();
            ColumnRefSet rightChildColumns = optExpr.inputAt(1).getOutputColumns();

            List<BinaryPredicateOperator> eqOnPredicates = JoinHelper.getEqualsPredicate(
                    leftChildColumns, rightChildColumns, onPredicates);
            eqOnPredicates = eqOnPredicates.stream().filter(p -> !p.isCorrelated()).collect(Collectors.toList());
            Preconditions.checkState(!eqOnPredicates.isEmpty(), "must be eq-join");

            for (BinaryPredicateOperator s : eqOnPredicates) {
                if (!optExpr.inputAt(0).getLogicalProperty().getOutputColumns()
                        .containsAll(s.getChild(0).getUsedColumns())) {
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

            List<ScalarOperator> otherJoin = Utils.extractConjuncts(onPredicate);
            otherJoin.removeAll(eqOnPredicates);
            List<Expr> otherJoinConjuncts = otherJoin.stream().map(e -> ScalarOperatorToExpr.buildExecExpression(e,
                            new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())))
                    .collect(Collectors.toList());

            List<ScalarOperator> predicates = Utils.extractConjuncts(predicate);
            List<Expr> conjuncts = predicates.stream().map(e -> ScalarOperatorToExpr.buildExecExpression(e,
                            new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())))
                    .collect(Collectors.toList());

            return new JoinExprInfo(eqJoinConjuncts, otherJoinConjuncts, conjuncts);
        }

        // TODO(murphy) consider state distribution
        @Override
        public PlanFragment visitPhysicalStreamJoin(OptExpression optExpr, ExecPlan context) {
            PhysicalStreamJoinOperator node = (PhysicalStreamJoinOperator) optExpr.getOp();
            PlanFragment leftFragment = visit(optExpr.inputAt(0), context);
            PlanFragment rightFragment = visit(optExpr.inputAt(1), context);

            ColumnRefSet leftChildColumns = optExpr.inputAt(0).getLogicalProperty().getOutputColumns();
            ColumnRefSet rightChildColumns = optExpr.inputAt(1).getLogicalProperty().getOutputColumns();

            if (!node.getJoinType().isInnerJoin()) {
                throw new NotImplementedException("Only inner join is supported");
            }

            JoinOperator joinOperator = node.getJoinType();
            PlanNode leftFragmentPlanRoot = leftFragment.getPlanRoot();
            PlanNode rightFragmentPlanRoot = rightFragment.getPlanRoot();

            // 1. Build distributionMode
            // TODO(murphy): support other distribution mode
            JoinNode.DistributionMode distributionMode = JoinNode.DistributionMode.SHUFFLE_HASH_BUCKET;
            // 2. Build join expression
            JoinExprInfo joinExpr = buildJoinExpr(optExpr, context);
            List<Expr> eqJoinConjuncts = joinExpr.eqJoinConjuncts;
            List<Expr> otherJoinConjuncts = joinExpr.otherJoin;
            List<Expr> conjuncts = joinExpr.conjuncts;

            // 3. Build tuple descriptor
            List<PlanFragment> nullablePlanFragments = new ArrayList<>();
            if (joinOperator.isLeftOuterJoin()) {
                nullablePlanFragments.add(rightFragment);
            } else if (joinOperator.isRightOuterJoin()) {
                nullablePlanFragments.add(leftFragment);
            } else if (joinOperator.isFullOuterJoin()) {
                nullablePlanFragments.add(leftFragment);
                nullablePlanFragments.add(rightFragment);
            }
            for (PlanFragment planFragment : nullablePlanFragments) {
                for (TupleId tupleId : planFragment.getPlanRoot().getTupleIds()) {
                    context.getDescTbl().getTupleDesc(tupleId).getSlots().forEach(slot -> slot.setIsNullable(true));
                }
            }

            JoinNode joinNode =
                    new StreamJoinNode(context.getNextNodeId(), leftFragmentPlanRoot, rightFragmentPlanRoot,
                            node.getJoinType(), eqJoinConjuncts, otherJoinConjuncts);
            currentExecGroup.add(joinNode, true);
            // 4. Build outputColumns
            fillSlotsInfo(node.getProjection(), joinNode, optExpr);

            joinNode.setDistributionMode(distributionMode);
            joinNode.getConjuncts().addAll(conjuncts);
            joinNode.setLimit(node.getLimit());
            joinNode.computeStatistics(optExpr.getStatistics());

            return buildJoinFragment(context, leftFragment, rightFragment, distributionMode, joinNode);
        }

        @NotNull
        private PlanFragment buildJoinFragment(ExecPlan context, PlanFragment leftFragment, PlanFragment rightFragment,
                                               JoinNode.DistributionMode distributionMode, JoinNode joinNode) {
            if (distributionMode.equals(JoinNode.DistributionMode.BROADCAST)) {
                setJoinPushDown(joinNode);

                // Connect parent and child fragment
                rightFragment.getPlanRoot().setFragment(leftFragment);

                // Currently, we always generate new fragment for PhysicalDistribution.
                // So we need to remove exchange node only fragment for Join.
                context.getFragments().remove(rightFragment);

                // Move leftFragment to end, it depends on all of its children
                context.getFragments().remove(leftFragment);
                context.getFragments().add(leftFragment);
                leftFragment.setPlanRoot(joinNode);
                leftFragment.addChildren(rightFragment.getChildren());
                leftFragment.mergeQueryGlobalDicts(rightFragment.getQueryGlobalDicts());
                leftFragment.mergeQueryDictExprs(rightFragment.getQueryGlobalDictExprs());
                return leftFragment;
            } else if (distributionMode.equals(JoinNode.DistributionMode.PARTITIONED)) {
                PlanFragment joinFragment = new PlanFragment(context.getNextFragmentId(),
                        joinNode, leftFragment.getChild(0).getOutputPartition());
                // Currently, we always generate new fragment for PhysicalDistribution.
                // So we need to remove exchange node only fragment for Join.
                mergeChildFragmentsIntoParent(joinFragment, Lists.newArrayList(leftFragment, rightFragment), context);

                context.getFragments().add(joinFragment);

                return joinFragment;
            } else if (distributionMode.equals(JoinNode.DistributionMode.COLOCATE) ||
                    distributionMode.equals(JoinNode.DistributionMode.REPLICATED)) {
                if (distributionMode.equals(JoinNode.DistributionMode.COLOCATE)) {
                    joinNode.setColocate(true, "");
                } else {
                    joinNode.setReplicated(true);
                }
                setJoinPushDown(joinNode);

                joinNode.setChild(0, leftFragment.getPlanRoot());
                joinNode.setChild(1, rightFragment.getPlanRoot());
                leftFragment.setPlanRoot(joinNode);
                leftFragment.addChildren(rightFragment.getChildren());
                context.getFragments().remove(rightFragment);

                context.getFragments().remove(leftFragment);
                context.getFragments().add(leftFragment);
                leftFragment.mergeQueryGlobalDicts(rightFragment.getQueryGlobalDicts());
                leftFragment.mergeQueryDictExprs(rightFragment.getQueryGlobalDictExprs());

                return leftFragment;
            } else if (distributionMode.equals(JoinNode.DistributionMode.SHUFFLE_HASH_BUCKET)) {
                setJoinPushDown(joinNode);

                // distributionMode is SHUFFLE_HASH_BUCKET
                if (!(leftFragment.getPlanRoot() instanceof ExchangeNode) &&
                        !(rightFragment.getPlanRoot() instanceof ExchangeNode)) {
                    joinNode.setChild(0, leftFragment.getPlanRoot());
                    joinNode.setChild(1, rightFragment.getPlanRoot());
                    leftFragment.setPlanRoot(joinNode);
                    leftFragment.addChildren(rightFragment.getChildren());
                    context.getFragments().remove(rightFragment);

                    context.getFragments().remove(leftFragment);
                    context.getFragments().add(leftFragment);

                    leftFragment.mergeQueryGlobalDicts(rightFragment.getQueryGlobalDicts());
                    leftFragment.mergeQueryDictExprs(rightFragment.getQueryGlobalDictExprs());
                    return leftFragment;
                } else if (leftFragment.getPlanRoot() instanceof ExchangeNode &&
                        !(rightFragment.getPlanRoot() instanceof ExchangeNode)) {
                    return computeShuffleHashBucketPlanFragment(context, rightFragment,
                            leftFragment, joinNode);
                } else {
                    return computeShuffleHashBucketPlanFragment(context, leftFragment,
                            rightFragment, joinNode);
                }
            } else {
                setJoinPushDown(joinNode);

                // distributionMode is BUCKET_SHUFFLE
                if (leftFragment.getPlanRoot() instanceof ExchangeNode &&
                        !(rightFragment.getPlanRoot() instanceof ExchangeNode)) {
                    leftFragment = computeBucketShufflePlanFragment(context, rightFragment,
                            leftFragment, joinNode);
                } else {
                    leftFragment = computeBucketShufflePlanFragment(context, leftFragment,
                            rightFragment, joinNode);
                }

                return leftFragment;
            }
        }

        @NotNull
        private JoinNode.DistributionMode inferDistributionMode(OptExpression optExpr, PlanNode leftFragmentPlanRoot,
                                                                PlanNode rightFragmentPlanRoot) {
            JoinNode.DistributionMode distributionMode;
            if (isExchangeWithDistributionType(leftFragmentPlanRoot, DistributionSpec.DistributionType.SHUFFLE) &&
                    isExchangeWithDistributionType(rightFragmentPlanRoot,
                            DistributionSpec.DistributionType.SHUFFLE)) {
                distributionMode = JoinNode.DistributionMode.PARTITIONED;
            } else if (isExchangeWithDistributionType(rightFragmentPlanRoot,
                    DistributionSpec.DistributionType.BROADCAST)) {
                distributionMode = JoinNode.DistributionMode.BROADCAST;
            } else if (!(leftFragmentPlanRoot instanceof ExchangeNode) &&
                    !(rightFragmentPlanRoot instanceof ExchangeNode)) {
                if (isColocateJoin(optExpr)) {
                    distributionMode = HashJoinNode.DistributionMode.COLOCATE;
                } else if (ConnectContext.get().getSessionVariable().isEnableReplicationJoin() &&
                        rightFragmentPlanRoot.canDoReplicatedJoin()) {
                    distributionMode = JoinNode.DistributionMode.REPLICATED;
                } else if (isShuffleJoin(optExpr)) {
                    distributionMode = JoinNode.DistributionMode.SHUFFLE_HASH_BUCKET;
                } else {
                    Preconditions.checkState(false, "Must be colocate/bucket/replicate join");
                    distributionMode = JoinNode.DistributionMode.COLOCATE;
                }
            } else if (isShuffleJoin(optExpr)) {
                distributionMode = JoinNode.DistributionMode.SHUFFLE_HASH_BUCKET;
            } else {
                distributionMode = JoinNode.DistributionMode.LOCAL_HASH_BUCKET;
            }
            return distributionMode;
        }

        @Override
        public PlanFragment visitPhysicalStreamAgg(OptExpression optExpr, ExecPlan context) {
            PhysicalStreamAggOperator node = (PhysicalStreamAggOperator) optExpr.getOp();
            PlanFragment inputFragment = visit(optExpr.inputAt(0), context);
            TupleDescriptor outputTupleDesc = context.getDescTbl().createTupleDescriptor();
            AggregateExprInfo aggExpr =
                    buildAggregateTuple(node.getAggregations(), node.getGroupBys(), null, outputTupleDesc, context);

            // TODO(murphy) refine the aggregate info
            AggregateInfo aggInfo =
                    AggregateInfo.create(aggExpr.groupExpr, aggExpr.aggregateExpr, outputTupleDesc, outputTupleDesc,
                            AggregateInfo.AggPhase.FIRST);
            StreamAggNode aggNode = new StreamAggNode(context.getNextNodeId(), inputFragment.getPlanRoot(), aggInfo);

            aggNode.setHasNullableGenerateChild();
            aggNode.computeStatistics(optExpr.getStatistics());
            currentExecGroup.add(aggNode, true);
            inputFragment.setPlanRoot(aggNode);
            return inputFragment;
        }

        // TODO: distinguish various stream scan node, only binlog scan now
        @Override
        public PlanFragment visitPhysicalStreamScan(OptExpression optExpr, ExecPlan context) {
            PhysicalStreamScanOperator node = (PhysicalStreamScanOperator) optExpr.getOp();
            OlapTable scanTable = (OlapTable) node.getTable();
            context.getDescTbl().addReferencedTable(scanTable);

            TupleDescriptor tupleDescriptor = context.getDescTbl().createTupleDescriptor();
            tupleDescriptor.setTable(scanTable);

            BinlogScanNode binlogScanNode = new BinlogScanNode(context.getNextNodeId(), tupleDescriptor);
            binlogScanNode.computeStatistics(optExpr.getStatistics());
            currentExecGroup.add(binlogScanNode, true);
            try {
                binlogScanNode.computeScanRanges();
            } catch (StarRocksException e) {
                throw new StarRocksPlannerException(
                        "Failed to compute scan ranges for StreamScanNode, " + e.getMessage(), INTERNAL_ERROR);
            }

            // Add slots from table
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
                binlogScanNode.getConjuncts()
                        .add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
            }
            tupleDescriptor.computeMemLayout();

            context.getScanNodes().add(binlogScanNode);
            PlanFragment fragment = new PlanFragment(context.getNextFragmentId(), binlogScanNode, DataPartition.RANDOM);
            context.getFragments().add(fragment);
            return fragment;
        }

        private void fillSlotsInfo(Projection projection, JoinNode joinNode, OptExpression optExpr) {
            ColumnRefSet outputCols = new ColumnRefSet();
            if (projection != null) {
                for (ScalarOperator s : projection.getColumnRefMap().values()) {
                    outputCols.union(s.getUsedColumns());
                }
                for (ScalarOperator s : projection.getCommonSubOperatorMap().values()) {
                    outputCols.union(s.getUsedColumns());
                }
                outputCols.except(new ArrayList<>(projection.getCommonSubOperatorMap().keySet()));
                // when the project doesn't require any cols from join, we just select the first col in the build table
                // of join as the output col for simple
                if (outputCols.isEmpty()) {
                    outputCols.union(optExpr.inputAt(1).getOutputColumns().getFirstId());
                }
                joinNode.setOutputSlots(outputCols.getStream().collect(Collectors.toList()));
            }
        }

        @Override
        public PlanFragment visitPhysicalTableFunctionTableScan(OptExpression optExpression, ExecPlan context) {
            PhysicalTableFunctionTableScanOperator node = (PhysicalTableFunctionTableScanOperator) optExpression.getOp();

            TableFunctionTable table = (TableFunctionTable) node.getTable();

            TupleDescriptor tupleDesc = context.getDescTbl().createTupleDescriptor();
            prepareContextSlots(node, context, tupleDesc);

            List<List<TBrokerFileStatus>> files = new ArrayList<>();
            files.add(table.loadFileList());
            long warehouseId = context.getConnectContext().getCurrentWarehouseId();
            FileScanNode scanNode = new FileScanNode(context.getNextNodeId(), tupleDesc,
                    "FileScanNode", files, table.loadFileList().size(), warehouseId);

            Set<String> scanColumns = tupleDesc.getSlots().stream().map(SlotDescriptor::getColumn).map(Column::getName).collect(
                    Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
            List<BrokerFileGroup> fileGroups = new ArrayList<>();
            try {
                BrokerFileGroup grp = new BrokerFileGroup(table, scanColumns);
                fileGroups.add(grp);
            } catch (StarRocksException e) {
                throw new StarRocksPlannerException(
                        "Build Exec FileScanNode fail, scan info is invalid," + e.getMessage(),
                        INTERNAL_ERROR);
            }

            int dop = ConnectContext.get().getSessionVariable().getSinkDegreeOfParallelism();
            scanNode.setLoadInfo(-1, -1, table, new BrokerDesc(table.getProperties()), fileGroups, table.isStrictMode(), dop);
            scanNode.setUseVectorizedLoad(true);
            scanNode.setFlexibleColumnMapping(table.isFlexibleColumnMapping());
            scanNode.setFileScanType(table.isLoadType() ? TFileScanType.FILES_INSERT : TFileScanType.FILES_QUERY);

            Analyzer analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), context.getConnectContext());
            analyzer.setDescTbl(context.getDescTbl());
            try {
                scanNode.init(analyzer);
                scanNode.finalizeStats(analyzer);
            } catch (StarRocksException e) {
                throw new StarRocksPlannerException(
                        "Build Exec FileScanNode fail, scan info is invalid," + e.getMessage(),
                        INTERNAL_ERROR);
            }

            // set predicate
            List<ScalarOperator> predicates = Utils.extractConjuncts(node.getPredicate());
            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
            for (ScalarOperator predicate : predicates) {
                scanNode.getConjuncts().add(ScalarOperatorToExpr.buildExecExpression(predicate, formatterContext));
            }

            scanNode.setLimit(node.getLimit());
            scanNode.computeStatistics(optExpression.getStatistics());
            currentExecGroup.add(scanNode, true);

            context.getScanNodes().add(scanNode);
            PlanFragment fragment =
                    new PlanFragment(context.getNextFragmentId(), scanNode, DataPartition.RANDOM);
            context.getFragments().add(fragment);
            return fragment;
        }

        @Override
        public PlanFragment visitPhysicalSplitProducer(OptExpression optExpression, ExecPlan context) {
            PlanFragment child = visit(optExpression.inputAt(0), context);
            int splitId = ((PhysicalSplitProduceOperator) optExpression.getOp()).getSplitId();
            context.getFragments().remove(child);
            SplitCastPlanFragment splitProduce = new SplitCastPlanFragment(child);

            context.getSplitProduceFragments().put(splitId, splitProduce);
            context.getFragments().add(splitProduce);

            return splitProduce;
        }

        @Override
        public PlanFragment visitPhysicalSplitConsumer(OptExpression optExpression, ExecPlan context) {
            PhysicalSplitConsumeOperator consumerOperator = (PhysicalSplitConsumeOperator) optExpression.getOp();
            int splitId = consumerOperator.getSplitId();

            SplitCastPlanFragment splitProduceFragment =
                    (SplitCastPlanFragment) context.getSplitProduceFragments().get(splitId);
            ExchangeNode exchangeNode = new ExchangeNode(context.getNextNodeId(),
                    splitProduceFragment.getPlanRoot(), consumerOperator.getDistributionSpec().getType());

            DataPartition dataPartition =
                    translateDistributionToDataPartition(consumerOperator.getDistributionSpec(), context);

            this.currentExecGroup.add(exchangeNode, true);

            exchangeNode.setDataPartition(dataPartition);
            PlanFragment splitConsumeFragment =
                    new PlanFragment(context.getNextFragmentId(), exchangeNode, dataPartition);
            splitConsumeFragment.setQueryGlobalDicts(splitProduceFragment.getQueryGlobalDicts());
            splitConsumeFragment.setQueryGlobalDictExprs(splitProduceFragment.getQueryGlobalDictExprs());
            splitConsumeFragment.setLoadGlobalDicts(splitProduceFragment.getLoadGlobalDicts());
            
            if (consumerOperator.hasLimit()) {
                splitConsumeFragment.getPlanRoot().setLimit(consumerOperator.getLimit());
            }

            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());

            splitProduceFragment.getDestNodeList().add(exchangeNode);
            splitProduceFragment.getOutputPartitions().add(dataPartition);

            splitProduceFragment.getSplitExprs()
                    .add(ScalarOperatorToExpr.buildExecExpression(consumerOperator.getSplitPredicate(),
                            formatterContext));
            splitConsumeFragment.addChild(splitProduceFragment);
            context.getFragments().add(splitConsumeFragment);
            return splitConsumeFragment;
        }

        @Override
        public PlanFragment visitPhysicalConcatenater(OptExpression optExpr, ExecPlan context) {
            PlanFragment leftChild = visit(optExpr.inputAt(0), context);
            PlanFragment rightChild = visit(optExpr.inputAt(1), context);

            PhysicalConcatenateOperator mergeOperator = (PhysicalConcatenateOperator) optExpr.getOp();
            TupleDescriptor mergeOperationTuple = context.getDescTbl().createTupleDescriptor();

            UnionNode setOperationNode = new UnionNode(context.getNextNodeId(), mergeOperationTuple.getId());
            if (rightChild.getPlanRoot() instanceof ExchangeNode) {
                setOperationNode.setLocalExchangeType(LocalExchangerType.DIRECT);
            }

            for (ColumnRefOperator columnRefOperator : mergeOperator.getOutputColumnRefOp()) {
                SlotDescriptor slotDesc = context.getDescTbl()
                        .addSlotDescriptor(mergeOperationTuple, new SlotId(columnRefOperator.getId()));
                slotDesc.setType(columnRefOperator.getType());
                slotDesc.setIsMaterialized(true);
                slotDesc.setIsNullable(columnRefOperator.isNullable());

                context.getColRefToExpr().put(columnRefOperator, new SlotRef(columnRefOperator.toString(), slotDesc));
            }

            // all use union pass through, wchch means just output the input-chunk
            setOperationNode.setFirstMaterializedChildIdx_(optExpr.arity());

            currentExecGroup.add(setOperationNode, true);

            List<Map<Integer, Integer>> outputSlotIdToChildSlotIdMaps = new ArrayList<>();

            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr());
            // materializedResultExprLists is actually useless, since all child is union-passthrough
            // we add it just for pass check
            List<List<Expr>> materializedResultExprLists = Lists.newArrayList();

            for (int childIdx = 0; childIdx < optExpr.arity(); ++childIdx) {
                Map<Integer, Integer> slotIdMap = new HashMap<>();
                List<ColumnRefOperator> childOutput = mergeOperator.getChildOutputColumns().get(childIdx);
                Preconditions.checkState(childOutput.size() == mergeOperator.getOutputColumnRefOp().size());
                for (int columnIdx = 0; columnIdx < mergeOperator.getOutputColumnRefOp().size(); ++columnIdx) {
                    Integer resultColumnIdx = mergeOperator.getOutputColumnRefOp().get(columnIdx).getId();
                    slotIdMap.put(resultColumnIdx, childOutput.get(columnIdx).getId());
                }
                outputSlotIdToChildSlotIdMaps.add(slotIdMap);
                Preconditions.checkState(slotIdMap.size() == mergeOperator.getOutputColumnRefOp().size());

                List<Expr> materializedExpressions = Lists.newArrayList();
                for (ColumnRefOperator ref : childOutput) {
                    materializedExpressions.add(ScalarOperatorToExpr.buildExecExpression(ref, formatterContext));
                }

                materializedResultExprLists.add(materializedExpressions);
            }
            setOperationNode.setOutputSlotIdToChildSlotIdMaps(outputSlotIdToChildSlotIdMaps);
            setOperationNode.setMaterializedResultExprLists_(materializedResultExprLists);

            Preconditions.checkState(optExpr.getInputs().size() == mergeOperator.getChildOutputColumns().size());

            // must add child before create new fragment, otherwise child's fragment won't change.
            setOperationNode.addChild(leftChild.getPlanRoot());
            setOperationNode.addChild(rightChild.getPlanRoot());

            // mergeOperationFragment's data partition is the same as the first child shuffle join 's data partition
            PlanFragment mergeOperationFragment = new PlanFragment(context.getNextFragmentId(), setOperationNode,
                    leftChild.getDataPartition());

            // left plan fragment is shuffle join, and right plan fragment can be broadcast join or exchange
            // we always need to merge these two child plan fragments with mergeOperationFragment
            mergeChildFragmentsIntoParent(mergeOperationFragment, Lists.newArrayList(leftChild, rightChild), context);

            setOperationNode.setLimit(mergeOperator.getLimit());
            setOperationNode.computeStatistics(optExpr.getStatistics());

            context.getFragments().add(mergeOperationFragment);

            return mergeOperationFragment;
        }

        private DataPartition translateDistributionToDataPartition(DistributionSpec distributionSpec,
                                                                   ExecPlan context) {
            DataPartition dataPartition;
            if (DistributionSpec.DistributionType.GATHER.equals(distributionSpec.getType())) {
                dataPartition = DataPartition.UNPARTITIONED;
            } else if (DistributionSpec.DistributionType.BROADCAST
                    .equals(distributionSpec.getType())) {
                dataPartition = DataPartition.UNPARTITIONED;
            } else if (DistributionSpec.DistributionType.SHUFFLE.equals(distributionSpec.getType())) {
                List<ColumnRefOperator> partitionColumns =
                        getShuffleColumns((HashDistributionSpec) distributionSpec, columnRefFactory);
                List<Expr> distributeExpressions =
                        partitionColumns.stream().map(e -> ScalarOperatorToExpr.buildExecExpression(e,
                                        new ScalarOperatorToExpr.FormatterContext(context.getColRefToExpr())))
                                .collect(Collectors.toList());
                dataPartition = DataPartition.hashPartitioned(distributeExpressions);
            } else if (DistributionSpec.DistributionType.ROUND_ROBIN.equals(
                    distributionSpec.getType())) {
                dataPartition = DataPartition.RANDOM;
            } else {
                throw new StarRocksPlannerException("Unsupport exchange type : "
                        + distributionSpec.getType(), INTERNAL_ERROR);
            }
            return dataPartition;
        }

        private void mergeChildFragmentsIntoParent(PlanFragment parentFragment, List<PlanFragment> childFragments,
                                                   ExecPlan context) {
            for (PlanFragment fragment : childFragments) {
                context.getFragments().remove(fragment);
                parentFragment.addChildren(fragment.getChildren());
                parentFragment.mergeQueryGlobalDicts(fragment.getQueryGlobalDicts());
                parentFragment.mergeQueryDictExprs(fragment.getQueryGlobalDictExprs());
            }
        }
    }
}
