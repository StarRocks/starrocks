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

package com.starrocks.sql;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.LogicalSinkMV;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.iceberg.IcebergRowLineageUtils;
import com.starrocks.load.Load;
import com.starrocks.planner.BlackHoleTableSink;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.HiveTableSink;
import com.starrocks.planner.IcebergTableSink;
import com.starrocks.planner.MultiSink;
import com.starrocks.planner.MysqlTableSink;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.TableFunctionTableSink;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.DefaultValueExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.ConnectorSinkShuffleSpec;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.EmptyDistributionProperty;
import com.starrocks.sql.optimizer.base.GatherDistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.base.RoundRobinDistributionSpec;
import com.starrocks.sql.optimizer.base.SortProperty;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.FoldConstantsRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ScalarOperatorRewriteRule;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.OptExprBuilder;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TPartialUpdateMode;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.type.NullType;
import com.starrocks.type.Type;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.starrocks.catalog.DefaultExpr.isValidDefaultFunction;
import static com.starrocks.sql.StatementPlanner.collectSourceTablesCount;
import static com.starrocks.sql.optimizer.rule.mv.MVUtils.MATERIALIZED_VIEW_NAME_PREFIX;

public class InsertPlanner {
    // Only for unit test
    public static boolean enableSingleReplicationShuffle = false;
    private boolean shuffleServiceEnable = false;
    private boolean forceReplicatedStorage = false;
    private boolean useOptimisticLock;
    private PlannerMetaLocker plannerMetaLocker;

    private List<Column> outputBaseSchema;
    private List<Column> outputFullSchema;

    private static final Logger LOG = LogManager.getLogger(InsertPlanner.class);

    public InsertPlanner() {
        this.useOptimisticLock = false;
    }

    public InsertPlanner(PlannerMetaLocker plannerMetaLocker, boolean optimisticLock) {
        this.useOptimisticLock = optimisticLock;
        this.plannerMetaLocker = plannerMetaLocker;
    }

    private enum GenColumnDependency {
        NO_DEPENDENCY,
        NONE_DEPEND_ON_TARGET_COLUMNS,
        ALL_DEPEND_ON_TARGET_COLUMNS,
        PARTIALLY_DEPEND_ON_TARGET_COLUMNS
    }

    // CHMV-P3: build the Fake-Root (CTE + LogicalMultiSink) logical plan for base + MV branches (IDENTITY for
    // now), optimize+translate it, and return the collector ExecPlan (fragments[0] = MultiSinkDispatchNode +
    // MultiSink). Per-branch OlapTableSinks are filled by postProcessMultiSinkSinks. Fully via the optimizer:
    // base load = force-materialized CTE (MultiCastPlanFragment), each branch = leaf CTEConsume.
    private ExecPlan buildFakeRootExecPlan(LogicalPlan logicalPlan, ColumnRefFactory columnRefFactory,
            List<ColumnRefOperator> outputColumns, OlapTable baseTable, InsertStmt insertStmt, ConnectContext session) {
        int cteId = 0;
        TableName catalogDbTable = TableName.fromTableRef(insertStmt.getTableRef());
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(session,
                catalogDbTable.getCatalog(), catalogDbTable.getDb());
        OptExprBuilder baseSource = logicalPlan.getRootBuilder();
        com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator produce =
                new com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator(cteId);
        OptExprBuilder produceBuilder = new OptExprBuilder(produce, Lists.newArrayList(baseSource), null);

        List<OptExprBuilder> branches = Lists.newArrayList();
        List<Long> targetTableIds = Lists.newArrayList();
        List<ColumnRefOperator> allOut = Lists.newArrayList();
        branches.add(buildIdentityConsumer(cteId, outputColumns, columnRefFactory, allOut));
        targetTableIds.add(baseTable.getId());
        for (LogicalSinkMV mv : baseTable.getLogicalSinkMVs().values()) {
            branches.add(buildMvBranch(mv, cteId, outputColumns, baseTable, db, columnRefFactory, session, allOut));
            targetTableIds.add(mv.getTargetTableId());
        }
        com.starrocks.sql.optimizer.operator.logical.LogicalMultiSinkOperator multiSink =
                new com.starrocks.sql.optimizer.operator.logical.LogicalMultiSinkOperator(targetTableIds);
        OptExprBuilder multiSinkBuilder = new OptExprBuilder(multiSink, branches, null);
        com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator anchor =
                new com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator(cteId);
        OptExprBuilder anchorBuilder =
                new OptExprBuilder(anchor, Lists.newArrayList(produceBuilder, multiSinkBuilder), null);
        LogicalPlan fakeRoot = new LogicalPlan(anchorBuilder, allOut, logicalPlan.getCorrelation());

        OptimizerContext oc = OptimizerFactory.initContext(session, columnRefFactory);
        Optimizer opt = OptimizerFactory.create(oc);
        OptExpression optimized = opt.optimize(fakeRoot.getRoot(), new PhysicalPropertySet(),
                new ColumnRefSet(fakeRoot.getOutputColumn()));
        List<String> colNames = allOut.stream().map(ColumnRefOperator::getName).collect(Collectors.toList());
        return PlanFragmentBuilder.createPhysicalPlan(optimized, session, fakeRoot.getOutputColumn(),
                columnRefFactory, colNames, TResultSinkType.MYSQL_PROTOCAL, false);
    }

    // CHMV-P3: after buildFakeRootExecPlan translates the Fake-Root, the collector fragment (fragments[0]) holds
    // a MultiSink recording (destNodeId, targetTableId) per branch but no OlapTableSinks yet. Build one
    // OlapTableSink per target (branch order: 0 = base table, rest = MV targets) against the collector's
    // DescriptorTable and fill them in. Txn/session are available here (unlike during logical construction).
    private void postProcessMultiSinkSinks(ExecPlan execPlan, InsertStmt insertStmt, ConnectContext session,
            OlapTable baseTable) {
        MultiSink multiSink = null;
        for (PlanFragment f : execPlan.getFragments()) {
            if (f.getSink() instanceof MultiSink) {
                multiSink = (MultiSink) f.getSink();
                // The collector root is non-merging: createOutputFragment gave it output_exprs = the
                // UNION of all branch columns, which is meaningless (each branch writes its own table).
                // The BE MultiSink decompose passes fragment.output_exprs to EVERY branch OlapTableSink
                // (data_sink_pipeline.cpp), so a non-empty union mismatches each branch's narrower tuple
                // ("number of exprs is not same with slots"). Clear it: each branch already projects
                // exactly its target columns, so the sink writes the branch chunk as-is (empty exprs).
                f.setOutputExprs(Lists.newArrayList());
                break;
            }
        }
        if (multiSink == null) {
            throw new SemanticException("logical-sink MV collector: MultiSink fragment not found");
        }
        TableName catalogDbTable = TableName.fromTableRef(insertStmt.getTableRef());
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(session,
                catalogDbTable.getCatalog(), catalogDbTable.getDb());
        List<Long> targetTableIds = multiSink.getTargetTableIds();
        List<OlapTableSink> sinks = Lists.newArrayList();
        TUniqueId baseId = session.getExecutionId();
        for (int idx = 0; idx < targetTableIds.size(); idx++) {
            long tid = targetTableIds.get(idx);
            Table target = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tid);
            if (!(target instanceof OlapTable)) {
                throw new SemanticException("logical-sink MV collector: target table " + tid
                        + " is missing or not an OLAP table");
            }
            OlapTable olapTarget = (OlapTable) target;
            DescriptorTable descTbl = execPlan.getDescTbl();
            TupleDescriptor tuple = descTbl.createTupleDescriptor();
            for (Column column : olapTarget.getFullSchema()) {
                SlotDescriptor slot = descTbl.addSlotDescriptor(tuple);
                slot.setIsMaterialized(true);
                slot.setType(column.getType());
                slot.setColumn(column);
                slot.setIsNullable(column.isAllowNull());
            }
            tuple.computeMemLayout();
            OlapTableSink sink = new OlapTableSink(olapTarget, tuple, olapTarget.getAllPartitionIds(),
                    olapTarget.writeQuorum(), olapTarget.enableReplicatedStorage(), false, false,
                    session.getCurrentComputeResource());
            try {
                sink.init(new TUniqueId(baseId.hi + idx, baseId.lo), insertStmt.getTxnId(), db.getId(),
                        session.getExecTimeout());
                sink.complete(null);
            } catch (StarRocksException e) {
                throw new SemanticException(e.getMessage());
            }
            sinks.add(sink);
        }
        multiSink.setOlapSinks(sinks);
    }

    private OptExprBuilder buildIdentityConsumer(int cteId, List<ColumnRefOperator> producerCols,
            ColumnRefFactory crf, List<ColumnRefOperator> allOut) {
        java.util.LinkedHashMap<ColumnRefOperator, ColumnRefOperator> colMap = new java.util.LinkedHashMap<>();
        for (ColumnRefOperator pcol : producerCols) {
            ColumnRefOperator newCol = crf.create(pcol.getName(), pcol.getType(), pcol.isNullable());
            colMap.put(newCol, pcol);
            allOut.add(newCol);
        }
        com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator consume =
                new com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator(cteId, colMap);
        return new OptExprBuilder(consume, Lists.newArrayList(), null);
    }

    // CHMV-P3 (join): build one MV branch by transforming the MV defineSql into a logical subtree (using the
    // MAIN columnRefFactory so its colrefs share the memo with the producer), then replacing the base-table
    // LogicalScan leaf with a leaf LogicalCTEConsume reading the shared force-materialized base payload. The
    // join with dimension tables + the projection to the target columns are left as the transformer built them
    // (base = leftmost table = CH load-trigger; dims = insert-time snapshot). Works for identity MVs too
    // (SELECT cols FROM base -> Project(CTEConsume)).
    private OptExprBuilder buildMvBranch(LogicalSinkMV mv, int cteId, List<ColumnRefOperator> producerCols,
            OlapTable baseTable, Database db, ColumnRefFactory crf, ConnectContext session,
            List<ColumnRefOperator> allOut) {
        List<com.starrocks.sql.ast.StatementBase> stmts =
                com.starrocks.sql.parser.SqlParser.parse(mv.getDefineSql(), session.getSessionVariable());
        com.starrocks.sql.ast.QueryStatement qs = (com.starrocks.sql.ast.QueryStatement) stmts.get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(qs, session);
        QueryRelation qr = qs.getQueryRelation();
        LogicalPlan branchLp = new RelationTransformer(crf, session).transformWithSelectLimit(qr);
        // producer payload cols align 1:1 with base full schema by position.
        java.util.Map<String, ColumnRefOperator> nameToProducerCol = new java.util.HashMap<>();
        List<Column> baseSchema = baseTable.getFullSchema();
        for (int i = 0; i < baseSchema.size(); i++) {
            nameToProducerCol.put(baseSchema.get(i).getName(), producerCols.get(i));
        }
        OptExpression replaced = replaceBaseScanWithCteConsume(branchLp.getRoot(), baseTable.getId(), cteId,
                nameToProducerCol);
        OptExprBuilder branchBuilder = wrapOptExpression(replaced);
        // Cast the branch output columns to the MV target table's column types (mirrors
        // castOutputColumnsTypeToTargetColumns for the main insert). Each branch OlapTableSink has empty
        // output_exprs (writes the branch chunk as-is), so a transform that widens the type (e.g. v*2 -> BIGINT)
        // would send a wider column than the narrower target slot (INT), corrupting the chunk on the
        // branch->collector exchange ("deserialize chunk data failed, row count mismatch").
        Table target = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(),
                mv.getTargetTableId());
        List<Column> targetSchema = ((OlapTable) target).getFullSchema();
        List<ColumnRefOperator> outCols = Lists.newArrayList(branchLp.getOutputColumn());
        java.util.Map<ColumnRefOperator, ScalarOperator> castMap = new java.util.HashMap<>();
        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        List<ScalarOperatorRewriteRule> rules = Arrays.asList(new FoldConstantsRule());
        for (int i = 0; i < targetSchema.size(); i++) {
            Column c = targetSchema.get(i);
            ColumnRefOperator outCol = outCols.get(i);
            if (!c.getType().matchesType(outCol.getType())) {
                ColumnRefOperator nk = crf.create(c.getName(), c.getType(), c.isAllowNull());
                castMap.put(nk, rewriter.rewrite(new CastOperator(c.getType(), outCol, true), rules));
                outCols.set(i, nk);
            } else {
                castMap.put(outCol, outCol);
            }
        }
        branchBuilder = branchBuilder.withNewRoot(new LogicalProjectOperator(new java.util.HashMap<>(castMap)));
        allOut.addAll(outCols);
        return branchBuilder;
    }

    // Recursively rewrite: any LogicalScan over the base table becomes a leaf LogicalCTEConsume whose output
    // colrefs ARE the scan's output colrefs (so the parent join/project is undisturbed), each mapped to the
    // producer payload column of the same base column. Being a leaf (no inputs) triggers CTE force-materialize.
    private OptExpression replaceBaseScanWithCteConsume(OptExpression node, long baseTableId, int cteId,
            java.util.Map<String, ColumnRefOperator> nameToProducerCol) {
        com.starrocks.sql.optimizer.operator.Operator op = node.getOp();
        if (op instanceof com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator) {
            com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator scan =
                    (com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator) op;
            if (scan.getTable().getId() == baseTableId) {
                java.util.LinkedHashMap<ColumnRefOperator, ColumnRefOperator> colMap =
                        new java.util.LinkedHashMap<>();
                for (java.util.Map.Entry<ColumnRefOperator, Column> e :
                        scan.getColRefToColumnMetaMap().entrySet()) {
                    ColumnRefOperator pcol = nameToProducerCol.get(e.getValue().getName());
                    // Skip synthetic scan columns (e.g. _tablet_id_) that are not part of the base payload;
                    // the MV query only references real base columns, so the CTEConsume need not output them.
                    if (pcol == null) {
                        continue;
                    }
                    colMap.put(e.getKey(), pcol);
                }
                return OptExpression.create(
                        new com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator(cteId, colMap));
            }
        }
        List<OptExpression> newInputs = Lists.newArrayList();
        for (OptExpression child : node.getInputs()) {
            newInputs.add(replaceBaseScanWithCteConsume(child, baseTableId, cteId, nameToProducerCol));
        }
        return OptExpression.create(op, newInputs);
    }

    // Wrap a built OptExpression tree back into an OptExprBuilder tree (mapping=null; getRoot() only reads
    // op+inputs) so it can be composed under the Fake-Root LogicalMultiSink.
    private OptExprBuilder wrapOptExpression(OptExpression e) {
        List<OptExprBuilder> inputs = Lists.newArrayList();
        for (OptExpression child : e.getInputs()) {
            inputs.add(wrapOptExpression(child));
        }
        return new OptExprBuilder(e.getOp(), inputs, null);
    }

    private static GenColumnDependency getDependencyType(Column column,
                                                         Set<String> targetColumns,
                                                         Map<ColumnId, Column> allColumns) {
        List<SlotRef> slots = column.getGeneratedColumnRef(allColumns);
        if (slots.isEmpty()) {
            return GenColumnDependency.NO_DEPENDENCY;
        }
        boolean allDependOnTargetColumns = true;
        boolean noneDependOnTargetColumns = true;
        for (SlotRef slot : slots) {
            String originName = slot.getColumnName().toLowerCase();
            if (targetColumns.contains(originName)) {
                noneDependOnTargetColumns = false;
            } else {
                allDependOnTargetColumns = false;
            }
        }
        if (allDependOnTargetColumns) {
            return GenColumnDependency.ALL_DEPEND_ON_TARGET_COLUMNS;
        }
        if (noneDependOnTargetColumns) {
            return GenColumnDependency.NONE_DEPEND_ON_TARGET_COLUMNS;
        }
        return GenColumnDependency.PARTIALLY_DEPEND_ON_TARGET_COLUMNS;
    }

    private static boolean checkIfUseColumnUpsertMode(ConnectContext session,
                                                      InsertStmt insertStmt,
                                                      OlapTable olapTable) {
        String sessionPartialUpdateMode = session.getSessionVariable().getPartialUpdateMode();
        List<String> targetColumnNames = insertStmt.getTargetColumnNames();
        int insertColumnCount = targetColumnNames != null ? targetColumnNames.size() :
                olapTable.getBaseSchemaWithoutGeneratedColumn().size();
        int totalColumnCount = olapTable.getBaseSchemaWithoutGeneratedColumn().size();
        // use COLUMN_UPSERT_MODE when explicitly set to column mode
        if (sessionPartialUpdateMode.equalsIgnoreCase("column")) {
            return true;
        } else if (sessionPartialUpdateMode.equalsIgnoreCase("auto")) {
            // @see com.starrocks.sql.analyzer.UpdateAnalyzer#checkIfUsePartialUpdate
            return insertColumnCount <= 3 && insertColumnCount < totalColumnCount * 0.3;
        }
        return false;
    }

    private void inferOutputSchemaForPartialUpdate(InsertStmt insertStmt) {
        outputBaseSchema = new ArrayList<>();
        outputFullSchema = new ArrayList<>();
        Set<String> legalGeneratedColumnDependencies = new HashSet<>();
        Set<String> outputColumnNames = insertStmt.getTargetColumnNames().stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
        Set<String> baseSchemaNames = insertStmt.getTargetTable().getBaseSchema().stream()
                .map(Column::getName)
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
        OlapTable targetTable = (OlapTable) insertStmt.getTargetTable();
        for (Column column : targetTable.getFullSchema()) {
            String columnName = column.getName().toLowerCase();
            if (outputColumnNames.contains(columnName) || column.isKey()) {
                if (baseSchemaNames.contains(columnName)) {
                    outputBaseSchema.add(column);
                }
                outputFullSchema.add(column);
                legalGeneratedColumnDependencies.add(columnName);
                continue;
            }
            if (column.isAutoIncrement() || column.getDefaultExpr() != null) {
                if (baseSchemaNames.contains(columnName)) {
                    outputBaseSchema.add(column);
                }
                outputFullSchema.add(column);
                continue;
            }
            if (column.isGeneratedColumn()) {
                // check if the generated column only depends on the columns in the output schema
                // if so, add it to the output schema
                // if is not related to target columns at all, skip it (TODO in future)
                // else raise error
                switch (getDependencyType(column, legalGeneratedColumnDependencies, targetTable.getIdToColumn())) {
                    case NO_DEPENDENCY:
                        // should not happen, just skip
                        continue;
                    case ALL_DEPEND_ON_TARGET_COLUMNS:
                        if (baseSchemaNames.contains(columnName)) {
                            outputBaseSchema.add(column);
                        }
                        outputFullSchema.add(column);
                        continue;
                    case NONE_DEPEND_ON_TARGET_COLUMNS: // TODO: handle this case
                    case PARTIALLY_DEPEND_ON_TARGET_COLUMNS:
                        ErrorReport.reportSemanticException(ErrorCode.ERR_MISSING_DEPENDENCY_FOR_GENERATED_COLUMN,
                                column.getName());
                }
            }
            if (column.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX)) {
                String originName = Column.removeNamePrefix(column.getName());
                if (outputColumnNames.contains(originName.toLowerCase())) {
                    if (baseSchemaNames.contains(column.getName())) {
                        outputBaseSchema.add(column);
                    }
                    outputFullSchema.add(column);
                }
                continue;
            }
        }
    }

    public ExecPlan plan(InsertStmt insertStmt, ConnectContext session) {
        QueryRelation queryRelation = insertStmt.getQueryStatement().getQueryRelation();
        List<ColumnRefOperator> outputColumns = new ArrayList<>();
        Table targetTable = insertStmt.getTargetTable();

        if (insertStmt.usePartialUpdate()) {
            inferOutputSchemaForPartialUpdate(insertStmt);
        } else {
            outputBaseSchema = targetTable.getBaseSchema();
            outputFullSchema = targetTable.getFullSchema();
        }

        if (targetTable.isIcebergTable()) {
            boolean preserveRowLineage = targetTable instanceof IcebergTable
                    && IcebergRowLineageUtils.shouldWriteRowLineageColumns(insertStmt, (IcebergTable) targetTable);
            Set<String> columnsToFilter = preserveRowLineage
                    ? IcebergTable.ICEBERG_META_COLUMNS.stream()
                    .filter(col -> !col.equals(IcebergTable.ROW_ID)
                            && !col.equals(IcebergTable.LAST_UPDATED_SEQUENCE_NUMBER))
                    .collect(Collectors.toSet())
                    : IcebergTable.ICEBERG_META_COLUMNS;
            outputBaseSchema = outputBaseSchema.stream().filter(col ->
                    !columnsToFilter.contains(col.getName())).toList();
            outputFullSchema = outputFullSchema.stream().filter(col ->
                    !columnsToFilter.contains(col.getName())).toList();
        }

        //1. Process the literal value of the insert values type and cast it into the type of the target table
        if (queryRelation instanceof ValuesRelation) {
            castLiteralToTargetColumnsType(insertStmt);
        }

        //2. Build Logical plan
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan;
        try (Timer ignore = Tracers.watchScope("Transform")) {
            logicalPlan = new RelationTransformer(columnRefFactory, session).transform(queryRelation);
        }

        //3. Fill in the default value and NULL
        OptExprBuilder optExprBuilder = fillDefaultValue(logicalPlan, columnRefFactory, insertStmt, outputColumns);

        //4. Fill key partition columns constant value for data lake format table (hive/iceberg/hudi/delta_lake)
        if (insertStmt.isSpecifyKeyPartition()) {
            optExprBuilder = fillKeyPartitionsColumn(columnRefFactory, insertStmt, outputColumns, optExprBuilder);
        }

        //5. Fill in the generated columns
        optExprBuilder = fillGeneratedColumns(columnRefFactory, insertStmt, outputColumns, optExprBuilder, session);

        //6. Fill in the shadow column
        optExprBuilder = fillShadowColumns(columnRefFactory, insertStmt, outputColumns, optExprBuilder, session);

        //7. Cast output columns type to target type
        optExprBuilder =
                castOutputColumnsTypeToTargetColumns(columnRefFactory, insertStmt, outputColumns, optExprBuilder);

        //8. Optimize logical plan and build physical plan
        logicalPlan = new LogicalPlan(optExprBuilder, outputColumns, logicalPlan.getCorrelation());


        // TODO: remove forceDisablePipeline when all the operators support pipeline engine.
        SessionVariable currentVariable = (SessionVariable) session.getSessionVariable().clone();
        session.setSessionVariable(currentVariable);
        boolean isEnablePipeline = session.getSessionVariable().isEnablePipelineEngine();
        boolean canUsePipeline = isEnablePipeline && DataSink.canTableSinkUsePipeline(targetTable);
        boolean forceDisablePipeline = isEnablePipeline && !canUsePipeline;
        boolean enableMVRewrite = currentVariable.isEnableMaterializedViewRewriteForInsert() &&
                currentVariable.isEnableMaterializedViewRewrite();
        try (Timer ignore = Tracers.watchScope("InsertPlanner")) {
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(false);
            }
            // Non-query must use the strategy assign scan ranges per driver sequence, which local shuffle agg cannot use.
            session.getSessionVariable().setEnableLocalShuffleAgg(false);
            session.getSessionVariable().setEnableMaterializedViewRewrite(enableMVRewrite);

            boolean fakeRootCollector = targetTable instanceof OlapTable
                    && ((OlapTable) targetTable).hasLogicalSinkMV();
            if (fakeRootCollector) {
                // INSERT OVERWRITE relies on temp-partition swap on the base table, which the Fake-Root
                // base branch sink does not honor (it would empty the base while appending to the MV
                // targets -- base data loss). CK-compatible logical-sink MVs are insert-triggered only.
                if (insertStmt.isOverwrite() || insertStmt.isFromOverwrite()) {
                    throw new SemanticException("INSERT OVERWRITE is not supported on a base table with "
                            + "CK-compatible logical-sink materialized views (insert-triggered only); "
                            + "use INSERT INTO");
                }
                ExecPlan collectorPlan = buildFakeRootExecPlan(logicalPlan, columnRefFactory, outputColumns,
                        (OlapTable) targetTable, insertStmt, session);
                postProcessMultiSinkSinks(collectorPlan, insertStmt, session, (OlapTable) targetTable);
                return collectorPlan;
            }
            ExecPlan execPlan =
                    useOptimisticLock ?
                            buildExecPlanWithRetry(insertStmt, session, outputColumns, logicalPlan, columnRefFactory,
                                    queryRelation, targetTable) :
                            buildExecPlan(insertStmt, session, outputColumns, logicalPlan, columnRefFactory,
                                    queryRelation,
                                    targetTable);

            DescriptorTable descriptorTable = execPlan.getDescTbl();
            TupleDescriptor tupleDesc = descriptorTable.createTupleDescriptor();

            List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();
            long tableId = targetTable.getId();
            for (Column column : outputFullSchema) {
                SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(tupleDesc);
                slotDescriptor.setIsMaterialized(true);
                slotDescriptor.setType(column.getType());
                slotDescriptor.setColumn(column);
                slotDescriptor.setIsNullable(column.isAllowNull());
                if (column.getType().isVarchar() &&
                        IDictManager.getInstance().hasGlobalDict(tableId, column.getColumnId())) {
                    Optional<ColumnDict> dict = IDictManager.getInstance().getGlobalDict(tableId, column.getColumnId());
                    dict.ifPresent(
                            columnDict -> globalDicts.add(new Pair<>(slotDescriptor.getId().asInt(), columnDict)));
                }
                // TODO: attach the dict for JSON
            }
            tupleDesc.computeMemLayout();

            DataSink dataSink;
            if (targetTable instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) targetTable;
                boolean enableAutomaticPartition;
                List<Long> targetPartitionIds = insertStmt.getTargetPartitionIds();
                if (insertStmt.isSystem() && insertStmt.isPartitionNotSpecifiedInOverwrite()) {
                    Preconditions.checkState(!CollectionUtils.isEmpty(targetPartitionIds));
                    enableAutomaticPartition = olapTable.supportedAutomaticPartition();
                } else if (insertStmt.isDynamicOverwrite()) {
                    Preconditions.checkState(CollectionUtils.isEmpty(targetPartitionIds));
                    enableAutomaticPartition = olapTable.supportedAutomaticPartition();
                } else if (insertStmt.isSpecifyPartitionNames()) {
                    Preconditions.checkState(!CollectionUtils.isEmpty(targetPartitionIds));
                    enableAutomaticPartition = false;
                } else if (insertStmt.isStaticKeyPartitionInsert()) {
                    enableAutomaticPartition = false;
                } else {
                    Preconditions.checkState(!CollectionUtils.isEmpty(targetPartitionIds));
                    enableAutomaticPartition = olapTable.supportedAutomaticPartition();
                }
                boolean nullExprInAutoIncrement = false;
                // In INSERT INTO SELECT, if AUTO_INCREMENT column
                // is specified, the column values must not be NULL
                if (!(queryRelation instanceof ValuesRelation) &&
                        !(targetTable instanceof MaterializedView)) {
                    boolean specifyAutoIncrementColumn = false;
                    if (insertStmt.getTargetColumnNames() != null) {
                        for (String colName : insertStmt.getTargetColumnNames()) {
                            for (Column col : olapTable.getBaseSchema()) {
                                if (col.isAutoIncrement() && col.getName().equals(colName)) {
                                    specifyAutoIncrementColumn = true;
                                    break;
                                }
                            }
                        }
                    }
                    if (insertStmt.getTargetColumnNames() == null || specifyAutoIncrementColumn) {
                        nullExprInAutoIncrement = true;
                    }

                }
                dataSink = new OlapTableSink(olapTable, tupleDesc, targetPartitionIds,
                        olapTable.writeQuorum(),
                        forceReplicatedStorage ? true : olapTable.enableReplicatedStorage(),
                        nullExprInAutoIncrement, enableAutomaticPartition, session.getCurrentComputeResource());
                if (insertStmt.usePartialUpdate()) {
                    TPartialUpdateMode partialUpdateMode = TPartialUpdateMode.AUTO_MODE;
                    if (checkIfUseColumnUpsertMode(session, insertStmt, olapTable)) {
                        partialUpdateMode = TPartialUpdateMode.COLUMN_UPSERT_MODE;
                    }
                    ((OlapTableSink) dataSink).setPartialUpdateMode(partialUpdateMode);
                    if (insertStmt.autoIncrementPartialUpdate()) {
                        ((OlapTableSink) dataSink).setMissAutoIncrementColumn();
                    }
                }
                if (olapTable.getAutomaticBucketSize() > 0) {
                    ((OlapTableSink) dataSink).setAutomaticBucketSize(olapTable.getAutomaticBucketSize());
                }
                if (insertStmt.isDynamicOverwrite()) {
                    ((OlapTableSink) dataSink).setDynamicOverwrite(true);
                }
                if (insertStmt.isFromOverwrite()) {
                    ((OlapTableSink) dataSink).setIsFromOverwrite(true);
                }
                if (session.getTxnId() != 0) {
                    ((OlapTableSink) dataSink).setIsMultiStatementsTxn(true);
                }
                if (insertStmt.getTargetWriteIndexId() != null) {
                    ((OlapTableSink) dataSink).setTargetWriteIndexId(insertStmt.getTargetWriteIndexId());
                }

                // if sink is OlapTableSink Assigned to Be execute this sql [cn execute OlapTableSink will crash]
                session.getSessionVariable().setPreferComputeNode(false);
                session.getSessionVariable().setUseComputeNodes(0);
                OlapTableSink olapTableSink = (OlapTableSink) dataSink;
                TableRef tableRef = insertStmt.getTableRef();
                TableName catalogDbTable = TableName.fromTableRef(tableRef);
                Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(session, catalogDbTable.getCatalog(),
                        catalogDbTable.getDb());
                try {
                    Load.checkMergeCondition(insertStmt.getMergingCondition(), olapTable, outputFullSchema,
                            ((OlapTableSink) dataSink).missAutoIncrementColumn());
                    olapTableSink.init(session.getExecutionId(), insertStmt.getTxnId(), db.getId(), session.getExecTimeout());
                    olapTableSink.complete(insertStmt.getMergingCondition());
                } catch (StarRocksException e) {
                    throw new SemanticException(e.getMessage());
                }
                // CK-compatible logical-sink MVs are routed unconditionally through the Fake-Root collector
                // (buildFakeRootExecPlan), which returns earlier in this method before reaching here.
            } else if (insertStmt.getTargetTable() instanceof MysqlTable) {
                dataSink = new MysqlTableSink((MysqlTable) targetTable);
            } else if (targetTable instanceof IcebergTable) {
                descriptorTable.addReferencedTable(targetTable);
                dataSink = new IcebergTableSink((IcebergTable) targetTable, tupleDesc,
                        isKeyPartitionStaticInsert(insertStmt, queryRelation), session.getSessionVariable(),
                        insertStmt.getTargetBranch());
            } else if (targetTable instanceof HiveTable) {
                dataSink = new HiveTableSink((HiveTable) targetTable, tupleDesc,
                        isKeyPartitionStaticInsert(insertStmt, queryRelation), session.getSessionVariable());
            } else if (targetTable instanceof TableFunctionTable) {
                dataSink = new TableFunctionTableSink((TableFunctionTable) targetTable);
            } else if (targetTable.isBlackHoleTable()) {
                dataSink = new BlackHoleTableSink();
            } else {
                throw new SemanticException("Unknown table type " + insertStmt.getTargetTable().getType());
            }

            // enable spill for connector sink
            if (session.getSessionVariable().isEnableConnectorSinkSpill() && (targetTable instanceof IcebergTable
                    || targetTable instanceof HiveTable || targetTable instanceof TableFunctionTable)) {
                session.getSessionVariable().setEnableSpill(true);
                if (currentVariable.getConnectorSinkSpillMemLimitThreshold() < currentVariable.getSpillMemLimitThreshold()) {
                    currentVariable.setSpillMemLimitThreshold(currentVariable.getConnectorSinkSpillMemLimitThreshold());
                }
            }

            PlanFragment sinkFragment = execPlan.getFragments().get(0);
            if (canUsePipeline && (targetTable instanceof OlapTable || targetTable.isIcebergTable() ||
                    targetTable.isHiveTable() || targetTable.isTableFunctionTable())) {
                if (shuffleServiceEnable) {
                    // For shuffle insert into, we only support tablet sink dop = 1
                    // because for tablet sink dop > 1, local passthourgh exchange will influence the order of sending,
                    // which may lead to inconsisten replica for primary key.
                    // If you want to set tablet sink dop > 1, please enable single tablet loading and disable shuffle service
                    sinkFragment.setPipelineDop(1);
                } else {
                    SessionVariable sv = session.getSessionVariable();
                    if (sv.getEnableAdaptiveSinkDop()) {
                        long warehouseId = session.getCurrentComputeResource().getWarehouseId();
                        sinkFragment.setPipelineDop(sv.getSinkDegreeOfParallelism(warehouseId));
                    } else {
                        sinkFragment.setPipelineDop(sv.getParallelExecInstanceNum());
                    }
                }

                if (targetTable instanceof OlapTable) {
                    sinkFragment.setHasOlapTableSink();
                    sinkFragment.setForceAssignScanRangesPerDriverSeq();
                } else if (targetTable.isHiveTable()) {
                    sinkFragment.setHasHiveTableSink();
                } else if (targetTable.isIcebergTable()) {
                    sinkFragment.setHasIcebergTableSink();
                } else if (targetTable.isTableFunctionTable()) {
                    sinkFragment.setHasTableFunctionTableSink();
                }

                sinkFragment.disableRuntimeAdaptiveDop();
                sinkFragment.setForceSetTableSinkDop();
            }
            sinkFragment.setSink(dataSink);
            sinkFragment.setLoadGlobalDicts(globalDicts);
            return execPlan;
        }
    }

    /**
     * The workhorse of InsertPlanner, which may takes a lot of time, so we would release the lock during planning
     */
    private ExecPlan buildExecPlanWithRetry(InsertStmt insertStmt, ConnectContext session,
                                            List<ColumnRefOperator> outputColumns,
                                            LogicalPlan logicalPlan, ColumnRefFactory columnRefFactory,
                                            QueryRelation queryRelation, Table targetTable) {
        boolean isSchemaValid = true;
        Set<OlapTable> olapTables = StatementPlanner.collectOriginalOlapTables(session, insertStmt);
        Stopwatch watch = Stopwatch.createStarted();

        for (int i = 0; i < Config.max_query_retry_time; i++) {
            long planStartTime = OptimisticVersion.generate();
            if (!isSchemaValid) {
                olapTables = StatementPlanner.reAnalyzeStmt(insertStmt, session, plannerMetaLocker);
            }

            // Release the lock during planning, and reacquire the lock before validating
            plannerMetaLocker.unlock();
            ExecPlan plan;
            try {
                plan = buildExecPlan(insertStmt, session, outputColumns, logicalPlan, columnRefFactory, queryRelation,
                        targetTable);
            } finally {
                try (Timer ignore2 = Tracers.watchScope("Lock")) {
                    StatementPlanner.lock(plannerMetaLocker);
                }
            }
            isSchemaValid =
                    olapTables.stream().allMatch(t -> OptimisticVersion.validateTableUpdate(t, planStartTime));
            if (isSchemaValid) {
                return plan;
            }
        }
        throw new StarRocksPlannerException(String.format("failed to generate plan for the statement after %dms",
                watch.elapsed(TimeUnit.MILLISECONDS)), ErrorType.INTERNAL_ERROR);
    }

    private ExecPlan buildExecPlan(InsertStmt insertStmt, ConnectContext session, List<ColumnRefOperator> outputColumns,
                                   LogicalPlan logicalPlan, ColumnRefFactory columnRefFactory,
                                   QueryRelation queryRelation, Table targetTable) {
        PreOptimizePlanContext preOptimizePlanContext = preparePreOptimizePlanContext(
                insertStmt, session.getSessionVariable(), targetTable, outputColumns, columnRefFactory, logicalPlan);

        PhysicalPropertySet requiredPropertySet = createPhysicalPropertySet(insertStmt, outputColumns,
                session.getSessionVariable(), preOptimizePlanContext.partitionColumnIDs);
        OptExpression optimizedPlan;

        int sourceTablesCount = collectSourceTablesCount(session, insertStmt);

        try (Timer ignore2 = Tracers.watchScope("Optimizer")) {
            OptimizerContext optimizerContext = OptimizerFactory.initContext(session, columnRefFactory);
            optimizerContext.setSourceTablesCount(sourceTablesCount);
            if (session.getSessionVariable().isEnableIVMRefresh()) {
                // Position i of outputColumns writes targetTable fullSchema[i]; IvmRewriter relies on
                // this pairing to bind aggregates to MV state columns (bindStateColumnsForAggregate).
                optimizerContext.getTvrOptContext().setIvmInsertOutputColumns(outputColumns);
            }
            Optimizer optimizer = OptimizerFactory.create(optimizerContext);
            optimizedPlan = optimizer.optimize(
                    preOptimizePlanContext.root,
                    requiredPropertySet,
                    preOptimizePlanContext.requiredColumns);
        }

        //8. Build fragment exec plan
        boolean hasOutputFragment = ((queryRelation instanceof SelectRelation && queryRelation.hasLimit())
                || targetTable instanceof MysqlTable);
        ExecPlan execPlan;
        try (Timer ignore3 = Tracers.watchScope("PlanBuilder")) {
            execPlan = PlanFragmentBuilder.createPhysicalPlan(
                    optimizedPlan, session, logicalPlan.getOutputColumn(), columnRefFactory,
                    queryRelation.getColumnOutputNames(), TResultSinkType.MYSQL_PROTOCAL, hasOutputFragment);
        }
        return execPlan;
    }

    private PreOptimizePlanContext preparePreOptimizePlanContext(InsertStmt insertStmt,
                                                                 SessionVariable sessionVariable,
                                                                 Table targetTable,
                                                                 List<ColumnRefOperator> outputColumns,
                                                                 ColumnRefFactory columnRefFactory,
                                                                 LogicalPlan logicalPlan) {
        OptExpression root = logicalPlan.getRoot();
        ColumnRefSet requiredColumns = new ColumnRefSet(logicalPlan.getOutputColumn());
        Optional<ConnectorSinkShuffleSpec> spec = ConnectorSinkShuffleSpec.forTable(targetTable);
        if (spec.isEmpty() || !spec.get().shouldShuffle(insertStmt, sessionVariable)) {
            return new PreOptimizePlanContext(root, requiredColumns, null);
        }
        ConnectorSinkShuffleSpec.PreOptimizeResult r = spec.get()
                .prepare(insertStmt, outputColumns, columnRefFactory, root, requiredColumns);
        return new PreOptimizePlanContext(r.root, r.requiredColumns, r.partitionColumnIDs);
    }



    private void castLiteralToTargetColumnsType(InsertStmt insertStatement) {
        Preconditions.checkState(insertStatement.getQueryStatement().getQueryRelation() instanceof ValuesRelation,
                "must values");
        ValuesRelation values = (ValuesRelation) insertStatement.getQueryStatement().getQueryRelation();
        RelationFields fields = insertStatement.getQueryStatement().getQueryRelation().getRelationFields();

        for (int columnIdx = 0; columnIdx < outputBaseSchema.size(); ++columnIdx) {
            if (needToSkip(insertStatement, columnIdx)) {
                continue;
            }

            Column targetColumn = outputBaseSchema.get(columnIdx);
            if (targetColumn.isGeneratedColumn()) {
                continue;
            }
            boolean isAutoIncrement = targetColumn.isAutoIncrement();
            if (insertStatement.getTargetColumnNames() == null) {
                for (List<Expr> row : values.getRows()) {
                    if (isAutoIncrement && row.get(columnIdx).getType() == NullType.NULL) {
                        throw new SemanticException(" `NULL` value is not supported for an AUTO_INCREMENT column: " +
                                targetColumn.getName() + " You can use `default` for an" +
                                " AUTO INCREMENT column");
                    }
                    if (row.get(columnIdx) instanceof DefaultValueExpr) {
                        if (isAutoIncrement) {
                            row.set(columnIdx, new NullLiteral());
                        } else {
                            row.set(columnIdx, new StringLiteral(targetColumn.calculatedDefaultValue()));
                        }
                    }
                    row.set(columnIdx, TypeManager.addCastExpr(row.get(columnIdx), targetColumn.getType()));
                }
                fields.getFieldByIndex(columnIdx).setType(targetColumn.getType());
            } else {
                int idx = insertStatement.getTargetColumnNames().indexOf(targetColumn.getName().toLowerCase());
                if (idx != -1) {
                    for (List<Expr> row : values.getRows()) {
                        if (isAutoIncrement && row.get(idx).getType() == NullType.NULL) {
                            throw new SemanticException(
                                    " `NULL` value is not supported for an AUTO_INCREMENT column: " +
                                            targetColumn.getName() + " You can use `default` for an" +
                                            " AUTO INCREMENT column");
                        }
                        if (row.get(idx) instanceof DefaultValueExpr) {
                            if (isAutoIncrement) {
                                row.set(idx, new NullLiteral());
                            } else {
                                row.set(idx, new StringLiteral(targetColumn.calculatedDefaultValue()));
                            }
                        }
                        row.set(idx, TypeManager.addCastExpr(row.get(idx), targetColumn.getType()));
                    }
                    fields.getFieldByIndex(idx).setType(targetColumn.getType());
                }
            }
        }
    }

    private OptExprBuilder fillDefaultValue(LogicalPlan logicalPlan, ColumnRefFactory columnRefFactory,
                                            InsertStmt insertStatement, List<ColumnRefOperator> outputColumns) {
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();

        for (int columnIdx = 0; columnIdx < outputBaseSchema.size(); ++columnIdx) {
            if (needToSkip(insertStatement, columnIdx)) {
                continue;
            }

            Column targetColumn = outputBaseSchema.get(columnIdx);
            if (targetColumn.isGeneratedColumn()) {
                continue;
            }
            if (insertStatement.getTargetColumnNames() == null) {
                outputColumns.add(logicalPlan.getOutputColumn().get(columnIdx));
                columnRefMap.put(logicalPlan.getOutputColumn().get(columnIdx),
                        logicalPlan.getOutputColumn().get(columnIdx));
            } else {
                int idx = insertStatement.getTargetColumnNames().indexOf(targetColumn.getName().toLowerCase());
                if (idx == -1) {
                    ScalarOperator scalarOperator;
                    Column.DefaultValueType defaultValueType = targetColumn.getDefaultValueType();
                    if (defaultValueType == Column.DefaultValueType.NULL || targetColumn.isAutoIncrement()) {
                        scalarOperator = ConstantOperator.createNull(targetColumn.getType());
                    } else if (defaultValueType == Column.DefaultValueType.CONST) {
                        if (targetColumn.getDefaultExpr() != null && targetColumn.getDefaultExpr().getExprObject() != null) {
                            Expr expr = targetColumn.getDefaultExpr().obtainExpr();
                            if (!expr.getType().equals(targetColumn.getType())) {
                                expr = new CastExpr(targetColumn.getType(), expr);
                            }
                            scalarOperator = SqlToScalarOperatorTranslator.translate(expr);
                        } else {
                            scalarOperator = ConstantOperator.createVarchar(targetColumn.calculatedDefaultValue());
                        }
                    } else if (defaultValueType == Column.DefaultValueType.VARY) {
                        if (isValidDefaultFunction(targetColumn.getDefaultExpr().getExpr())) {
                            scalarOperator = SqlToScalarOperatorTranslator.
                                    translate(targetColumn.getDefaultExpr().obtainExpr());
                        } else {
                            throw new SemanticException(
                                    "Column:" + targetColumn.getName() + " has unsupported default value:"
                                            + targetColumn.getDefaultExpr().getExpr());
                        }
                    } else {
                        throw new SemanticException("Unknown default value type:%s", defaultValueType.toString());
                    }
                    ColumnRefOperator col = columnRefFactory
                            .create(scalarOperator, scalarOperator.getType(), scalarOperator.isNullable());

                    outputColumns.add(col);
                    columnRefMap.put(col, scalarOperator);
                } else {
                    outputColumns.add(logicalPlan.getOutputColumn().get(idx));
                    columnRefMap.put(logicalPlan.getOutputColumn().get(idx), logicalPlan.getOutputColumn().get(idx));
                }
            }
        }
        return logicalPlan.getRootBuilder().withNewRoot(new LogicalProjectOperator(new HashMap<>(columnRefMap)));
    }

    private OptExprBuilder fillGeneratedColumns(ColumnRefFactory columnRefFactory, InsertStmt insertStatement,
                                                List<ColumnRefOperator> outputColumns, OptExprBuilder root,
                                                ConnectContext session) {
        Set<Column> baseSchema = Sets.newHashSet(outputBaseSchema);
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();

        for (int columnIdx = 0; columnIdx < outputFullSchema.size(); ++columnIdx) {
            Column targetColumn = outputFullSchema.get(columnIdx);

            if (targetColumn.isGeneratedColumn()) {
                // If fe restart and Insert INTO is executed, the re-analyze is needed.
                Expr expr = targetColumn.getGeneratedColumnExpr(insertStatement.getTargetTable().getIdToColumn());
                ExpressionAnalyzer.analyzeExpression(expr,
                        new AnalyzeState(), new Scope(RelationId.anonymous(), new RelationFields(
                                insertStatement.getTargetTable().getBaseSchema().stream()
                                        .map(col -> {
                                            TableRef tableRef = insertStatement.getTableRef();
                                            TableName tableName = TableName.fromTableRef(tableRef);
                                            return new Field(col.getName(), col.getType(), tableName, null);
                                        })
                                        .collect(Collectors.toList()))), session);

                List<SlotRef> slots = new ArrayList<>();
                expr.collect(SlotRef.class, slots);

                ExpressionMapping expressionMapping =
                        new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()),
                                Lists.newArrayList());

                for (SlotRef slot : slots) {
                    String originName = slot.getColumnName();

                    Optional<Column> optOriginColumn = outputFullSchema.stream()
                            .filter(c -> c.nameEquals(originName, false)).findFirst();
                    Column originColumn = optOriginColumn.get();
                    ColumnRefOperator originColRefOp = outputColumns.get(outputFullSchema.indexOf(originColumn));

                    expressionMapping.put(slot, originColRefOp);
                }

                ScalarOperator scalarOperator =
                        SqlToScalarOperatorTranslator.translate(expr, expressionMapping, columnRefFactory);

                ColumnRefOperator columnRefOperator =
                        columnRefFactory.create(scalarOperator, scalarOperator.getType(), scalarOperator.isNullable());
                outputColumns.add(columnRefOperator);
                columnRefMap.put(columnRefOperator, scalarOperator);
            } else if (baseSchema.contains(outputFullSchema.get(columnIdx))) {
                ColumnRefOperator columnRefOperator = outputColumns.get(columnIdx);
                columnRefMap.put(columnRefOperator, columnRefOperator);
            }
        }

        return root.withNewRoot(new LogicalProjectOperator(new HashMap<>(columnRefMap)));
    }

    private OptExprBuilder fillShadowColumns(ColumnRefFactory columnRefFactory, InsertStmt insertStatement,
                                             List<ColumnRefOperator> outputColumns, OptExprBuilder root,
                                             ConnectContext session) {
        Set<Column> baseSchema = Sets.newHashSet(outputBaseSchema);
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();
        for (int columnIdx = 0; columnIdx < outputFullSchema.size(); ++columnIdx) {
            Column targetColumn = outputFullSchema.get(columnIdx);

            if (targetColumn.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX)) {
                if (targetColumn.isGeneratedColumn()) {
                    continue;
                }

                String originName = Column.removeNamePrefix(targetColumn.getName());
                Optional<Column> optOriginColumn = outputFullSchema.stream()
                        .filter(c -> c.nameEquals(originName, false)).findFirst();
                Preconditions.checkState(optOriginColumn.isPresent());
                Column originColumn = optOriginColumn.get();
                ColumnRefOperator originColRefOp = outputColumns.get(outputFullSchema.indexOf(originColumn));

                ColumnRefOperator columnRefOperator = columnRefFactory.create(
                        targetColumn.getName(), targetColumn.getType(), targetColumn.isAllowNull());

                outputColumns.add(columnRefOperator);
                columnRefMap.put(columnRefOperator, new CastOperator(targetColumn.getType(), originColRefOp, true));
                continue;
            }

            // Target column which starts with "mv" should not be treated as materialized view column when this column exists
            // in base schema,
            // this could be created by user.
            if (targetColumn.isNameWithPrefix(MATERIALIZED_VIEW_NAME_PREFIX) &&
                    !baseSchema.contains(targetColumn)) {
                if (targetColumn.getDefineExpr() == null) {
                    Table targetTable = insertStatement.getTargetTable();
                    // Only olap table can have the synchronized materialized view.
                    OlapTable targetOlapTable = (OlapTable) targetTable;
                    MaterializedIndexMeta targetIndexMeta = null;
                    for (MaterializedIndexMeta indexMeta : targetOlapTable.getIndexMetaIdToMeta().values()) {
                        if (indexMeta.getIndexMetaId() == targetOlapTable.getBaseIndexMetaId()) {
                            continue;
                        }
                        for (Column column : indexMeta.getSchema()) {
                            if (column.getName().equals(targetColumn.getName())) {
                                targetIndexMeta = indexMeta;
                                break;
                            }
                        }
                    }
                    String targetIndexMetaName = targetIndexMeta == null ? "" :
                            targetOlapTable.getIndexNameByMetaId(targetIndexMeta.getIndexMetaId());
                    throw new SemanticException(
                            "The define expr of shadow column " + targetColumn.getName() + " is null, " +
                                    "please check the associated materialized view " + targetIndexMetaName
                                    + " of target table:" + insertStatement.getTargetTable().getName());
                }
                ExpressionMapping expressionMapping =
                        new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()),
                                Lists.newArrayList());

                List<SlotRef> slots = targetColumn.getRefColumns();
                for (SlotRef slot : slots) {
                    String originName = slot.getColumnName();
                    Optional<Column> optOriginColumn = outputFullSchema.stream()
                            .filter(c -> c.nameEquals(originName, false)).findFirst();
                    Preconditions.checkState(optOriginColumn.isPresent());
                    Column originColumn = optOriginColumn.get();
                    ColumnRefOperator originColRefOp = outputColumns.get(outputFullSchema.indexOf(originColumn));
                    expressionMapping.put(slot, originColRefOp);
                }

                ScalarOperator scalarOperator =
                        SqlToScalarOperatorTranslator.translate(targetColumn.getDefineExpr(), expressionMapping,
                                columnRefFactory);

                ColumnRefOperator columnRefOperator =
                        columnRefFactory.create(scalarOperator, scalarOperator.getType(), scalarOperator.isNullable());
                outputColumns.add(columnRefOperator);
                columnRefMap.put(columnRefOperator, scalarOperator);
                continue;
            }

            // columnIdx >= outputColumns.size() mean this is a new add schema change column
            if (columnIdx >= outputColumns.size()) {
                ScalarOperator scalarOperator = null;
                Column.DefaultValueType defaultValueType = targetColumn.getDefaultValueType();
                if (defaultValueType == Column.DefaultValueType.NULL) {
                    scalarOperator = ConstantOperator.createNull(targetColumn.getType());
                } else if (defaultValueType == Column.DefaultValueType.CONST) {
                    scalarOperator = ConstantOperator.createVarchar(targetColumn.calculatedDefaultValue());
                } else if (defaultValueType == Column.DefaultValueType.VARY) {
                    throw new SemanticException("Column:" + targetColumn.getName() + " has unsupported default value:"
                            + targetColumn.getDefaultExpr().getExpr());
                }
                ColumnRefOperator col = columnRefFactory
                        .create(scalarOperator, scalarOperator.getType(), scalarOperator.isNullable());
                outputColumns.add(col);
                columnRefMap.put(col, scalarOperator);
            } else {
                columnRefMap.put(outputColumns.get(columnIdx), outputColumns.get(columnIdx));
            }
        }
        return root.withNewRoot(new LogicalProjectOperator(new HashMap<>(columnRefMap)));
    }

    private OptExprBuilder castOutputColumnsTypeToTargetColumns(ColumnRefFactory columnRefFactory,
                                                                InsertStmt insertStatement,
                                                                List<ColumnRefOperator> outputColumns,
                                                                OptExprBuilder root) {
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();
        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        List<ScalarOperatorRewriteRule> rewriteRules = Arrays.asList(new FoldConstantsRule());
        for (int columnIdx = 0; columnIdx < outputFullSchema.size(); ++columnIdx) {
            if (!outputFullSchema.get(columnIdx).getType().matchesType(outputColumns.get(columnIdx).getType())) {
                Column c = outputFullSchema.get(columnIdx);
                ColumnRefOperator k = columnRefFactory.create(c.getName(), c.getType(), c.isAllowNull());
                ScalarOperator castOperator = new CastOperator(outputFullSchema.get(columnIdx).getType(),
                        outputColumns.get(columnIdx), true);
                columnRefMap.put(k, rewriter.rewrite(castOperator, rewriteRules));
                outputColumns.set(columnIdx, k);
            } else {
                columnRefMap.put(outputColumns.get(columnIdx), outputColumns.get(columnIdx));
            }
        }
        return root.withNewRoot(new LogicalProjectOperator(new HashMap<>(columnRefMap)));
    }


    private static final class PreOptimizePlanContext {
        private final OptExpression root;
        private final ColumnRefSet requiredColumns;
        private final List<Integer> partitionColumnIDs;

        private PreOptimizePlanContext(OptExpression root, ColumnRefSet requiredColumns,
                                       List<Integer> partitionColumnIDs) {
            this.root = root;
            this.requiredColumns = requiredColumns;
            this.partitionColumnIDs = partitionColumnIDs;
        }
    }

    /**
     * OlapTableSink may be executed in multiply fragment instances of different machines
     * For non-duplicate key types, we must guarantee that the orders of the same key are
     * exactly the same. In order to achieve this goal, we can perform shuffle before TableSink
     * so that the same key will be sent to the same fragment instance
     */
    private PhysicalPropertySet createPhysicalPropertySet(InsertStmt insertStmt,
                                                          List<ColumnRefOperator> outputColumns,
                                                          SessionVariable session,
                                                          List<Integer> preComputedPartitionColumnIDs) {
        QueryRelation queryRelation = insertStmt.getQueryStatement().getQueryRelation();
        if ((queryRelation instanceof SelectRelation && queryRelation.hasLimit())) {
            DistributionProperty distributionProperty = DistributionProperty
                    .createProperty(new GatherDistributionSpec());
            return new PhysicalPropertySet(distributionProperty);
        }

        Table targetTable = insertStmt.getTargetTable();
        Optional<ConnectorSinkShuffleSpec> connectorSpec = ConnectorSinkShuffleSpec.forTable(targetTable);
        if (connectorSpec.isPresent()) {
            // preComputedPartitionColumnIDs is non-null iff the spec decided to shuffle
            // (set in preparePreOptimizePlanContext). When non-empty, create the hash
            // distribution; otherwise leave the property set distribution-less.
            DistributionProperty distributionProperty =
                    (preComputedPartitionColumnIDs != null && !preComputedPartitionColumnIDs.isEmpty())
                            ? createDistributionPropertyFromPartitions(preComputedPartitionColumnIDs)
                            : EmptyDistributionProperty.INSTANCE;
            SortProperty sortProperty = connectorSpec.get().computeSortProperty(session, outputColumns);
            return new PhysicalPropertySet(distributionProperty, sortProperty);
        }

        if (targetTable instanceof TableFunctionTable) {
            TableFunctionTable table = (TableFunctionTable) targetTable;
            if (table.isWriteSingleFile()) {
                return new PhysicalPropertySet(DistributionProperty
                        .createProperty(new GatherDistributionSpec()));
            }

            if (session.isEnableConnectorSinkGlobalShuffle()) {
                // use random shuffle for unpartitioned table
                if (table.getPartitionColumnNames().isEmpty()) {
                    return new PhysicalPropertySet(DistributionProperty
                            .createProperty(new RoundRobinDistributionSpec()));
                } else { // use hash shuffle for partitioned table
                    List<Integer> partitionColumnIDs = table.getPartitionColumnIDs().stream()
                            .map(x -> outputColumns.get(x).getId()).collect(Collectors.toList());
                    return new PhysicalPropertySet(createDistributionPropertyFromPartitions(partitionColumnIDs));
                }
            }

            // no global shuffle
            return PhysicalPropertySet.EMPTY;
        }

        if (!(targetTable instanceof OlapTable)) {
            return new PhysicalPropertySet();
        }

        OlapTable table = (OlapTable) targetTable;

        if (KeysType.DUP_KEYS.equals(table.getKeysType())) {
            return new PhysicalPropertySet();
        }

        // No extra distribution property is needed if replication num is 1
        if (!enableSingleReplicationShuffle && table.getDefaultReplicationNum() <= 1) {
            return new PhysicalPropertySet();
        }

        if (table.enableReplicatedStorage()) {
            return new PhysicalPropertySet();
        }

        List<Column> columns = outputFullSchema;
        Preconditions.checkState(columns.size() == outputColumns.size(),
                "outputColumn's size must equal with table's column size");

        List<Column> keyColumns = table.getKeyColumnsByIndexMetaId(table.getBaseIndexMetaId());
        List<Integer> keyColumnIds = Lists.newArrayList();
        keyColumns.forEach(column -> {
            int index = columns.indexOf(column);
            Preconditions.checkState(index >= 0);
            keyColumnIds.add(outputColumns.get(index).getId());
        });

        HashDistributionDesc desc =
                new HashDistributionDesc(keyColumnIds, HashDistributionDesc.SourceType.SHUFFLE_AGG);
        DistributionSpec spec = DistributionSpec.createHashDistributionSpec(desc);
        DistributionProperty property = DistributionProperty.createProperty(spec);

        if (Config.eliminate_shuffle_load_by_replicated_storage) {
            forceReplicatedStorage = true;
            return new PhysicalPropertySet();
        }

        shuffleServiceEnable = true;

        return new PhysicalPropertySet(property);
    }

    private DistributionProperty createDistributionPropertyFromPartitions(List<Integer> partitionColumnIDs) {
        HashDistributionDesc desc = new HashDistributionDesc(partitionColumnIDs, HashDistributionDesc.SourceType.SHUFFLE_AGG);
        DistributionSpec spec = DistributionSpec.createHashDistributionSpec(desc);
        return DistributionProperty.createProperty(spec);
    }

    private OptExprBuilder fillKeyPartitionsColumn(ColumnRefFactory columnRefFactory,
                                                   InsertStmt insertStatement, List<ColumnRefOperator> outputColumns,
                                                   OptExprBuilder root) {
        Table targetTable = insertStatement.getTargetTable();
        LogicalProjectOperator projectOperator = (LogicalProjectOperator) root.getRoot().getOp();
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = projectOperator.getColumnRefMap();
        List<String> partitionColNames = insertStatement.getTargetPartitionNames().getPartitionColNames();
        List<Expr> partitionColValues = insertStatement.getTargetPartitionNames().getPartitionColValues();
        List<String> tablePartitionColumnNames = targetTable.getPartitionColumnNames();

        for (Column column : targetTable.getFullSchema()) {
            String columnName = column.getName();
            if (tablePartitionColumnNames.contains(columnName)) {
                int index = partitionColNames.indexOf(columnName);
                LiteralExpr expr = (LiteralExpr) partitionColValues.get(index);
                Type type = expr.isConstantNull() ? NullType.NULL : column.getType();
                ScalarOperator scalarOperator =
                        ConstantOperator.createObject(expr.getRealObjectValue(), type);
                ColumnRefOperator col = columnRefFactory
                        .create(scalarOperator, scalarOperator.getType(), scalarOperator.isNullable());
                outputColumns.add(col);
                columnRefMap.put(col, scalarOperator);
            }
        }
        return root.withNewRoot(new LogicalProjectOperator(new HashMap<>(columnRefMap)));
    }

    private boolean needToSkip(InsertStmt stmt, int columnIdx) {
        Table targetTable = stmt.getTargetTable();
        boolean skip = false;
        if (stmt.isSpecifyKeyPartition()) {
            if (targetTable.isIcebergTable()) {
                return ((IcebergTable) targetTable).partitionColumnIndexes().contains(columnIdx);
            } else if (targetTable.isHiveTable()) {
                return columnIdx >= targetTable.getFullSchema().size() - targetTable.getPartitionColumnNames().size();
            }
        }

        return skip;
    }

    private boolean checkPartitionInsertValid(Table targetTable) {
        if (targetTable instanceof IcebergTable) {
            IcebergTable icebergTable = (IcebergTable) targetTable;
            if (icebergTable.isPartitioned()) {
                PartitionSpec partitionSpec = icebergTable.getNativeTable().spec();
                boolean isInvalid = partitionSpec.fields().stream().anyMatch(field -> !field.transform().isIdentity());
                if (isInvalid) {
                    throw new SemanticException("Staitc insert into Iceberg table %s is not supported" +
                            " for not partitioned by identity transform", icebergTable.getName());
                }
            }
        }

        return true;
    }

    private boolean isKeyPartitionStaticInsert(InsertStmt insertStmt, QueryRelation queryRelation) {
        Table targetTable = insertStmt.getTargetTable();

        if (!(targetTable.isHiveTable() || targetTable.isIcebergTable())) {
            return false;
        }

        if (targetTable.isUnPartitioned()) {
            return false;
        }

        if (insertStmt.isSpecifyKeyPartition()) {
            checkPartitionInsertValid(targetTable);
            return true;
        }

        if (!(queryRelation instanceof SelectRelation)) {
            return false;
        }

        SelectRelation selectRelation = (SelectRelation) queryRelation;
        List<SelectListItem> listItems = selectRelation.getSelectList().getItems();

        for (SelectListItem item : listItems) {
            if (item.isStar()) {
                return false;
            }
        }

        List<String> targetColumnNames;
        if (insertStmt.getTargetColumnNames() == null) {
            targetColumnNames = targetTable.getFullSchema().stream()
                    .map(Column::getName).collect(Collectors.toList());
        } else {
            targetColumnNames = Lists.newArrayList(insertStmt.getTargetColumnNames());
        }

        for (int i = 0; i < targetColumnNames.size(); i++) {
            String columnName = targetColumnNames.get(i);
            if (targetTable.getPartitionColumnNames().contains(columnName)) {
                Expr expr = listItems.get(i).getExpr();
                if (!expr.isConstant()) {
                    return false;
                }
            }
        }
        checkPartitionInsertValid(targetTable);
        return true;
    }


}
