// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.transformer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.Subquery;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunction;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.external.elasticsearch.EsTablePartitions;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.FieldId;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SetQualifier;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.SubqueryUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaLakeScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFileScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHudiScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubqueryOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.ReduceCastRule;
import org.apache.commons.lang3.tuple.Triple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class RelationTransformer extends AstVisitor<LogicalPlan, ExpressionMapping> {
    private final ColumnRefFactory columnRefFactory;
    private final ConnectContext session;

    private final ExpressionMapping outer;
    private final CTETransformerContext cteContext;
    private final List<ColumnRefOperator> correlation = new ArrayList<>();

    public RelationTransformer(ColumnRefFactory columnRefFactory, ConnectContext session) {
        this(columnRefFactory, session,
                new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields())),
                new CTETransformerContext());
    }

    public RelationTransformer(ColumnRefFactory columnRefFactory, ConnectContext session, ExpressionMapping outer,
                               CTETransformerContext cteContext) {
        this.columnRefFactory = columnRefFactory;
        this.session = session;
        this.outer = outer;
        this.cteContext = cteContext;
    }

    // transform relation to plan with session variable sql_select_limit
    // only top relation need set limit, transform method used by CTE/Subquery/Insert
    public LogicalPlan transformWithSelectLimit(Relation relation) {
        LogicalPlan plan = transform(relation);
        OptExprBuilder root = plan.getRootBuilder();
        // Set limit if user set sql_select_limit.
        long selectLimit = ConnectContext.get().getSessionVariable().getSqlSelectLimit();
        if (!root.getRoot().getOp().hasLimit() && selectLimit != SessionVariable.DEFAULT_SELECT_LIMIT) {
            LogicalLimitOperator limitOperator = LogicalLimitOperator.init(selectLimit);
            root = root.withNewRoot(limitOperator);
            return new LogicalPlan(root, plan.getOutputColumn(), plan.getCorrelation());
        }

        return plan;
    }

    public LogicalPlan transform(Relation relation) {
        OptExprBuilder optExprBuilder;
        if (relation instanceof QueryRelation && !((QueryRelation) relation).getCteRelations().isEmpty()) {
            Pair<OptExprBuilder, OptExprBuilder> cteRootAndMostDeepAnchor =
                    buildCTEAnchorAndProducer((QueryRelation) relation);
            optExprBuilder = cteRootAndMostDeepAnchor.first;
            LogicalPlan logicalPlan = visit(relation);
            cteRootAndMostDeepAnchor.second.addChild(logicalPlan.getRootBuilder());
            return new LogicalPlan(optExprBuilder, logicalPlan.getOutputColumn(), logicalPlan.getCorrelation());
        } else {
            return visit(relation);
        }
    }

    Pair<OptExprBuilder, OptExprBuilder> buildCTEAnchorAndProducer(QueryRelation node) {
        OptExprBuilder root = null;
        OptExprBuilder anchorOptBuilder = null;
        for (CTERelation cteRelation : node.getCteRelations()) {
            int cteId = cteContext.registerCteRef(cteRelation.getCteMouldId());
            LogicalCTEAnchorOperator anchorOperator = new LogicalCTEAnchorOperator(cteId);
            LogicalCTEProduceOperator produceOperator = new LogicalCTEProduceOperator(cteId);
            LogicalPlan producerPlan =
                    new RelationTransformer(columnRefFactory, session,
                            new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields())),
                            cteContext).transform(cteRelation.getCteQueryStatement().getQueryRelation());
            OptExprBuilder produceOptBuilder =
                    new OptExprBuilder(produceOperator, Lists.newArrayList(producerPlan.getRootBuilder()),
                            producerPlan.getRootBuilder().getExpressionMapping());

            OptExprBuilder newAnchorOptBuilder = new OptExprBuilder(anchorOperator,
                    Lists.newArrayList(produceOptBuilder), null);

            if (anchorOptBuilder != null) {
                anchorOptBuilder.addChild(newAnchorOptBuilder);
            } else {
                root = newAnchorOptBuilder;
            }
            anchorOptBuilder = newAnchorOptBuilder;

            cteContext.getCteExpressions().put(cteId, new ExpressionMapping(
                    new Scope(RelationId.of(
                            cteRelation.getCteQueryStatement().getQueryRelation()),
                            cteRelation.getRelationFields()),
                    producerPlan.getOutputColumn()));
        }

        return new Pair<>(root, anchorOptBuilder);
    }

    @Override
    public LogicalPlan visitQueryStatement(QueryStatement node, ExpressionMapping context) {
        return visit(node.getQueryRelation());
    }

    @Override
    public LogicalPlan visitSelect(SelectRelation node, ExpressionMapping context) {
        return new QueryTransformer(columnRefFactory, session, cteContext).plan(node, outer);
    }

    @Override
    public LogicalPlan visitUnion(UnionRelation node, ExpressionMapping context) {
        return processSetOperation(node);
    }

    @Override
    public LogicalPlan visitExcept(ExceptRelation node, ExpressionMapping context) {
        return processSetOperation(node);
    }

    @Override
    public LogicalPlan visitIntersect(IntersectRelation node, ExpressionMapping context) {
        return processSetOperation(node);
    }

    // When transform SetOperationRelation into setOperation LogicalOperator, child exprs of the LogicalOperator
    // are not processed by ReduceCastRule, that leads cast(string as string) is generated and propagated to BE
    // unexpectedly.
    public ScalarOperator foldCast(ScalarOperator operator) {
        return new ScalarOperatorRewriter().rewrite(operator, Lists.newArrayList(new ReduceCastRule()));
    }

    private LogicalPlan processSetOperation(SetOperationRelation setOperationRelation) {
        List<OptExprBuilder> childPlan = new ArrayList<>();
        /*
         * setColumns records the columns of all children,
         * which are used for column prune.
         * If only the first child is recorded, it may cause the
         * columns used in other children to be pruned
         */
        List<List<ColumnRefOperator>> childOutputColumnList = new ArrayList<>();
        for (QueryRelation relation : setOperationRelation.getRelations()) {
            LogicalPlan setPlan = visit(relation);
            OptExprBuilder optExprBuilder = setPlan.getRootBuilder();
            List<ColumnRefOperator> childOutputColumn = setPlan.getOutputColumn();

            if (optExprBuilder.getRoot().getOp() instanceof LogicalValuesOperator) {
                LogicalValuesOperator valuesOperator = (LogicalValuesOperator) optExprBuilder.getRoot().getOp();
                List<ScalarOperator> row = valuesOperator.getRows().get(0);
                for (int i = 0; i < setOperationRelation.getRelationFields().getAllFields().size(); ++i) {
                    Type outputType = setOperationRelation.getRelationFields().getFieldByIndex(i).getType();
                    Type relationType = relation.getRelationFields().getFieldByIndex(i).getType();
                    if (!outputType.equals(relationType)) {
                        try {
                            if (relationType.isNull()) {
                                row.get(i).setType(outputType);
                            } else {
                                ScalarOperator expr = foldCast(((ConstantOperator) row.get(i)).castTo(outputType));
                                row.set(i, expr);
                            }
                            valuesOperator.getColumnRefSet().get(i).setType(outputType);
                        } catch (Exception e) {
                            throw new SemanticException(e.toString());
                        }
                    }
                }
                // Note: must copy here
                childOutputColumnList.add(Lists.newArrayList(childOutputColumn));
            } else {
                Map<ColumnRefOperator, ScalarOperator> projections = Maps.newHashMap();
                List<ColumnRefOperator> newChildOutputs = new ArrayList<>();
                for (int i = 0; i < setOperationRelation.getRelationFields().getAllFields().size(); ++i) {
                    Type outputType = setOperationRelation.getRelationFields().getFieldByIndex(i).getType();
                    if (!outputType.equals(relation.getRelationFields().getFieldByIndex(i).getType())) {
                        ColumnRefOperator c = columnRefFactory.create("cast", outputType, true);
                        ScalarOperator expr = foldCast(new CastOperator(outputType, childOutputColumn.get(i), true));
                        projections.put(c, expr);
                        newChildOutputs.add(c);
                    } else {
                        projections.put(childOutputColumn.get(i), childOutputColumn.get(i));
                        newChildOutputs.add(childOutputColumn.get(i));
                    }
                }
                LogicalProjectOperator projectOperator = new LogicalProjectOperator(projections);
                optExprBuilder = optExprBuilder.withNewRoot(projectOperator);
                childOutputColumnList.add(newChildOutputs);
            }
            childPlan.add(optExprBuilder);
        }

        List<ColumnRefOperator> outputColumns = childOutputColumnList.get(0).stream()
                .map(c -> columnRefFactory.create(c, c.getType(), c.isNullable())).collect(Collectors.toList());
        for (int childIdx = 1; childIdx < childOutputColumnList.size(); ++childIdx) {
            List<ColumnRefOperator> childOutputColumn = childOutputColumnList.get(childIdx);
            for (int i = 0; i < childOutputColumn.size(); ++i) {
                if (!outputColumns.get(i).isNullable() && childOutputColumn.get(i).isNullable()) {
                    outputColumns.get(i).setNullable(true);
                }
            }
        }

        Scope outputScope = setOperationRelation.getRelations().get(0).getScope();
        ExpressionMapping expressionMapping = new ExpressionMapping(outputScope, outputColumns);

        OptExprBuilder root;
        if (setOperationRelation instanceof UnionRelation) {
            root = new OptExprBuilder(new LogicalUnionOperator.Builder()
                    .setOutputColumnRefOp(outputColumns)
                    .setChildOutputColumns(childOutputColumnList)
                    .isUnionAll(!SetQualifier.DISTINCT.equals(setOperationRelation.getQualifier()))
                    .build(),
                    childPlan, expressionMapping);

            if (setOperationRelation.getQualifier().equals(SetQualifier.DISTINCT)) {
                root = root.withNewRoot(
                        new LogicalAggregationOperator(AggType.GLOBAL, outputColumns, Maps.newHashMap()));
            }
        } else if (setOperationRelation instanceof ExceptRelation) {
            root = new OptExprBuilder(new LogicalExceptOperator.Builder()
                    .setOutputColumnRefOp(outputColumns)
                    .setChildOutputColumns(childOutputColumnList).build(),
                    childPlan, expressionMapping);

        } else if (setOperationRelation instanceof IntersectRelation) {
            root = new OptExprBuilder(new LogicalIntersectOperator.Builder()
                    .setOutputColumnRefOp(outputColumns)
                    .setChildOutputColumns(childOutputColumnList).build(),
                    childPlan, expressionMapping);
        } else {
            throw unsupportedException("New Planner only support Query Statement");
        }

        root = addOrderByLimit(root, setOperationRelation);
        return new LogicalPlan(root, outputColumns, null);
    }

    private OptExprBuilder addOrderByLimit(OptExprBuilder root, QueryRelation relation) {
        List<OrderByElement> orderBy = relation.getOrderBy();
        if (relation.hasOrderByClause()) {
            List<Ordering> orderings = new ArrayList<>();
            List<ColumnRefOperator> orderByColumns = Lists.newArrayList();
            for (OrderByElement item : orderBy) {
                ColumnRefOperator column = (ColumnRefOperator) SqlToScalarOperatorTranslator.translate(item.getExpr(),
                        root.getExpressionMapping(), columnRefFactory);
                Ordering ordering = new Ordering(column, item.getIsAsc(),
                        OrderByElement.nullsFirst(item.getNullsFirstParam()));
                if (!orderByColumns.contains(column)) {
                    orderByColumns.add(column);
                    orderings.add(ordering);
                }
            }
            root = root.withNewRoot(new LogicalTopNOperator(orderings));
        }

        LimitElement limit = relation.getLimit();
        if (limit != null) {
            LogicalLimitOperator limitOperator = LogicalLimitOperator.init(limit.getLimit(), limit.getOffset());
            root = root.withNewRoot(limitOperator);
        }
        return root;
    }

    @Override
    public LogicalPlan visitValues(ValuesRelation node, ExpressionMapping context) {
        List<ColumnRefOperator> valuesOutputColumns = Lists.newArrayList();
        for (int fieldIdx = 0; fieldIdx < node.getRelationFields().size(); ++fieldIdx) {
            valuesOutputColumns.add(columnRefFactory.create(node.getColumnOutputNames().get(fieldIdx),
                    node.getRelationFields().getFieldByIndex(fieldIdx).getType(), false));
        }

        List<List<ScalarOperator>> values = new ArrayList<>();
        for (List<Expr> row : node.getRows()) {
            List<ScalarOperator> valuesRow = new ArrayList<>();
            for (int fieldIdx = 0; fieldIdx < row.size(); ++fieldIdx) {
                Expr rowField = row.get(fieldIdx);
                Type outputType = node.getRelationFields().getFieldByIndex(fieldIdx).getType();
                Type fieldType = rowField.getType();
                if (!outputType.equals(fieldType)) {
                    if (fieldType.isNull()) {
                        rowField.setType(outputType);
                    } else {
                        row.set(fieldIdx, TypeManager.addCastExpr(rowField, outputType));
                    }
                }

                ScalarOperator constant = SqlToScalarOperatorTranslator.translate(row.get(fieldIdx),
                        new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields())),
                        columnRefFactory);
                valuesRow.add(constant);

                if (constant.isNullable()) {
                    valuesOutputColumns.get(fieldIdx).setNullable(true);
                }
            }
            values.add(valuesRow);
        }
        OptExprBuilder valuesOpt = new OptExprBuilder(new LogicalValuesOperator(valuesOutputColumns, values),
                Collections.emptyList(),
                new ExpressionMapping(new Scope(RelationId.of(node), node.getRelationFields()), valuesOutputColumns));
        return new LogicalPlan(valuesOpt, valuesOutputColumns, null);
    }

    @Override
    public LogicalPlan visitTable(TableRelation node, ExpressionMapping context) {
        ImmutableMap.Builder<ColumnRefOperator, Column> colRefToColumnMetaMapBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Column, ColumnRefOperator> columnMetaToColRefMapBuilder = ImmutableMap.builder();
        ImmutableList.Builder<ColumnRefOperator> outputVariablesBuilder = ImmutableList.builder();

        int relationId = columnRefFactory.getNextRelationId();
        for (Map.Entry<Field, Column> column : node.getColumns().entrySet()) {
            ColumnRefOperator columnRef = columnRefFactory.create(column.getKey().getName(),
                    column.getKey().getType(),
                    column.getValue().isAllowNull());
            columnRefFactory.updateColumnToRelationIds(columnRef.getId(), relationId);
            columnRefFactory.updateColumnRefToColumns(columnRef, column.getValue(), node.getTable());
            outputVariablesBuilder.add(columnRef);
            colRefToColumnMetaMapBuilder.put(columnRef, column.getValue());
            columnMetaToColRefMapBuilder.put(column.getValue(), columnRef);
        }

        Map<Column, ColumnRefOperator> columnMetaToColRefMap = columnMetaToColRefMapBuilder.build();
        List<ColumnRefOperator> outputVariables = outputVariablesBuilder.build();

        ScalarOperator partitionPredicate = null;
        if (node.getPartitionPredicate() != null) {
            partitionPredicate = SqlToScalarOperatorTranslator.translate(node.getPartitionPredicate(),
                    new ExpressionMapping(node.getScope(), outputVariables), columnRefFactory);
        }

        LogicalScanOperator scanOperator;
        if (node.getTable().isNativeTable()) {
            DistributionInfo distributionInfo = ((OlapTable) node.getTable()).getDefaultDistributionInfo();
            Preconditions.checkState(distributionInfo instanceof HashDistributionInfo);
            HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
            List<Column> distributedColumns = hashDistributionInfo.getDistributionColumns();
            List<Integer> hashDistributeColumns = new ArrayList<>();
            for (Column distributedColumn : distributedColumns) {
                hashDistributeColumns.add(columnMetaToColRefMap.get(distributedColumn).getId());
            }

            HashDistributionDesc hashDistributionDesc =
                    new HashDistributionDesc(hashDistributeColumns, HashDistributionDesc.SourceType.LOCAL);
            if (node.isMetaQuery()) {
                scanOperator = new LogicalMetaScanOperator(node.getTable(), colRefToColumnMetaMapBuilder.build());
            } else {
                scanOperator = new LogicalOlapScanOperator(node.getTable(),
                        colRefToColumnMetaMapBuilder.build(),
                        columnMetaToColRefMap,
                        DistributionSpec.createHashDistributionSpec(hashDistributionDesc),
                        Operator.DEFAULT_LIMIT,
                        null,
                        ((OlapTable) node.getTable()).getBaseIndexId(),
                        null,
                        node.getPartitionNames(),
                        node.getHasHintsPartitionNames(),
                        Lists.newArrayList(),
                        node.getTabletIds(),
                        node.isUsePkIndex());
            }
        } else if (Table.TableType.HIVE.equals(node.getTable().getType())) {
            scanOperator = new LogicalHiveScanOperator(node.getTable(), colRefToColumnMetaMapBuilder.build(),
                    columnMetaToColRefMap, Operator.DEFAULT_LIMIT, partitionPredicate);
        } else if (Table.TableType.FILE.equals(node.getTable().getType())) {
            scanOperator = new LogicalFileScanOperator(node.getTable(), colRefToColumnMetaMapBuilder.build(),
                    columnMetaToColRefMap, Operator.DEFAULT_LIMIT, null);
        } else if (Table.TableType.ICEBERG.equals(node.getTable().getType())) {
            if (!((IcebergTable) node.getTable()).isCatalogTbl()) {
                ((IcebergTable) node.getTable()).refreshTable();
            }
            scanOperator = new LogicalIcebergScanOperator(node.getTable(), colRefToColumnMetaMapBuilder.build(),
                    columnMetaToColRefMap, Operator.DEFAULT_LIMIT, partitionPredicate);
        } else if (Table.TableType.HUDI.equals(node.getTable().getType())) {
            scanOperator = new LogicalHudiScanOperator(node.getTable(), colRefToColumnMetaMapBuilder.build(),
                    columnMetaToColRefMap, Operator.DEFAULT_LIMIT, partitionPredicate);
        } else if (Table.TableType.DELTALAKE.equals(node.getTable().getType())) {
            scanOperator = new LogicalDeltaLakeScanOperator(node.getTable(), colRefToColumnMetaMapBuilder.build(),
                    columnMetaToColRefMap, Operator.DEFAULT_LIMIT, null);
        } else if (Table.TableType.PAIMON.equals(node.getTable().getType())) {
            scanOperator = new LogicalPaimonScanOperator(node.getTable(), colRefToColumnMetaMapBuilder.build(),
                    columnMetaToColRefMap, Operator.DEFAULT_LIMIT, null);
        } else if (Table.TableType.SCHEMA.equals(node.getTable().getType())) {
            scanOperator =
                    new LogicalSchemaScanOperator(node.getTable(),
                            colRefToColumnMetaMapBuilder.build(),
                            columnMetaToColRefMap, Operator.DEFAULT_LIMIT,
                            null, null);
        } else if (Table.TableType.MYSQL.equals(node.getTable().getType())) {
            scanOperator =
                    new LogicalMysqlScanOperator(node.getTable(), colRefToColumnMetaMapBuilder.build(),
                            columnMetaToColRefMap, Operator.DEFAULT_LIMIT,
                            null, null);
            if (node.getTemporalClause() != null) {
                ((LogicalMysqlScanOperator) scanOperator).setTemporalClause(node.getTemporalClause());
            }
        } else if (Table.TableType.ELASTICSEARCH.equals(node.getTable().getType())) {
            scanOperator =
                    new LogicalEsScanOperator(node.getTable(), colRefToColumnMetaMapBuilder.build(),
                            columnMetaToColRefMap, Operator.DEFAULT_LIMIT,
                            null, null);
            EsTablePartitions esTablePartitions = ((LogicalEsScanOperator) scanOperator).getEsTablePartitions();
            EsTable table = (EsTable) scanOperator.getTable();
            if (esTablePartitions == null) {
                if (table.getLastMetaDataSyncException() != null) {
                    throw new StarRocksPlannerException("fetch es table [" + table.getName() + "] metadata failure: " +
                            table.getLastMetaDataSyncException().getLocalizedMessage(), ErrorType.USER_ERROR);
                }
                throw new StarRocksPlannerException("EsTable metadata has not been synced, Try it later",
                        ErrorType.USER_ERROR);
            }
        } else if (Table.TableType.JDBC.equals(node.getTable().getType())) {
            scanOperator =
                    new LogicalJDBCScanOperator(node.getTable(), colRefToColumnMetaMapBuilder.build(),
                            columnMetaToColRefMap, Operator.DEFAULT_LIMIT,
                            null, null);
        } else {
            throw new StarRocksPlannerException("Not support table type: " + node.getTable().getType(),
                    ErrorType.UNSUPPORTED);
        }

        OptExprBuilder scanBuilder = new OptExprBuilder(scanOperator, Collections.emptyList(),
                new ExpressionMapping(node.getScope(), outputVariables));
        LogicalProjectOperator projectOperator =
                new LogicalProjectOperator(outputVariables.stream().distinct()
                        .collect(Collectors.toMap(Function.identity(), Function.identity())));

        return new LogicalPlan(scanBuilder.withNewRoot(projectOperator), outputVariables, null);
    }

    @Override
    public LogicalPlan visitCTE(CTERelation node, ExpressionMapping context) {
        int cteId = cteContext.getCurrentCteRef(node.getCteMouldId());
        ExpressionMapping expressionMapping = cteContext.getCteExpressions().get(cteId);
        Map<ColumnRefOperator, ColumnRefOperator> cteOutputColumnRefMap = new HashMap<>();
        LogicalPlan childPlan = transform(node.getCteQueryStatement().getQueryRelation());

        Preconditions.checkState(childPlan.getOutputColumn().size() == expressionMapping.getFieldMappings().size());

        for (int i = 0; i < expressionMapping.getFieldMappings().size(); i++) {
            ColumnRefOperator childColumn = childPlan.getOutputColumn().get(i);
            ColumnRefOperator produceColumn = expressionMapping.getFieldMappings().get(i);

            Preconditions.checkState(childColumn.getType().equals(produceColumn.getType()));
            Preconditions.checkState(childColumn.isNullable() == produceColumn.isNullable());

            cteOutputColumnRefMap.put(childColumn, produceColumn);
        }

        LogicalCTEConsumeOperator consume = new LogicalCTEConsumeOperator(cteId, cteOutputColumnRefMap);
        OptExprBuilder consumeBuilder = new OptExprBuilder(consume, Lists.newArrayList(childPlan.getRootBuilder()),
                new ExpressionMapping(node.getScope(), childPlan.getOutputColumn()));

        return new LogicalPlan(consumeBuilder, childPlan.getOutputColumn(), null);
    }

    @Override
    public LogicalPlan visitSubquery(SubqueryRelation node, ExpressionMapping context) {
        LogicalPlan logicalPlan = transform(node.getQueryStatement().getQueryRelation());
        OptExprBuilder builder = new OptExprBuilder(
                logicalPlan.getRoot().getOp(),
                logicalPlan.getRootBuilder().getInputs(),
                new ExpressionMapping(node.getScope(), logicalPlan.getOutputColumn()));

        builder = addOrderByLimit(builder, node);
        return new LogicalPlan(builder, logicalPlan.getOutputColumn(), logicalPlan.getCorrelation());
    }

    @Override
    public LogicalPlan visitView(ViewRelation node, ExpressionMapping context) {
        LogicalPlan logicalPlan = transform(node.getQueryStatement().getQueryRelation());
        OptExprBuilder builder = new OptExprBuilder(
                logicalPlan.getRoot().getOp(),
                logicalPlan.getRootBuilder().getInputs(),
                new ExpressionMapping(node.getScope(), logicalPlan.getOutputColumn()));
        return new LogicalPlan(builder, logicalPlan.getOutputColumn(), logicalPlan.getCorrelation());
    }

    @Override
    public LogicalPlan visitJoin(JoinRelation node, ExpressionMapping context) {
        if (node.isLateral() || node.getRight() instanceof TableFunctionRelation) {
            LogicalPlan leftPlan = visit(node.getLeft());
            LogicalPlan rightPlan = visit(node.getRight(), leftPlan.getRootBuilder().getExpressionMapping());

            ExpressionMapping expressionMapping = new ExpressionMapping(new Scope(RelationId.of(node),
                    node.getLeft().getRelationFields().joinWith(node.getRight().getRelationFields())),
                    Streams.concat(leftPlan.getRootBuilder().getFieldMappings().stream(),
                                    rightPlan.getRootBuilder().getFieldMappings().stream())
                            .collect(Collectors.toList()));

            Operator root = LogicalApplyOperator.builder().setCorrelationColumnRefs(correlation)
                    .setNeedCheckMaxRows(false)
                    .setUseSemiAnti(false)
                    .setUnCorrelationSubqueryPredicateColumns(new ColumnRefSet())
                    .setNeedOutputRightChildColumns(true).build();
            return new LogicalPlan(
                    new OptExprBuilder(root, Lists.newArrayList(leftPlan.getRootBuilder(), rightPlan.getRootBuilder()),
                            expressionMapping), expressionMapping.getFieldMappings(), null);
        }

        LogicalPlan leftPlan = visit(node.getLeft());
        LogicalPlan rightPlan = visit(node.getRight());
        OptExprBuilder leftOpt = leftPlan.getRootBuilder();
        OptExprBuilder rightOpt = rightPlan.getRootBuilder();

        // The scope needs to be rebuilt here, because the scope of Semi/Anti Join
        // only has a child field. Bug on predicate needs to see the two child field
        Scope joinScope = new Scope(RelationId.of(node),
                node.getLeft().getRelationFields().joinWith(node.getRight().getRelationFields()));
        joinScope.setParent(node.getScope().getParent());
        ExpressionMapping expressionMapping = new ExpressionMapping(joinScope, Streams.concat(
                        leftOpt.getFieldMappings().stream(),
                        rightOpt.getFieldMappings().stream())
                .collect(Collectors.toList()));

        ScalarOperator onPredicate = null;
        if (node.getOnPredicate() != null) {
            Triple<ScalarOperator, OptExprBuilder, OptExprBuilder> triple = parseJoinOnPredicate(node,
                    leftOpt, rightOpt, leftPlan.getOutputColumn(), rightPlan.getOutputColumn(), expressionMapping);
            onPredicate = triple.getLeft();
            leftOpt = triple.getMiddle();
            rightOpt = triple.getRight();
        }

        // There are two cases where join on predicate is null
        // case 1: no join on predicate
        // case 2: one join on predicate containing existential/quantified subquery which will be removed after subquery rewrite procedure
        if (onPredicate == null) {
            OptExprBuilder joinOptExprBuilder = new OptExprBuilder(new LogicalJoinOperator.Builder()
                    .setJoinType(JoinOperator.CROSS_JOIN)
                    .setJoinHint(node.getJoinHint())
                    .build(), Lists.newArrayList(leftOpt, rightOpt),
                    expressionMapping);

            LogicalProjectOperator projectOperator =
                    new LogicalProjectOperator(expressionMapping.getFieldMappings().stream().distinct()
                            .collect(Collectors.toMap(Function.identity(), Function.identity())));
            return new LogicalPlan(joinOptExprBuilder.withNewRoot(projectOperator),
                    expressionMapping.getFieldMappings(), null);
        }

        ExpressionMapping outputExpressionMapping;
        if (node.getJoinOp().isLeftSemiAntiJoin()) {
            outputExpressionMapping = new ExpressionMapping(node.getScope(),
                    Lists.newArrayList(leftOpt.getFieldMappings()));
        } else if (node.getJoinOp().isRightSemiAntiJoin()) {
            outputExpressionMapping = new ExpressionMapping(node.getScope(),
                    Lists.newArrayList(rightOpt.getFieldMappings()));
        } else {
            outputExpressionMapping = new ExpressionMapping(node.getScope(), Streams.concat(
                            leftOpt.getFieldMappings().stream(),
                            rightOpt.getFieldMappings().stream())
                    .collect(Collectors.toList()));
        }

        LogicalJoinOperator joinOperator = new LogicalJoinOperator.Builder()
                .setJoinType(node.getJoinOp())
                .setOnPredicate(onPredicate)
                .setJoinHint(node.getJoinHint())
                .build();

        OptExprBuilder joinOptExprBuilder =
                new OptExprBuilder(joinOperator,
                        Lists.newArrayList(leftOpt, rightOpt),
                        outputExpressionMapping);
        LogicalProjectOperator projectOperator =
                new LogicalProjectOperator(outputExpressionMapping.getFieldMappings().stream().distinct()
                        .collect(Collectors.toMap(Function.identity(), Function.identity())));
        return new LogicalPlan(joinOptExprBuilder.withNewRoot(projectOperator),
                outputExpressionMapping.getFieldMappings(), null);
    }

    @Override
    public LogicalPlan visitTableFunction(TableFunctionRelation node, ExpressionMapping context) {
        List<ColumnRefOperator> outputColumns = new ArrayList<>();
        TableFunction tableFunction = node.getTableFunction();

        for (int i = 0; i < tableFunction.getTableFnReturnTypes().size(); ++i) {
            String colName;
            if (node.getColumnNames() == null) {
                colName = tableFunction.getDefaultColumnNames().get(i);
            } else {
                colName = node.getColumnNames().get(i);
            }

            outputColumns.add(columnRefFactory.create(colName, tableFunction.getTableFnReturnTypes().get(i), true));
        }

        FunctionCallExpr expr = new FunctionCallExpr(tableFunction.getFunctionName(), node.getChildExpressions());
        expr.setFn(tableFunction);
        ScalarOperator operator = SqlToScalarOperatorTranslator.translate(expr, context, columnRefFactory);

        if (operator.isConstantRef() && ((ConstantOperator) operator).isNull()) {
            throw new StarRocksPlannerException("table function not support null parameter", ErrorType.USER_ERROR);
        }

        List<Pair<ColumnRefOperator, ScalarOperator>> projectMap = new ArrayList<>();
        for (ScalarOperator scalarOperator : operator.getChildren()) {
            if (scalarOperator instanceof ColumnRefOperator) {
                projectMap.add(Pair.create((ColumnRefOperator) scalarOperator, scalarOperator));
            } else {
                ColumnRefOperator columnRefOperator =
                        columnRefFactory.create(scalarOperator, scalarOperator.getType(), scalarOperator.isNullable());
                projectMap.add(Pair.create(columnRefOperator, scalarOperator));
            }
        }

        Operator root = new LogicalTableFunctionOperator(
                new ColumnRefSet(outputColumns), node.getTableFunction(), projectMap);
        return new LogicalPlan(new OptExprBuilder(root, Collections.emptyList(),
                new ExpressionMapping(new Scope(RelationId.of(node), node.getRelationFields()), outputColumns)),
                null, null);
    }

    /**
     * The process is as follows:
     * Step1. Parse each conjunct of joinOnPredicate(Expr), and transforming to ScalarOperator.
     * Step2. Compound those conjuncts(ScalarOperator) together, and perform all the scalar rewritten rules, during which
     * some applyOperators may not necessary anymore.
     * Step3. Process each conjunct of output from step2, and perform subquery rewrite rule, attaching applyOperator to the
     * corresponding(left or right) logical plan
     * Step4. Compound those conjuncts from step3 together again as the final joinOnPredicate(ScalarOperator)
     */
    private Triple<ScalarOperator, OptExprBuilder, OptExprBuilder> parseJoinOnPredicate(
            JoinRelation node, OptExprBuilder leftOpt, OptExprBuilder rightOpt,
            List<ColumnRefOperator> leftOutputColumns, List<ColumnRefOperator> rightOutputColumns,
            ExpressionMapping expressionMapping) {
        // Step1
        List<Expr> exprConjuncts = Expr.extractConjuncts(node.getOnPredicate());

        List<ScalarOperator> scalarConjuncts = Lists.newArrayList();
        Map<ScalarOperator, SubqueryOperator> allSubqueryPlaceholders = Maps.newHashMap();
        // True means subquery related to join's left relation while false means right
        Map<ScalarOperator, Boolean> subqueryRelations = Maps.newHashMap();
        for (Expr exprConjunct : exprConjuncts) {
            ScalarOperator scalarConjunct;
            Map<ScalarOperator, SubqueryOperator> subqueryPlaceholders = Maps.newHashMap();
            if (isJoinLeftRelatedSubquery(node, exprConjunct)) {
                scalarConjunct = SqlToScalarOperatorTranslator.translate(exprConjunct,
                        expressionMapping, columnRefFactory,
                        session, cteContext, leftOpt, subqueryPlaceholders, true);
                allSubqueryPlaceholders.putAll(subqueryPlaceholders);
                subqueryPlaceholders.keySet().forEach(o -> subqueryRelations.put(o, true));
            } else {
                scalarConjunct = SqlToScalarOperatorTranslator.translate(exprConjunct,
                        expressionMapping, columnRefFactory,
                        session, cteContext, rightOpt, subqueryPlaceholders, true);
                allSubqueryPlaceholders.putAll(subqueryPlaceholders);
                subqueryPlaceholders.keySet().forEach(o -> subqueryRelations.put(o, false));
            }
            scalarConjuncts.add(scalarConjunct);
        }

        // Step2
        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        ScalarOperator scalarOperator = rewriter.rewrite(
                Utils.compoundAnd(scalarConjuncts),
                ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
        //  If the on-predicate condition is rewrite to false.
        //  We need to extract the equivalence conditions to meet query analysis and
        //  avoid hash joins without equivalence conditions
        if (scalarOperator.isConstant() && scalarOperator.getType().isBoolean()
                && !node.getJoinOp().isCrossJoin() && !node.getJoinOp().isInnerJoin()) {
            ScalarOperator scalarOperatorWithoutRewrite = Utils.compoundAnd(scalarConjuncts);
            List<BinaryPredicateOperator> eqPredicate = JoinHelper.getEqualsPredicate(
                    new ColumnRefSet(leftOutputColumns),
                    new ColumnRefSet(rightOutputColumns),
                    Utils.extractConjuncts(scalarOperatorWithoutRewrite));

            if (eqPredicate.size() > 0) {
                scalarOperator = Utils.compoundAnd(eqPredicate.get(0), scalarOperator);
            }
        }

        // Step3
        scalarConjuncts = Utils.extractConjuncts(scalarOperator);
        List<ScalarOperator> newScalarConjuncts = Lists.newArrayList();
        for (ScalarOperator scalarConjunct : scalarConjuncts) {
            boolean leftRelated = Utils.collect(scalarConjunct, ScalarOperator.class).stream()
                    .filter(subqueryRelations::containsKey)
                    .map(subqueryRelations::get)
                    .findAny()
                    .orElse(true);
            ScalarOperator newScalarConjunct;
            if (leftRelated) {
                Pair<ScalarOperator, OptExprBuilder> pair =
                        SubqueryUtils.rewriteScalarOperator(scalarConjunct, leftOpt, allSubqueryPlaceholders);
                newScalarConjunct = pair.first;
                leftOpt = pair.second;
            } else {
                Pair<ScalarOperator, OptExprBuilder> pair =
                        SubqueryUtils.rewriteScalarOperator(scalarConjunct, rightOpt, allSubqueryPlaceholders);
                newScalarConjunct = pair.first;
                rightOpt = pair.second;
            }
            newScalarConjuncts.add(newScalarConjunct);
        }

        // Step4
        return Triple.of(Utils.compoundAnd(newScalarConjuncts), leftOpt, rightOpt);
    }

    private boolean isJoinLeftRelatedSubquery(JoinRelation node, Expr joinOnConjunct) {
        List<Subquery> subqueries = Lists.newArrayList();
        joinOnConjunct.collect(Subquery.class, subqueries);
        Preconditions.checkState(subqueries.size() <= 1,
                "Not support ON Clause conjunct contains more than one subquery");
        if (subqueries.isEmpty()) {
            return true;
        }
        Subquery subquery = subqueries.get(0);
        QueryStatement subqueryStmt = subquery.getQueryStatement();
        SelectRelation selectRelation = (SelectRelation) subqueryStmt.getQueryRelation();
        RelationId subqueryRelationId = selectRelation.getRelation().getScope().getRelationId();
        List<FieldId> correlatedFieldIds = selectRelation.getColumnReferences().values().stream()
                .filter(field -> !Objects.equals(subqueryRelationId, field.getRelationId()))
                .collect(Collectors.toList());

        /*
         * Apply comprises two children, R and E(r) respectively
         *         ApplyOperator
         *       /              \
         *   Outer:R        Inner: E(r)
         * Since join node has two relation, we should to determine which one(left or right or both)
         * is the outer relation of ApplyOperator
         * usingLeftRelation = true, then the left relation of join will be the outer relation of apply
         * usingLeftRelation = false, then the right relation of join will be the outer relation of apply
         * TODO, both of the left and right relations should be taken into account, and it is not supported yet
         */
        final boolean usingLeftRelation;

        List<SlotRef> slotRefs = Lists.newArrayList();
        joinOnConjunct.collect(SlotRef.class, slotRefs);
        RelationFields leftRelationFields = node.getLeft().getRelationFields();
        RelationFields rightRelationFields = node.getRight().getRelationFields();
        boolean isJoinLeftNonCorrelated =
                slotRefs.stream().anyMatch(slotRef -> !leftRelationFields.resolveFields(slotRef).isEmpty());
        boolean isJoinRightNonCorrelated =
                slotRefs.stream().anyMatch(slotRef -> !rightRelationFields.resolveFields(slotRef).isEmpty());

        if (correlatedFieldIds.isEmpty()) {
            Preconditions.checkState(!(isJoinLeftNonCorrelated && isJoinRightNonCorrelated),
                    "Not support ON Clause un-correlated subquery referencing columns of more than one table");
            usingLeftRelation = isJoinLeftNonCorrelated;
        } else {
            boolean isJoinLeftCorrelated = false;
            boolean isJoinRightCorrelated = false;
            for (FieldId correlatedFieldId : correlatedFieldIds) {
                Field field = node.getRelationFields().getAllFields().get(correlatedFieldId.getFieldIndex());
                if (node.getLeft().getRelationFields().getAllFields().contains(field)) {
                    isJoinLeftCorrelated = true;
                }
                if (node.getRight().getRelationFields().getAllFields().contains(field)) {
                    isJoinRightCorrelated = true;
                }
            }
            Preconditions.checkState(isJoinLeftCorrelated || isJoinRightCorrelated);
            Preconditions.checkState(!(isJoinLeftCorrelated && isJoinRightCorrelated),
                    "Not support ON Clause correlated subquery referencing columns of more than one table");
            if (joinOnConjunct instanceof InPredicate) {
                // We DO NOT support this kind of in-predicate like below
                // select * from t0 join t1 on t0.v1 in (select t2.v7 from t2 where t1.v5 = t2.v8)
                Preconditions.checkState(!(isJoinLeftCorrelated && isJoinRightNonCorrelated ||
                                isJoinRightCorrelated && isJoinLeftNonCorrelated),
                        "Not support ON Clause correlated in-subquery referencing columns of more than one table");
            }
            usingLeftRelation = isJoinLeftCorrelated;
        }
        return usingLeftRelation;
    }
}
