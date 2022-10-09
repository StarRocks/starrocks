// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.transformer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
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
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.base.SetQualifier;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        this.columnRefFactory = columnRefFactory;
        this.session = session;
        this.outer = new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()));
        this.cteContext = new CTETransformerContext();
    }

    public RelationTransformer(ColumnRefFactory columnRefFactory, ConnectContext session, ExpressionMapping outer,
                               CTETransformerContext cteContext) {
        this.columnRefFactory = columnRefFactory;
        this.session = session;
        this.cteContext = cteContext;
        this.outer = outer;
    }

    // transform relation to plan with session variable sql_select_limit
    // only top relation need set limit, transform method used by CTE/Subquery/Insert
    public LogicalPlan transformWithSelectLimit(Relation relation) {
        LogicalPlan plan = transform(relation);
        OptExprBuilder root = plan.getRootBuilder();
        // Set limit if user set sql_select_limit.
        long selectLimit = ConnectContext.get().getSessionVariable().getSqlSelectLimit();
        if (!root.getRoot().getOp().hasLimit() &&
                selectLimit != SessionVariable.DEFAULT_SELECT_LIMIT) {
            LogicalLimitOperator limitOperator = LogicalLimitOperator.local(selectLimit);
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
                                row.set(i, ((ConstantOperator) row.get(i)).castTo(outputType));
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
                        projections.put(c, new CastOperator(outputType, childOutputColumn.get(i), true));
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

        if (setOperationRelation.hasOrderByClause()) {
            List<Ordering> orderings = new ArrayList<>();
            List<ColumnRefOperator> orderByColumns = Lists.newArrayList();
            for (OrderByElement item : setOperationRelation.getOrderBy()) {
                ColumnRefOperator column = (ColumnRefOperator) SqlToScalarOperatorTranslator.translate(item.getExpr(),
                        root.getExpressionMapping());
                Ordering ordering = new Ordering(column, item.getIsAsc(),
                        OrderByElement.nullsFirst(item.getNullsFirstParam()));
                if (!orderByColumns.contains(column)) {
                    orderByColumns.add(column);
                    orderings.add(ordering);
                }
            }
            root = root.withNewRoot(new LogicalTopNOperator(orderings));
        }

        LimitElement limit = setOperationRelation.getLimit();
        if (limit != null) {
            LogicalLimitOperator limitOperator = LogicalLimitOperator.init(limit.getLimit(), limit.getOffset());
            root = root.withNewRoot(limitOperator);
        }
        return new LogicalPlan(root, outputColumns, null);
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
                        new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields())));
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
        LogicalScanOperator scanOperator;
        if (node.getTable().getType().equals(Table.TableType.OLAP)) {
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
                        Lists.newArrayList(),
                        node.getTabletIds());
            }
        } else if (Table.TableType.HIVE.equals(node.getTable().getType())) {
            scanOperator = new LogicalHiveScanOperator(node.getTable(), node.getTable().getType(),
                    colRefToColumnMetaMapBuilder.build(), columnMetaToColRefMap, Operator.DEFAULT_LIMIT, null);
        } else if (Table.TableType.ICEBERG.equals(node.getTable().getType())) {
            ((IcebergTable) node.getTable()).refreshTable();
            scanOperator = new LogicalIcebergScanOperator(node.getTable(), node.getTable().getType(),
                    colRefToColumnMetaMapBuilder.build(), columnMetaToColRefMap, Operator.DEFAULT_LIMIT, null);
        } else if (Table.TableType.HUDI.equals(node.getTable().getType())) {
            scanOperator = new LogicalHudiScanOperator(node.getTable(), node.getTable().getType(),
                    colRefToColumnMetaMapBuilder.build(), columnMetaToColRefMap, Operator.DEFAULT_LIMIT, null);
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


        if (node.hasOrderByClause()) {
            List<Ordering> orderings = new ArrayList<>();
            List<ColumnRefOperator> orderByColumns = Lists.newArrayList();
            for (OrderByElement item : node.getOrderBy()) {
                ColumnRefOperator column = (ColumnRefOperator) SqlToScalarOperatorTranslator.translate(item.getExpr(),
                        builder.getExpressionMapping(), columnRefFactory);
                Ordering ordering = new Ordering(column, item.getIsAsc(),
                        OrderByElement.nullsFirst(item.getNullsFirstParam()));
                if (!orderByColumns.contains(column)) {
                    orderByColumns.add(column);
                    orderings.add(ordering);
                }
            }
            builder = builder.withNewRoot(new LogicalTopNOperator(orderings));
        }

        LimitElement limit = node.getLimit();
        if (limit != null) {
            LogicalLimitOperator limitOperator = LogicalLimitOperator.init(limit.getLimit(), limit.getOffset());
            builder = builder.withNewRoot(limitOperator);
        }

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

            Operator root = new LogicalApplyOperator(null, null, correlation, false, true);
            return new LogicalPlan(
                    new OptExprBuilder(root, Lists.newArrayList(leftPlan.getRootBuilder(), rightPlan.getRootBuilder()),
                            expressionMapping), expressionMapping.getFieldMappings(), null);
        }

        LogicalPlan leftPlan = visit(node.getLeft());
        LogicalPlan rightPlan = visit(node.getRight());

        // The scope needs to be rebuilt here, because the scope of Semi/Anti Join
        // only has a child field. Bug on predicate needs to see the two child field
        Scope joinScope = new Scope(RelationId.of(node),
                node.getLeft().getRelationFields().joinWith(node.getRight().getRelationFields()));
        joinScope.setParent(node.getScope().getParent());
        ExpressionMapping expressionMapping = new ExpressionMapping(joinScope, Streams.concat(
                        leftPlan.getRootBuilder().getFieldMappings().stream(),
                        rightPlan.getRootBuilder().getFieldMappings().stream())
                .collect(Collectors.toList()));

        if (node.getOnPredicate() == null) {
            OptExprBuilder joinOptExprBuilder = new OptExprBuilder(new LogicalJoinOperator.Builder()
                    .setJoinType(JoinOperator.CROSS_JOIN)
                    .setJoinHint(node.getJoinHint())
                    .build(), Lists.newArrayList(leftPlan.getRootBuilder(), rightPlan.getRootBuilder()),
                    expressionMapping);

            LogicalProjectOperator projectOperator =
                    new LogicalProjectOperator(expressionMapping.getFieldMappings().stream().distinct()
                            .collect(Collectors.toMap(Function.identity(), Function.identity())));
            return new LogicalPlan(joinOptExprBuilder.withNewRoot(projectOperator),
                    expressionMapping.getFieldMappings(), null);
        }

        ScalarOperator onPredicateWithoutRewrite = SqlToScalarOperatorTranslator
                .translateWithoutRewrite(node.getOnPredicate(), expressionMapping);
        ScalarOperator onPredicate = SqlToScalarOperatorTranslator.translate(node.getOnPredicate(), expressionMapping);

        /*
         * If the on-predicate condition is rewrite to false.
         * We need to extract the equivalence conditions to meet query analysis and
         * avoid hash joins without equivalence conditions
         */
        if (onPredicate.isConstant() && onPredicate.getType().isBoolean()
                && !node.getJoinOp().isCrossJoin() && !node.getJoinOp().isInnerJoin()) {

            List<ScalarOperator> conjuncts = Utils.extractConjuncts(onPredicateWithoutRewrite);

            List<BinaryPredicateOperator> eqPredicate = JoinHelper.getEqualsPredicate(
                    new ColumnRefSet(leftPlan.getOutputColumn()),
                    new ColumnRefSet(rightPlan.getOutputColumn()), conjuncts);

            if (eqPredicate.size() > 0) {
                onPredicate = Utils.compoundAnd(eqPredicate.get(0), onPredicate);
            }
        }

        ExpressionMapping outputExpressionMapping;
        if (node.getJoinOp().isLeftSemiAntiJoin()) {
            outputExpressionMapping = new ExpressionMapping(node.getScope(),
                    Lists.newArrayList(leftPlan.getRootBuilder().getFieldMappings()));
        } else if (node.getJoinOp().isRightSemiAntiJoin()) {
            outputExpressionMapping = new ExpressionMapping(node.getScope(),
                    Lists.newArrayList(rightPlan.getRootBuilder().getFieldMappings()));
        } else {
            outputExpressionMapping = new ExpressionMapping(node.getScope(), Streams.concat(
                            leftPlan.getRootBuilder().getFieldMappings().stream(),
                            rightPlan.getRootBuilder().getFieldMappings().stream())
                    .collect(Collectors.toList()));
        }

        LogicalJoinOperator joinOperator = new LogicalJoinOperator.Builder()
                .setJoinType(node.getJoinOp())
                .setOnPredicate(onPredicate)
                .setJoinHint(node.getJoinHint())
                .build();

        OptExprBuilder joinOptExprBuilder =
                new OptExprBuilder(joinOperator,
                        Lists.newArrayList(leftPlan.getRootBuilder(), rightPlan.getRootBuilder()),
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
            outputColumns.add(columnRefFactory.create(
                    tableFunction.getDefaultColumnNames().get(i),
                    tableFunction.getTableFnReturnTypes().get(i),
                    true));
        }

        FunctionCallExpr expr = new FunctionCallExpr(tableFunction.getFunctionName(), node.getChildExpressions());
        expr.setFn(tableFunction);
        ScalarOperator operator = SqlToScalarOperatorTranslator.translate(expr, context);

        if (operator.isConstantRef() && ((ConstantOperator) operator).isNull()) {
            throw new StarRocksPlannerException("table function not support null parameter", ErrorType.USER_ERROR);
        }

        Map<ColumnRefOperator, ScalarOperator> projectMap = new HashMap<>();
        for (ScalarOperator scalarOperator : operator.getChildren()) {
            if (scalarOperator instanceof ColumnRefOperator) {
                projectMap.put((ColumnRefOperator) scalarOperator, scalarOperator);
            } else {
                ColumnRefOperator columnRefOperator =
                        columnRefFactory.create(scalarOperator, scalarOperator.getType(), scalarOperator.isNullable());
                projectMap.put(columnRefOperator, scalarOperator);
            }
        }

        Operator root =
                new LogicalTableFunctionOperator(new ColumnRefSet(outputColumns), node.getTableFunction(), projectMap);
        return new LogicalPlan(new OptExprBuilder(root, Collections.emptyList(),
                new ExpressionMapping(new Scope(RelationId.of(node), node.getRelationFields()), outputColumns)),
                null, null);
    }
}
