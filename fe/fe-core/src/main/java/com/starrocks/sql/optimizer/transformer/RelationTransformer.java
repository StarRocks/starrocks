// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.transformer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunction;
import com.starrocks.external.elasticsearch.EsTablePartitions;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.relation.ExceptRelation;
import com.starrocks.sql.analyzer.relation.IntersectRelation;
import com.starrocks.sql.analyzer.relation.JoinRelation;
import com.starrocks.sql.analyzer.relation.QueryRelation;
import com.starrocks.sql.analyzer.relation.QuerySpecification;
import com.starrocks.sql.analyzer.relation.Relation;
import com.starrocks.sql.analyzer.relation.RelationVisitor;
import com.starrocks.sql.analyzer.relation.SetOperationRelation;
import com.starrocks.sql.analyzer.relation.SubqueryRelation;
import com.starrocks.sql.analyzer.relation.TableFunctionRelation;
import com.starrocks.sql.analyzer.relation.TableRelation;
import com.starrocks.sql.analyzer.relation.UnionRelation;
import com.starrocks.sql.analyzer.relation.ValuesRelation;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.SetQualifier;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;
import static com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator.findOrCreateColumnRefForExpr;

public class RelationTransformer extends RelationVisitor<OptExprBuilder, ExpressionMapping> {
    private final ColumnRefFactory columnRefFactory;
    private List<ColumnRefOperator> outputColumn;
    private List<ColumnRefOperator> correlation = new ArrayList<>();
    private final ExpressionMapping outer;

    public RelationTransformer(ColumnRefFactory columnRefFactory) {
        this.columnRefFactory = columnRefFactory;
        this.outer = new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()));
    }

    public RelationTransformer(ColumnRefFactory columnRefFactory, ExpressionMapping outer) {
        this.columnRefFactory = columnRefFactory;
        this.outer = outer;
    }

    public LogicalPlan transform(Relation relation) {
        // Set limit if user set sql_select_limit.
        if (relation instanceof QuerySpecification) {
            QuerySpecification querySpecification = (QuerySpecification) relation;
            long selectLimit = ConnectContext.get().getSessionVariable().getSqlSelectLimit();
            if (!querySpecification.hasLimit() &&
                    selectLimit != SessionVariable.DEFAULT_SELECT_LIMIT) {
                querySpecification.setLimit(new LimitElement(selectLimit));
            }
        }
        OptExprBuilder optExprBuilder = visit(relation);
        return new LogicalPlan(optExprBuilder, outputColumn, correlation);
    }

    @Override
    public OptExprBuilder visitQuery(QueryRelation node, ExpressionMapping context) {
        throw new StarRocksPlannerException("query block not materialized", ErrorType.INTERNAL_ERROR);
    }

    @Override
    public OptExprBuilder visitQuerySpecification(QuerySpecification node, ExpressionMapping context) {
        LogicalPlan logicalPlan = new QueryTransformer(columnRefFactory, outer).plan(node);

        outputColumn = logicalPlan.getOutputColumn();
        correlation = logicalPlan.getCorrelation();
        return logicalPlan.getRootBuilder();
    }

    @Override
    public OptExprBuilder visitUnion(UnionRelation node, ExpressionMapping context) {
        return processSetOperation(node);
    }

    @Override
    public OptExprBuilder visitExcept(ExceptRelation node, ExpressionMapping context) {
        return processSetOperation(node);
    }

    @Override
    public OptExprBuilder visitIntersect(IntersectRelation node, ExpressionMapping context) {
        return processSetOperation(node);
    }

    private OptExprBuilder processSetOperation(SetOperationRelation setOperationRelation) {
        List<OptExprBuilder> childPlan = new ArrayList<>();
        boolean first = true;
        /*
         * setColumns records the columns of all children,
         * which are used for column prune.
         * If only the first child is recorded, it may cause the
         * columns used in other children to be pruned
         */
        List<ColumnRefOperator> outputColumns = new ArrayList<>();
        List<List<ColumnRefOperator>> childOutputColumns = new ArrayList<>();
        for (QueryRelation relation : setOperationRelation.getRelations()) {
            OptExprBuilder optExprBuilder = visit(relation);
            if (first) {
                for (ColumnRefOperator c : this.outputColumn) {
                    outputColumns.add(columnRefFactory.create(c, c.getType(), c.isNullable()));
                }
                first = false;
            } else {
                for (int i = 0; i < this.outputColumn.size(); ++i) {
                    if (!outputColumns.get(i).isNullable() && this.outputColumn.get(i).isNullable()) {
                        outputColumns.get(i).setNullable(true);
                    }
                }
            }

            // Note: must copy here
            childOutputColumns.add(Lists.newArrayList(outputColumn));

            if (!(optExprBuilder.getRoot().getOp() instanceof LogicalProjectOperator) &&
                    !(optExprBuilder.getRoot().getOp() instanceof LogicalValuesOperator)) {

                ExpressionMapping outputTranslations =
                        new ExpressionMapping(optExprBuilder.getScope(), optExprBuilder.getFieldMappings());

                Map<ColumnRefOperator, ScalarOperator> projections = Maps.newHashMap();
                for (Expr expression : relation.getOutputExpr()) {
                    ColumnRefOperator columnRef = findOrCreateColumnRefForExpr(expression,
                            optExprBuilder.getExpressionMapping(), projections, columnRefFactory);
                    outputTranslations.put(expression, columnRef);
                }

                LogicalProjectOperator projectOperator = new LogicalProjectOperator(projections);
                optExprBuilder =
                        new OptExprBuilder(projectOperator, Lists.newArrayList(optExprBuilder), outputTranslations);
            }
            childPlan.add(optExprBuilder);
        }

        Scope outputScope = setOperationRelation.getRelations().get(0).getOutputScope();
        ExpressionMapping expressionMapping = new ExpressionMapping(outputScope, outputColumns);

        LogicalOperator setOperator;
        if (setOperationRelation instanceof UnionRelation) {
            setOperator = new LogicalUnionOperator.Builder()
                    .setOutputColumnRefOp(outputColumns)
                    .setChildOutputColumns(childOutputColumns)
                    .isUnionAll(!SetQualifier.DISTINCT.equals(setOperationRelation.getQualifier()))
                    .build();

            if (setOperationRelation.getQualifier().equals(SetQualifier.DISTINCT)) {
                OptExprBuilder unionOpt = new OptExprBuilder(setOperator, childPlan, expressionMapping);
                this.outputColumn = outputColumns;
                return new OptExprBuilder(
                        new LogicalAggregationOperator(AggType.GLOBAL, outputColumns, Maps.newHashMap()),
                        Lists.newArrayList(unionOpt), expressionMapping);
            }
        } else if (setOperationRelation instanceof ExceptRelation) {
            setOperator = new LogicalExceptOperator.Builder()
                    .setOutputColumnRefOp(outputColumns)
                    .setChildOutputColumns(childOutputColumns).build();

        } else if (setOperationRelation instanceof IntersectRelation) {
            setOperator = new LogicalIntersectOperator.Builder()
                    .setOutputColumnRefOp(outputColumns)
                    .setChildOutputColumns(childOutputColumns).build();
        } else {
            throw unsupportedException("New Planner only support Query Statement");
        }

        this.outputColumn = outputColumns;
        return new OptExprBuilder(setOperator, childPlan, expressionMapping);
    }

    @Override
    public OptExprBuilder visitValues(ValuesRelation node, ExpressionMapping context) {
        LogicalPlan logicalPlan = new ValuesTransformer(columnRefFactory).plan(node);
        outputColumn = logicalPlan.getOutputColumn();
        return logicalPlan.getRootBuilder();
    }

    @Override
    public OptExprBuilder visitTable(TableRelation node, ExpressionMapping context) {
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
                        -1,
                        null,
                        ((OlapTable) node.getTable()).getBaseIndexId(),
                        null,
                        node.getPartitionNames(),
                        Lists.newArrayList(),
                        node.getTabletIds());
            }
        } else if (Table.TableType.HIVE.equals(node.getTable().getType())) {
            scanOperator = new LogicalHiveScanOperator(node.getTable(), node.getTable().getType(),
                    colRefToColumnMetaMapBuilder.build(), columnMetaToColRefMap, -1, null);
        } else if (Table.TableType.SCHEMA.equals(node.getTable().getType())) {
            scanOperator =
                    new LogicalSchemaScanOperator(node.getTable(),
                            colRefToColumnMetaMapBuilder.build(),
                            columnMetaToColRefMap, -1,
                            null, null);
        } else if (Table.TableType.MYSQL.equals(node.getTable().getType())) {
            scanOperator =
                    new LogicalMysqlScanOperator(node.getTable(), colRefToColumnMetaMapBuilder.build(),
                            columnMetaToColRefMap, -1,
                            null, null);
        } else if (Table.TableType.ELASTICSEARCH.equals(node.getTable().getType())) {
            scanOperator =
                    new LogicalEsScanOperator(node.getTable(), colRefToColumnMetaMapBuilder.build(),
                            columnMetaToColRefMap, -1,
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
        } else {
            throw new StarRocksPlannerException("Not support table type: " + node.getTable().getType(),
                    ErrorType.UNSUPPORTED);
        }

        OptExprBuilder scanBuilder = new OptExprBuilder(scanOperator, Collections.emptyList(),
                new ExpressionMapping(new Scope(RelationId.of(node), node.getRelationFields()), outputVariables));
        LogicalProjectOperator projectOperator =
                new LogicalProjectOperator(outputVariables.stream().distinct()
                        .collect(Collectors.toMap(Function.identity(), Function.identity())));
        return scanBuilder.withNewRoot(projectOperator);
    }

    @Override
    public OptExprBuilder visitSubquery(SubqueryRelation node, ExpressionMapping context) {
        OptExprBuilder builder = visit(node.getQuery());
        return new OptExprBuilder(builder.getRoot().getOp(), builder.getInputs(),
                new ExpressionMapping(new Scope(RelationId.of(node), node.getRelationFields()), outputColumn));
    }

    @Override
    public OptExprBuilder visitJoin(JoinRelation node, ExpressionMapping context) {
        if (node.isLateral() || node.getRight() instanceof TableFunctionRelation) {
            OptExprBuilder leftPlan = visit(node.getLeft());
            OptExprBuilder rightPlan = visit(node.getRight(), leftPlan.getExpressionMapping());

            ExpressionMapping expressionMapping = new ExpressionMapping(
                    new Scope(RelationId.of(node),
                            node.getLeft().getRelationFields().joinWith(node.getRight().getRelationFields())),
                    Streams.concat(leftPlan.getFieldMappings().stream(), rightPlan.getFieldMappings().stream())
                            .collect(Collectors.toList()));

            Operator root = new LogicalApplyOperator(null, null, correlation, false);
            return new OptExprBuilder(root, Lists.newArrayList(leftPlan, rightPlan), expressionMapping);
        }

        OptExprBuilder leftPlan = visit(node.getLeft());
        OptExprBuilder rightPlan = visit(node.getRight());

        ExpressionMapping expressionMapping = new ExpressionMapping(
                new Scope(RelationId.of(node),
                        node.getLeft().getRelationFields().joinWith(node.getRight().getRelationFields())),
                Streams.concat(leftPlan.getFieldMappings().stream(), rightPlan.getFieldMappings().stream())
                        .collect(Collectors.toList()));

        if (node.getOnPredicate() == null) {
            OptExprBuilder joinOptExprBuilder = new OptExprBuilder(new LogicalJoinOperator.Builder()
                    .setJoinType(JoinOperator.CROSS_JOIN)
                    .setJoinHint(node.getJoinHint())
                    .build(), Lists.newArrayList(leftPlan, rightPlan), expressionMapping);

            LogicalProjectOperator projectOperator =
                    new LogicalProjectOperator(expressionMapping.getFieldMappings().stream().distinct()
                            .collect(Collectors.toMap(Function.identity(), Function.identity())));
            return joinOptExprBuilder.withNewRoot(projectOperator);
        }

        ScalarOperator onPredicateWithoutRewrite = SqlToScalarOperatorTranslator
                .translateWithoutRewrite(node.getOnPredicate(), expressionMapping, null, null);
        ScalarOperator onPredicate = SqlToScalarOperatorTranslator.translate(node.getOnPredicate(), expressionMapping);

        /*
         * If the on-predicate condition is rewrite to false.
         * We need to extract the equivalence conditions to meet query analysis and
         * avoid hash joins without equivalence conditions
         */
        if (onPredicate.isConstant() && onPredicate.getType().isBoolean()
                && !node.getType().isCrossJoin() && !node.getType().isInnerJoin()) {

            List<ScalarOperator> eqConj = new ArrayList<>();
            List<ScalarOperator> conjuncts = Utils.extractConjuncts(onPredicateWithoutRewrite);
            for (ScalarOperator conj : conjuncts) {
                if (conj instanceof BinaryPredicateOperator && ((BinaryPredicateOperator) conj).getBinaryType()
                        .equals(BinaryPredicateOperator.BinaryType.EQ)) {
                    if (!Utils.extractColumnRef(conj.getChild(0)).isEmpty() && !Utils.extractColumnRef(conj.getChild(1))
                            .isEmpty()) {
                        eqConj.add(conj);
                    }
                }
            }

            onPredicate = Utils.compoundAnd(Utils.compoundAnd(eqConj), onPredicate);
        }

        ExpressionMapping outputExpressionMapping;
        if (node.getType().isLeftSemiAntiJoin()) {
            outputExpressionMapping =
                    new ExpressionMapping(new Scope(RelationId.of(node), node.getLeft().getRelationFields()),
                            Lists.newArrayList(leftPlan.getFieldMappings()));
        } else if (node.getType().isRightSemiAntiJoin()) {
            outputExpressionMapping =
                    new ExpressionMapping(new Scope(RelationId.of(node), node.getRight().getRelationFields()),
                            Lists.newArrayList(rightPlan.getFieldMappings()));
        } else {
            outputExpressionMapping = expressionMapping;
        }

        LogicalJoinOperator joinOperator = new LogicalJoinOperator.Builder()
                .setJoinType(node.getType())
                .setOnPredicate(onPredicate)
                .setJoinHint(node.getJoinHint())
                .build();

        OptExprBuilder joinOptExprBuilder =
                new OptExprBuilder(joinOperator, Lists.newArrayList(leftPlan, rightPlan), outputExpressionMapping);
        LogicalProjectOperator projectOperator =
                new LogicalProjectOperator(outputExpressionMapping.getFieldMappings().stream().distinct()
                        .collect(Collectors.toMap(Function.identity(), Function.identity())));
        return joinOptExprBuilder.withNewRoot(projectOperator);
    }

    @Override
    public OptExprBuilder visitTableFunction(TableFunctionRelation node, ExpressionMapping context) {
        List<ColumnRefOperator> outputColumns = new ArrayList<>();
        TableFunction tableFunction = node.getTableFunction();

        for (int i = 0; i < tableFunction.getTableFnReturnTypes().size(); ++i) {
            outputColumns.add(columnRefFactory.create(
                    tableFunction.getDefaultColumnNames().get(i),
                    tableFunction.getTableFnReturnTypes().get(i),
                    true));
        }

        Map<ColumnRefOperator, ScalarOperator> projectMap = new HashMap<>();
        for (Expr e : node.getChildExpressions()) {
            ScalarOperator scalarOperator = SqlToScalarOperatorTranslator.translate(e, context);
            if (e instanceof SlotRef) {
                projectMap.put((ColumnRefOperator) scalarOperator, scalarOperator);
            } else {
                ColumnRefOperator columnRefOperator = columnRefFactory.create(e, e.getType(), e.isNullable());
                projectMap.put(columnRefOperator, scalarOperator);
                context.put(e, columnRefOperator);
            }
        }

        Operator root =
                new LogicalTableFunctionOperator(new ColumnRefSet(outputColumns), node.getTableFunction(), projectMap);
        return new OptExprBuilder(root, Collections.emptyList(),
                new ExpressionMapping(new Scope(RelationId.of(node), node.getRelationFields()), outputColumns));
    }
}
