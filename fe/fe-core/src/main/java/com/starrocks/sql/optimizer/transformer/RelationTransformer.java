// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.transformer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.HashDistributionInfo;
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
import com.starrocks.sql.analyzer.relation.CTERelation;
import com.starrocks.sql.analyzer.relation.ExceptRelation;
import com.starrocks.sql.analyzer.relation.IntersectRelation;
import com.starrocks.sql.analyzer.relation.JoinRelation;
import com.starrocks.sql.analyzer.relation.QueryRelation;
import com.starrocks.sql.analyzer.relation.Relation;
import com.starrocks.sql.analyzer.relation.RelationVisitor;
import com.starrocks.sql.analyzer.relation.SelectRelation;
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
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
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
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class RelationTransformer extends RelationVisitor<LogicalPlan, ExpressionMapping> {
    private final ColumnRefFactory columnRefFactory;
    private final ConnectContext session;

    private final ExpressionMapping outer;
    private final Map<String, ExpressionMapping> cteContext;
    private final List<ColumnRefOperator> correlation = new ArrayList<>();

    public RelationTransformer(ColumnRefFactory columnRefFactory, ConnectContext session) {
        this.columnRefFactory = columnRefFactory;
        this.session = session;
        this.outer = new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()));
        this.cteContext = new HashMap<>();
    }

    public RelationTransformer(ColumnRefFactory columnRefFactory, ConnectContext session, ExpressionMapping outer,
                               Map<String, ExpressionMapping> cteContext) {
        this.columnRefFactory = columnRefFactory;
        this.session = session;
        this.cteContext = cteContext;
        this.outer = outer;
    }

    // transform relation to plan with session variable sql_select_limit
    // only top relation need set limit, transform method used by CTE/Subquery
    public LogicalPlan transformWithSelectLimit(Relation relation) {
        LogicalPlan plan = transform(relation);
        OptExprBuilder root = plan.getRootBuilder();
        // Set limit if user set sql_select_limit.
        long selectLimit = ConnectContext.get().getSessionVariable().getSqlSelectLimit();
        if (!root.getRoot().getOp().hasLimit() &&
                selectLimit != SessionVariable.DEFAULT_SELECT_LIMIT) {
            LogicalLimitOperator limitOperator = new LogicalLimitOperator(selectLimit);
            root = root.withNewRoot(limitOperator);
            return new LogicalPlan(root, plan.getOutputColumn(), plan.getCorrelation());
        }

        return plan;
    }

    public LogicalPlan transform(Relation relation) {
        OptExprBuilder optExprBuilder;
        if (relation instanceof QueryRelation && !((QueryRelation) relation).getCteRelations().isEmpty()
                && session.getSessionVariable().isCboCteReuse()) {
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
            LogicalCTEAnchorOperator anchorOperator = new LogicalCTEAnchorOperator(cteRelation.getCteId());
            LogicalCTEProduceOperator produceOperator = new LogicalCTEProduceOperator(cteRelation.getCteId());
            LogicalPlan producerPlan =
                    new RelationTransformer(columnRefFactory, session,
                            new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields())),
                            cteContext).transform(cteRelation.getCteQuery());
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

            cteContext.put(cteRelation.getCteId(), new ExpressionMapping(
                    new Scope(RelationId.of(cteRelation.getCteQuery()), cteRelation.getRelationFields()),
                    producerPlan.getOutputColumn()));
        }

        return new Pair<>(root, anchorOptBuilder);
    }

    @Override
    public LogicalPlan visitQuery(QueryRelation node, ExpressionMapping context) {
        throw new StarRocksPlannerException("query block not materialized", ErrorType.INTERNAL_ERROR);
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

        LogicalOperator setOperator;
        if (setOperationRelation instanceof UnionRelation) {
            setOperator = new LogicalUnionOperator.Builder()
                    .setOutputColumnRefOp(outputColumns)
                    .setChildOutputColumns(childOutputColumnList)
                    .isUnionAll(!SetQualifier.DISTINCT.equals(setOperationRelation.getQualifier()))
                    .build();

            if (setOperationRelation.getQualifier().equals(SetQualifier.DISTINCT)) {
                OptExprBuilder unionOpt = new OptExprBuilder(setOperator, childPlan, expressionMapping);
                return new LogicalPlan(new OptExprBuilder(
                        new LogicalAggregationOperator(AggType.GLOBAL, outputColumns, Maps.newHashMap()),
                        Lists.newArrayList(unionOpt), expressionMapping), outputColumns, null);
            }
        } else if (setOperationRelation instanceof ExceptRelation) {
            setOperator = new LogicalExceptOperator.Builder()
                    .setOutputColumnRefOp(outputColumns)
                    .setChildOutputColumns(childOutputColumnList).build();
        } else if (setOperationRelation instanceof IntersectRelation) {
            setOperator = new LogicalIntersectOperator.Builder()
                    .setOutputColumnRefOp(outputColumns)
                    .setChildOutputColumns(childOutputColumnList).build();
        } else {
            throw unsupportedException("New Planner only support Query Statement");
        }

        return new LogicalPlan(new OptExprBuilder(setOperator, childPlan, expressionMapping), outputColumns, null);
    }

    @Override
    public LogicalPlan visitValues(ValuesRelation node, ExpressionMapping context) {
        return new ValuesTransformer(columnRefFactory, session, cteContext).plan(node);
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
            scanOperator = new LogicalIcebergScanOperator(node.getTable(), node.getTable().getType(),
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
        if (session.getSessionVariable().isCboCteReuse()) {
            ExpressionMapping expressionMapping = cteContext.get(node.getCteId());
            List<ColumnRefOperator> cteOutputs = new ArrayList<>();
            Map<ColumnRefOperator, ColumnRefOperator> cteOutputColumnRefMap = new HashMap<>();
            for (ColumnRefOperator columnRefOperator : expressionMapping.getFieldMappings()) {
                ColumnRefOperator c = columnRefFactory.create(columnRefOperator, columnRefOperator.getType(),
                        columnRefOperator.isNullable());
                cteOutputs.add(c);
                cteOutputColumnRefMap.put(c, columnRefOperator);
            }

            return new LogicalPlan(
                    new OptExprBuilder(new LogicalCTEConsumeOperator(node.getCteId(), cteOutputColumnRefMap),
                            Collections.emptyList(), new ExpressionMapping(node.getScope(), cteOutputs)),
                    null, null);
        } else {
            LogicalPlan logicalPlan = visit(node.getCteQuery());
            return new LogicalPlan(
                    new OptExprBuilder(logicalPlan.getRoot().getOp(), logicalPlan.getRootBuilder().getInputs(),
                            new ExpressionMapping(node.getScope(), logicalPlan.getOutputColumn())),
                    logicalPlan.getOutputColumn(),
                    logicalPlan.getCorrelation());
        }
    }

    @Override
    public LogicalPlan visitSubquery(SubqueryRelation node, ExpressionMapping context) {
        LogicalPlan logicalPlan = transform(node.getQuery());
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

            Operator root = new LogicalApplyOperator(null, null, correlation, false);
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
                && !node.getType().isCrossJoin() && !node.getType().isInnerJoin()) {

            List<ScalarOperator> conjuncts = Utils.extractConjuncts(onPredicateWithoutRewrite);

            List<BinaryPredicateOperator> eqPredicate = JoinPredicateUtils.getEqConj(
                    new ColumnRefSet(leftPlan.getOutputColumn()),
                    new ColumnRefSet(rightPlan.getOutputColumn()), conjuncts);

            if (eqPredicate.size() > 0) {
                onPredicate = Utils.compoundAnd(eqPredicate.get(0), onPredicate);
            }
        }

        ExpressionMapping outputExpressionMapping;
        if (node.getType().isLeftSemiAntiJoin()) {
            outputExpressionMapping = new ExpressionMapping(node.getScope(),
                    Lists.newArrayList(leftPlan.getRootBuilder().getFieldMappings()));
        } else if (node.getType().isRightSemiAntiJoin()) {
            outputExpressionMapping = new ExpressionMapping(node.getScope(),
                    Lists.newArrayList(rightPlan.getRootBuilder().getFieldMappings()));
        } else {
            outputExpressionMapping = new ExpressionMapping(node.getScope(), Streams.concat(
                    leftPlan.getRootBuilder().getFieldMappings().stream(),
                    rightPlan.getRootBuilder().getFieldMappings().stream())
                    .collect(Collectors.toList()));
        }

        LogicalJoinOperator joinOperator = new LogicalJoinOperator.Builder()
                .setJoinType(node.getType())
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
        return new LogicalPlan(new OptExprBuilder(root, Collections.emptyList(),
                new ExpressionMapping(new Scope(RelationId.of(node), node.getRelationFields()), outputColumns)),
                null, null);
    }
}
