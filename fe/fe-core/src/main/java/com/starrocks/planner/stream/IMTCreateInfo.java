// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.planner.stream;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.physical.PhysicalStreamAggOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TIMTType;
import com.starrocks.thrift.TResultSinkType;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Information about how to create an IMT
 */
public class IMTCreateInfo {

    // TODO support other tables
    private final TIMTType imtType = TIMTType.OLAP_TABLE;
    private final String tableName;
    private final List<Column> columns;
    // TODO support other keys
    private final KeysType keyType = KeysType.PRIMARY_KEYS;
    private final PartitionInfo partitionInfo;
    private final DistributionInfo distributionInfo;
    private final Table.TableType tableType = Table.TableType.OLAP;
    private String comment;
    // TODO: rollup information

    public IMTCreateInfo(String tableName, List<Column> columns, PartitionInfo partitionInfo,
                         DistributionInfo distributionInfo) {
        this.tableName = tableName;
        this.columns = columns;
        this.partitionInfo = partitionInfo;
        this.distributionInfo = distributionInfo;
    }

    /**
     * Analyze IMT for MV
     *
     * @return map of id to IMT
     */
    public static Map<Integer, IMTCreateInfo> analyzeFromMV(MaterializedView mv, ConnectContext ctx) {
        SessionVariable session = ctx.getSessionVariable();
        String viewDefine = mv.getViewDefineSql();
        StatementBase sqlStmt = SqlParser.parse(viewDefine, ctx.getSessionVariable()).get(0);
        QueryStatement queryStmt = (QueryStatement) sqlStmt;
        QueryRelation queryRelation = queryStmt.getQueryRelation();
        Analyzer.analyze(sqlStmt, ctx);

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        List<String> colNames = queryRelation.getColumnOutputNames();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, ctx).transform(queryRelation);

        Optimizer optimizer = new Optimizer();
        ExecPlan execPlan;
        try {
            session.enableStreamPlanner(true);
            OptExpression optimizedPlan = optimizer.optimize(
                    ctx,
                    logicalPlan.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()),
                    columnRefFactory);

            execPlan = new PlanFragmentBuilder().createPhysicalPlan(
                    optimizedPlan, ctx, logicalPlan.getOutputColumn(), columnRefFactory, colNames,
                    TResultSinkType.MYSQL_PROTOCAL, false);
        } finally {
            session.enableStreamPlanner(false);
        }

        Map<Integer, IMTCreateInfo> createInfo = IMTAnalyzer.analyze(execPlan.getPhysicalPlan(), execPlan);
        if (createInfo.size() > 1) {
            throw new UnsupportedOperationException("not support more than one IMT right now");
        }
        return createInfo;
    }

    public TIMTType getImtType() {
        return imtType;
    }

    public String getTableName() {
        return tableName;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public KeysType getKeyType() {
        return keyType;
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public DistributionInfo getDistributionInfo() {
        return distributionInfo;
    }

    public Table.TableType getTableType() {
        return tableType;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    /**
     * Analyze IMT usage of each StreamOperator
     */
    static class IMTAnalyzer extends OptExpressionVisitor<Void, ExecPlan> {

        private Map<Integer, IMTCreateInfo> createInfoMap = new HashMap<>();
        private int exprSeq = 0;

        public IMTAnalyzer() {
        }

        public static Map<Integer, IMTCreateInfo> analyze(OptExpression optExpr, ExecPlan ctx) {
            IMTAnalyzer analyzer = new IMTAnalyzer();
            optExpr.getOp().accept(analyzer, optExpr, ctx);
            return analyzer.createInfoMap;
        }

        @Override
        public Void visit(OptExpression node, ExecPlan context) {
            // throw new UnsupportedOperationException("Not supported operator: " + node);
            return null;
        }

        @Override
        public Void visitPhysicalStreamJoin(OptExpression node, ExecPlan context) {
            // StreamJoin do not need any IMT/state
            return null;
        }

        @Override
        public Void visitPhysicalProject(OptExpression optExpr, ExecPlan context) {
            return null;
        }

        @Override
        public Void visitPhysicalFilter(OptExpression optExpr, ExecPlan context) {
            return null;
        }

        @Override
        public Void visitPhysicalStreamAgg(OptExpression optExpr, ExecPlan context) {
            visit(optExpr.inputAt(0), context);
            exprSeq++;

            PhysicalStreamAggOperator node = (PhysicalStreamAggOperator) optExpr.getOp();
            // Distribution Info
            List<ColumnRefOperator> groupByKeys = node.getGroupBys();
            if (CollectionUtils.isEmpty(groupByKeys)) {
                throw new UnsupportedOperationException("must have group by expression");
            }
            List<Column> distributeColumns = new ArrayList<>();
            for (ColumnRefOperator columnRef : groupByKeys) {
                SlotDescriptor slot = context.getDescTbl().getSlotDesc(new SlotId(columnRef.getId()));
                Column column = new Column(columnRef.getName(), slot.getType(), slot.getIsNullable());
                column.setIsKey(true);
                distributeColumns.add(column);
            }
            // TODO: bucket num
            DistributionInfo distributionInfo = new HashDistributionInfo(1, distributeColumns);

            // Table columns
            List<Column> columns = new ArrayList<>(distributeColumns);
            Map<ColumnRefOperator, CallOperator> aggregations = node.getAggregations();
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregations.entrySet()) {
                SlotDescriptor slot = context.getDescTbl().getSlotDesc(new SlotId(entry.getKey().getId()));
                CallOperator aggFunc = entry.getValue();
                String funcName = aggFunc.getFunction().functionName();
                boolean supported = funcName.equalsIgnoreCase("SUM") || funcName.equalsIgnoreCase("COUNT");
                if (!supported) {
                    throw new UnsupportedOperationException("agg function not supported in MV: " + funcName);
                }
                String columnName = funcName;
                for (ScalarOperator arg : aggFunc.getChildren()) {
                    ColumnRefOperator columnRef = (ColumnRefOperator) arg;
                    columnName += "_" + columnRef.getName();
                }
                Column column = new Column(columnName, slot.getType(), slot.getIsNullable());
                column.setIsKey(false);
                columns.add(column);
            }

            // PartitionInfo
            PartitionInfo partitionInfo = new PartitionInfo(PartitionType.UNPARTITIONED);

            // Table name
            String tableName = "agg_" + distributeColumns.stream().map(Column::getName).collect(Collectors.joining("_"));

            IMTCreateInfo res = new IMTCreateInfo(tableName, columns, partitionInfo, distributionInfo);
            res.setComment(node.toString());

            createInfoMap.put(exprSeq, res);

            return null;
        }
    }

    /**
     * Assign IMT to each StreamOperator
     */
    public static class IMTAssigner extends OptExpressionVisitor<Void, ExecPlan> {

        private final Map<Integer, IMTInfo> imtInfo;
        private int exprSeq = 0;

        private IMTAssigner(Map<Integer, IMTInfo> imtInfo) {
            this.imtInfo = imtInfo;
        }

        public static void assign(OptExpression optExpr, Map<Integer, IMTInfo> imtInfo) {
            IMTAssigner assigner = new IMTAssigner(imtInfo);
            optExpr.getOp().accept(assigner, optExpr, null);
        }

        @Override
        public Void visit(OptExpression optExpr, ExecPlan context) {
            return null;
        }

        @Override
        public Void visitPhysicalStreamJoin(OptExpression node, ExecPlan context) {
            // StreamJoin do not need any IMT/state
            return null;
        }

        @Override
        public Void visitPhysicalProject(OptExpression optExpr, ExecPlan context) {
            return null;
        }

        @Override
        public Void visitPhysicalFilter(OptExpression optExpr, ExecPlan context) {
            return null;
        }

        @Override
        public Void visitPhysicalStreamAgg(OptExpression optExpr, ExecPlan plan) {
            visit(optExpr.inputAt(0), plan);
            exprSeq++;

            PhysicalStreamAggOperator node = (PhysicalStreamAggOperator) optExpr.getOp();
            IMTInfo imt = imtInfo.get(exprSeq);
            Preconditions.checkState(imt != null, "Must have an imt here");

            node.setAggImt(imt);
            imt.setNeedMaintain(true);
            return null;
        }

    }

}
