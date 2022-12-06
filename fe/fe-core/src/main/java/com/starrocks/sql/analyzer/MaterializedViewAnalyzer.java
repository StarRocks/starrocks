// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DistributionDesc;
import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.HashDistributionDesc;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SelectListItem;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterMaterializedViewStatement;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.IntervalLiteral;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshSchemeDesc;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.common.MetaUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MaterializedViewAnalyzer {

    public static void analyze(StatementBase stmt, ConnectContext session) {
        new MaterializedViewAnalyzerVisitor().visit(stmt, session);
    }

    static class MaterializedViewAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        public enum RefreshTimeUnit {
            DAY,
            HOUR,
            MINUTE,
            SECOND
        }

        @Override
        public Void visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement,
                                                         ConnectContext context) {
            final TableName tableNameObject = statement.getTableName();
            MetaUtils.normalizationTableName(context, tableNameObject);
            final String tableName = tableNameObject.getTbl();
            try {
                FeNameFormat.checkTableName(tableName);
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_TABLE_NAME, tableName);
            }
            QueryStatement queryStatement = statement.getQueryStatement();
            // check query relation is select relation
            if (!(queryStatement.getQueryRelation() instanceof SelectRelation)) {
                throw new SemanticException("Materialized view query statement only support select");
            }
            SelectRelation selectRelation = ((SelectRelation) queryStatement.getQueryRelation());
            // check cte
            if (selectRelation.hasWithClause()) {
                throw new SemanticException("Materialized view query statement not support cte");
            }
            // check subquery
            if (!AnalyzerUtils.collectAllSubQueryRelation(queryStatement).isEmpty()) {
                throw new SemanticException("Materialized view query statement not support subquery");
            }
            // check alias except * and SlotRef
            List<SelectListItem> selectListItems = selectRelation.getSelectList().getItems();
            for (SelectListItem selectListItem : selectListItems) {
                if (selectListItem.isStar()) {
                    throw new SemanticException("Select * is not supported in materialized view");
                } else if (!(selectListItem.getExpr() instanceof SlotRef)
                        && selectListItem.getAlias() == null) {
                    throw new SemanticException("Materialized view query statement select item " +
                            selectListItem.getExpr().toSql() + " must has an alias");
                }
                // check select item has nondeterministic function
                checkNondeterministicFunction(selectListItem.getExpr());
            }
            // analyze query statement, can check whether tables and columns exist in catalog
            Analyzer.analyze(queryStatement, context);
            // convert queryStatement to sql and set
            statement.setInlineViewDef(ViewDefBuilder.build(queryStatement));
            // collect table from query statement
            Map<TableName, Table> tableNameTableMap = AnalyzerUtils.collectAllTableAndViewWithAlias(queryStatement);
            Set<Long> baseTableIds = Sets.newHashSet();
            Database db = context.getGlobalStateMgr().getDb(statement.getTableName().getDb());
            if (db == null) {
                throw new SemanticException("Can not find database:" + statement.getTableName().getDb());
            }
            if (tableNameTableMap.isEmpty()) {
                throw new SemanticException("Can not find base table in query statement");
            }
            tableNameTableMap.values().forEach(table -> {
                if (db.getTable(table.getId()) == null) {
                    throw new SemanticException(
                            "Materialized view do not support table: " + table.getName() +
                                    " do not exist in database: " + db.getOriginName());
                }
                if (!(table instanceof OlapTable)) {
                    throw new SemanticException(
                            "Materialized view only supports olap table, but the type of table: " +
                                    table.getName() + " is: " + table.getType().name());
                }
                if (table instanceof MaterializedView) {
                    throw new SemanticException(
                            "Creating a materialized view from materialized view is not supported now. The type of table: " +
                                    table.getName() + " is: Materialized View");
                }
                baseTableIds.add(table.getId());
            });
            statement.setBaseTableIds(baseTableIds);
            Map<Column, Expr> columnExprMap = Maps.newHashMap();
            // get outputExpressions and convert it to columns which in selectRelation
            // set the columns into createMaterializedViewStatement
            // record the relationship between columns and outputExpressions for next check
            genColumnAndSetIntoStmt(statement, selectRelation, columnExprMap);
            // some check if partition exp exists
            if (statement.getPartitionExpDesc() != null) {
                // check partition expression all in column list and
                // write the expr into partitionExpDesc if partition expression exists
                checkExpInColumn(statement, columnExprMap);
                // check whether partition expression functions are allowed if it exists
                checkPartitionExpParams(statement);
                // check partition column must be base table's partition column
                checkPartitionColumnWithBaseTable(statement, tableNameTableMap);
            }
            // check and analyze distribution
            checkDistribution(statement, tableNameTableMap);
            return null;
        }

        private void checkNondeterministicFunction(Expr expr) {
            if (expr instanceof FunctionCallExpr) {
                if (((FunctionCallExpr) expr).isNondeterministicBuiltinFnName()) {
                    throw new SemanticException("Materialized view query statement select item " +
                            expr.toSql() + " not supported nondeterministic function");
                }
            }
            ArrayList<Expr> children = expr.getChildren();
            for (Expr child : children) {
                checkNondeterministicFunction(child);
            }
        }

        private void genColumnAndSetIntoStmt(CreateMaterializedViewStatement statement, QueryRelation queryRelation,
                                             Map<Column, Expr> columnExprMap) {
            List<Column> mvColumns = Lists.newArrayList();
            List<String> columnOutputNames = queryRelation.getColumnOutputNames();
            List<Expr> outputExpression = queryRelation.getOutputExpression();
            for (int i = 0; i < outputExpression.size(); ++i) {
                Type type = AnalyzerUtils.transformTypeForMv(outputExpression.get(i).getType());
                Column column = new Column(columnOutputNames.get(i), type,
                        outputExpression.get(i).isNullable());
                // set default aggregate type, look comments in class Column
                column.setAggregationType(AggregateType.NONE, false);
                mvColumns.add(column);
                columnExprMap.put(column, outputExpression.get(i));
            }
            // set duplicate key
            int theBeginIndexOfValue = 0;
            int keySizeByte = 0;
            for (; theBeginIndexOfValue < mvColumns.size(); theBeginIndexOfValue++) {
                Column column = mvColumns.get(theBeginIndexOfValue);
                keySizeByte += column.getType().getIndexSize();
                if (theBeginIndexOfValue + 1 > FeConstants.shortkey_max_column_count ||
                        keySizeByte > FeConstants.shortkey_maxsize_bytes) {
                    if (theBeginIndexOfValue == 0 && column.getType().getPrimitiveType().isCharFamily()) {
                        column.setIsKey(true);
                        column.setAggregationType(null, false);
                        theBeginIndexOfValue++;
                    }
                    break;
                }
                if (!column.getType().canBeMVKey()) {
                    break;
                }
                if (column.getType().getPrimitiveType() == PrimitiveType.VARCHAR) {
                    column.setIsKey(true);
                    column.setAggregationType(null, false);
                    theBeginIndexOfValue++;
                    break;
                }
                column.setIsKey(true);
                column.setAggregationType(null, false);
            }
            if (theBeginIndexOfValue == 0) {
                throw new SemanticException("Data type of first column cannot be " + mvColumns.get(0).getType());
            }
            statement.setMvColumnItems(mvColumns);
        }

        private void checkExpInColumn(CreateMaterializedViewStatement statement,
                                      Map<Column, Expr> columnExprMap) {
            ExpressionPartitionDesc expressionPartitionDesc = statement.getPartitionExpDesc();
            List<Column> columns = statement.getMvColumnItems();
            SlotRef slotRef = getSlotRef(expressionPartitionDesc.getExpr());
            if (slotRef.getTblNameWithoutAnalyzed() != null) {
                throw new SemanticException("Materialized view partition exp: "
                        + slotRef.toSql() + " must related to column");
            }
            int columnId = 0;
            for (Column column : columns) {
                if (slotRef.getColumnName().equalsIgnoreCase(column.getName())) {
                    statement.setPartitionColumn(column);
                    SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(columnId), slotRef.getColumnName(),
                            column.getType(), column.isAllowNull());
                    slotRef.setDesc(slotDescriptor);
                    break;
                }
                columnId++;
            }
            if (statement.getPartitionColumn() == null) {
                throw new SemanticException("Materialized view partition exp column:"
                        + slotRef.getColumnName() + " is not found in query statement");
            }
            checkExpWithRefExpr(statement, columnExprMap);
        }

        private void checkExpWithRefExpr(CreateMaterializedViewStatement statement,
                                         Map<Column, Expr> columnExprMap) {
            ExpressionPartitionDesc expressionPartitionDesc = statement.getPartitionExpDesc();
            Column partitionColumn = statement.getPartitionColumn();
            Expr refExpr = columnExprMap.get(partitionColumn);
            if (expressionPartitionDesc.isFunction()) {
                // e.g. partition by date_trunc('month', ss)
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) expressionPartitionDesc.getExpr();
                if (!(refExpr instanceof SlotRef)) {
                    throw new SemanticException("Materialized view partition function " +
                            functionCallExpr.getFnName().getFunction() +
                            " must related with column");
                }
                SlotRef slotRef = getSlotRef(functionCallExpr);
                slotRef.setType(partitionColumn.getType());
                // copy function and set it into partitionRefTableExpr
                Expr partitionRefTableExpr = functionCallExpr.clone();
                List<Expr> children = partitionRefTableExpr.getChildren();
                for (int i = 0; i < children.size(); i++) {
                    if (children.get(i) instanceof SlotRef) {
                        partitionRefTableExpr.setChild(i, refExpr);
                    }
                }
                statement.setPartitionRefTableExpr(partitionRefTableExpr);
            } else {
                // e.g. partition by date_trunc('day',ss) or partition by ss
                if (refExpr instanceof FunctionCallExpr || refExpr instanceof SlotRef) {
                    statement.setPartitionRefTableExpr(refExpr);
                } else {
                    throw new SemanticException(
                            "Materialized view partition function must related with column");
                }
            }
        }

        private void checkPartitionExpParams(CreateMaterializedViewStatement statement) {
            Expr expr = statement.getPartitionRefTableExpr();
            if (expr instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = ((FunctionCallExpr) expr);
                String functionName = functionCallExpr.getFnName().getFunction();
                CheckPartitionFunction checkPartitionFunction =
                        PartitionFunctionChecker.FN_NAME_TO_PATTERN.get(functionName);
                if (checkPartitionFunction == null) {
                    throw new SemanticException("Materialized view partition function " +
                            functionName + " is not support");
                }
                if (!checkPartitionFunction.check(functionCallExpr)) {
                    throw new SemanticException("Materialized view partition function " +
                            functionName + " check failed");
                }
            }
        }

        private void checkPartitionColumnWithBaseTable(CreateMaterializedViewStatement statement,
                                                       Map<TableName, Table> tableNameTableMap) {
            SlotRef slotRef = getSlotRef(statement.getPartitionRefTableExpr());
            Table table = tableNameTableMap.get(slotRef.getTblNameWithoutAnalyzed());
            PartitionInfo partitionInfo = ((OlapTable) table).getPartitionInfo();
            if (partitionInfo instanceof SinglePartitionInfo) {
                throw new SemanticException("Materialized view partition column in partition exp " +
                        "must be base table partition column");
            } else if (partitionInfo instanceof RangePartitionInfo) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
                if (partitionColumns.size() > 1) {
                    throw new SemanticException("Materialized view related base table partition columns " +
                            "only supports single column");
                }
                boolean isInPartitionColumns = false;
                for (Column basePartitionColumn : partitionColumns) {
                    if (basePartitionColumn.getName().equalsIgnoreCase(slotRef.getColumnName())) {
                        isInPartitionColumns = true;
                        break;
                    }
                }
                if (!isInPartitionColumns) {
                    throw new SemanticException("Materialized view partition column in partition exp " +
                            "must be base table partition column");
                }
            } else {
                throw new SemanticException("Materialized view related base table partition type:" +
                        partitionInfo.getType().name() + "not supports");
            }
            replaceTableAlias(slotRef, statement, tableNameTableMap);
        }

        private SlotRef getSlotRef(Expr expr) {
            if (expr instanceof SlotRef) {
                return ((SlotRef) expr);
            } else {
                List<SlotRef> slotRefs = Lists.newArrayList();
                expr.collect(SlotRef.class, slotRefs);
                Preconditions.checkState(slotRefs.size() == 1);
                return slotRefs.get(0);
            }
        }

        private void replaceTableAlias(SlotRef slotRef,
                                       CreateMaterializedViewStatement statement,
                                       Map<TableName, Table> tableNameTableMap) {
            TableName tableName = slotRef.getTblNameWithoutAnalyzed();
            OlapTable table = ((OlapTable) tableNameTableMap.get(tableName));
            slotRef.setTblName(new TableName(null, statement.getTableName().getDb(), table.getName()));
        }

        private void checkDistribution(CreateMaterializedViewStatement statement,
                                       Map<TableName, Table> tableNameTableMap) {
            DistributionDesc distributionDesc = statement.getDistributionDesc();
            List<Column> mvColumnItems = statement.getMvColumnItems();
            Map<String, String> properties = statement.getProperties();
            if (properties == null) {
                properties = Maps.newHashMap();
                statement.setProperties(properties);
            }
            if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
                properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM,
                        autoInferReplicationNum(tableNameTableMap).toString());
            }
            if (distributionDesc == null) {
                if (ConnectContext.get().getSessionVariable().isAllowDefaultPartition()) {
                    distributionDesc = new HashDistributionDesc(Config.default_bucket_num,
                            Lists.newArrayList(mvColumnItems.get(0).getName()));
                    statement.setDistributionDesc(distributionDesc);
                } else {
                    throw new SemanticException("Materialized view should contain distribution desc");
                }
            }
            Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            for (Column columnDef : mvColumnItems) {
                if (!columnSet.add(columnDef.getName())) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, columnDef.getName());
                }
            }
            distributionDesc.analyze(columnSet);
        }

        private Short autoInferReplicationNum(Map<TableName, Table> tableNameTableMap) {
            // For replication_num, we select the maximum value of all tables replication_num
            Short defaultReplicationNum = 1;
            for (Table table : tableNameTableMap.values()) {
                if (table instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) table;
                    Short replicationNum = olapTable.getDefaultReplicationNum();
                    if (replicationNum > defaultReplicationNum) {
                        defaultReplicationNum = replicationNum;
                    }
                }
            }
            return defaultReplicationNum;
        }

        @Override
        public Void visitDropMaterializedViewStatement(DropMaterializedViewStmt stmt, ConnectContext context) {
            stmt.getDbMvName().normalization(context);
            return null;
        }

        @Override
        public Void visitAlterMaterializedViewStatement(AlterMaterializedViewStatement statement,
                                                        ConnectContext context) {
            statement.getMvName().normalization(context);
            final RefreshSchemeDesc refreshSchemeDesc = statement.getRefreshSchemeDesc();
            final String newMvName = statement.getNewMvName();
            if (newMvName != null) {
                if (statement.getMvName().getTbl().equals(newMvName)) {
                    throw new SemanticException("Same materialized view name %s", newMvName);
                }
            } else if (refreshSchemeDesc != null) {
                if (refreshSchemeDesc.getType() == MaterializedView.RefreshType.SYNC) {
                    throw new SemanticException("Unsupported change to SYNC refresh type");
                }
                if (refreshSchemeDesc instanceof AsyncRefreshSchemeDesc) {
                    AsyncRefreshSchemeDesc async = (AsyncRefreshSchemeDesc) refreshSchemeDesc;
                    final IntervalLiteral intervalLiteral = async.getIntervalLiteral();
                    if (intervalLiteral != null) {
                        long step = ((IntLiteral) intervalLiteral.getValue()).getLongValue();
                        if (step <= 0) {
                            throw new SemanticException("Unsupported negative or zero step value: %s", step);
                        }
                        final String unit = intervalLiteral.getUnitIdentifier().getDescription().toUpperCase();
                        try {
                            RefreshTimeUnit.valueOf(unit);
                        } catch (IllegalArgumentException e) {
                            throw new SemanticException(
                                    "Unsupported interval unit: %s, only timeunit %s are supported.", unit,
                                    Arrays.asList(RefreshTimeUnit.values()));
                        }
                    }
                }
            } else {
                throw new SemanticException("Unsupported modification for materialized view");
            }
            return null;
        }

        @Override
        public Void visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement statement,
                                                          ConnectContext context) {
            statement.getMvName().normalization(context);
            TableName mvName = statement.getMvName();
            Database db = context.getGlobalStateMgr().getDb(mvName.getDb());
            if (db == null) {
                throw new SemanticException("Can not find database:" + mvName.getDb());
            }
            Table table = db.getTable(mvName.getTbl());
            if (table == null) {
                throw new SemanticException("Can not find materialized view:" + mvName.getTbl());
            }
            Preconditions.checkState(table instanceof MaterializedView);
            MaterializedView mv = (MaterializedView) table;
            if (!mv.isActive()) {
                throw new SemanticException(
                        "Refresh materialized view failed because " + mv.getName() + " is not active.");
            }
            return null;
        }

        @Override
        public Void visitCancelRefreshMaterializedViewStatement(CancelRefreshMaterializedViewStatement statement,
                                                                ConnectContext context) {
            statement.getMvName().normalization(context);
            return null;
        }
    }

    @FunctionalInterface
    public interface CheckPartitionFunction {

        boolean check(Expr expr);
    }

    static class PartitionFunctionChecker {

        public static final Map<String, CheckPartitionFunction> FN_NAME_TO_PATTERN;

        static {
            FN_NAME_TO_PATTERN = Maps.newHashMap();
            // can add some other functions
            FN_NAME_TO_PATTERN.put("date_trunc", PartitionFunctionChecker::checkDateTrunc);
        }

        public static boolean checkDateTrunc(Expr expr) {
            if (!(expr instanceof FunctionCallExpr)) {
                return false;
            }
            FunctionCallExpr fnExpr = (FunctionCallExpr) expr;
            String fnNameString = fnExpr.getFnName().getFunction();
            if (!fnNameString.equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
                return false;
            }
            if (fnExpr.getChild(0) instanceof StringLiteral && fnExpr.getChild(1) instanceof SlotRef) {
                String fmt = ((StringLiteral) fnExpr.getChild(0)).getValue();
                if (fmt.equalsIgnoreCase("week")) {
                    throw new SemanticException("The function date_trunc used by the materialized view for partition" +
                            " does not support week formatting");
                }
                SlotRef slotRef = (SlotRef) fnExpr.getChild(1);
                PrimitiveType primitiveType = slotRef.getType().getPrimitiveType();
                // must check slotRef type, because function analyze don't check it.
                return primitiveType == PrimitiveType.DATETIME || primitiveType == PrimitiveType.DATE;
            } else {
                return false;
            }
        }
    }
}
