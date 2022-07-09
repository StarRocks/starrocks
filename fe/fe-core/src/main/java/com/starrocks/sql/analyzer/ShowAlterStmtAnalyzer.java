// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ShowAlterStmt;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Type;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcService;
import com.starrocks.common.proc.SchemaChangeProcDir;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ShowAlterStmtAnalyzer {

    private static  HashMap<String, Expr> filterMap = new HashMap<>();

    public static void handleShowAlterTable(ShowAlterStmt statement, ConnectContext context) {
        Database db = context.getGlobalStateMgr().getDb(statement.getDbName());
        if (db == null) {
            throw new SemanticException(ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg());
        }

        // build proc path
        ShowAlterStmt.AlterType type = statement.getType();
        StringBuilder sb = new StringBuilder();
        sb.append("/jobs/");
        sb.append(db.getId());
        if (type == ShowAlterStmt.AlterType.COLUMN) {
            sb.append("/schema_change");
        } else if (type == ShowAlterStmt.AlterType.ROLLUP || type == ShowAlterStmt.AlterType.MATERIALIZED_VIEW) {
            sb.append("/rollup");
        }

        // create show proc stmt
        // '/jobs/db_name/rollup|schema_change/
        ProcNodeInterface node = null;
        try {
            node = ProcService.getInstance().open(sb.toString());
        } catch (AnalysisException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_PROC_PATH, sb.toString());
        }
        statement.setNode(node);
    }

    public static void analyze(ShowAlterStmt statement, ConnectContext context) throws AnalysisException {
        String dbName = statement.getDbName();
        String catalog = context.getCurrentCatalog();
        if (CatalogMgr.isInternalCatalog(catalog)) {
            dbName = ClusterNamespace.getFullName(dbName);
        }
        statement.setDbName(dbName);
        ShowAlterStmt.AlterType type = statement.getType();

        Database db = context.getGlobalStateMgr().getDb(dbName);
        if (db == null) {
            throw new SemanticException(ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg());
        }
        Preconditions.checkNotNull(type);

        Expr whereClause = statement.getWhereClause();
        // analyze where clause if not null
        if (whereClause != null) {
            analyzeSubPredicate(whereClause);
        }
        statement.setFilter(filterMap);

        // order by
        List<OrderByElement> orderByElements = statement.getOrderByElements();
        ArrayList<OrderByPair> orderByPairs = new ArrayList<OrderByPair>();
        if (orderByElements != null && !orderByElements.isEmpty()) {
            for (OrderByElement orderByElement : orderByElements) {
                if (!(orderByElement.getExpr() instanceof SlotRef)) {
                    throw new AnalysisException("Should order by column");
                }
                SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                int index = SchemaChangeProcDir.analyzeColumn(slotRef.getColumnName());
                OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                orderByPairs.add(orderByPair);
            }
        }
        statement.setOrderByPairs(orderByPairs);
    }

    private static void getPredicateValue(Expr subExpr) throws AnalysisException {
        if (!(subExpr instanceof BinaryPredicate)) {
            throw new AnalysisException("The operator =|>=|<=|>|<|!= are supported.");
        }
        BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
        if (!(subExpr.getChild(0) instanceof SlotRef)) {
            throw new AnalysisException("Only support column = xxx syntax.");
        }
        String leftKey = ((SlotRef) subExpr.getChild(0)).getColumnName().toLowerCase();
        if (leftKey.equals("tablename") || leftKey.equals("state")) {
            if (!(subExpr.getChild(1) instanceof StringLiteral) ||
                    binaryPredicate.getOp() != BinaryPredicate.Operator.EQ) {
                throw new AnalysisException("Where clause : TableName = \"table1\" or "
                        + "State = \"FINISHED|CANCELLED|RUNNING|PENDING|WAITING_TXN\"");
            }
        } else if (leftKey.equals("createtime") || leftKey.equals("finishtime")) {
            if (!(subExpr.getChild(1) instanceof StringLiteral)) {
                throw new AnalysisException("Where clause : CreateTime/FinishTime =|>=|<=|>|<|!= "
                        + "\"2019-12-02|2019-12-02 14:54:00\"");
            }
            subExpr.setChild(1, ((StringLiteral) subExpr.getChild(1)).castTo(Type.DATETIME));
        } else {
            throw new AnalysisException("The columns of TableName/CreateTime/FinishTime/State are supported.");
        }
        filterMap.put(leftKey, subExpr);
    }

    private static void analyzeSubPredicate(Expr subExpr) throws AnalysisException {
        if (subExpr == null) {
            return;
        }
        if (subExpr instanceof CompoundPredicate) {
            CompoundPredicate cp = (CompoundPredicate) subExpr;
            if (cp.getOp() != com.starrocks.analysis.CompoundPredicate.Operator.AND) {
                throw new AnalysisException("Only allow compound predicate with operator AND");
            }
            analyzeSubPredicate(cp.getChild(0));
            analyzeSubPredicate(cp.getChild(1));
            return;
        }
        getPredicateValue(subExpr);
    }


}
