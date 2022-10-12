// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.DeleteStmt;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.load.Load;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DeleteAnalyzer {
    private static final Logger LOG = LogManager.getLogger(DeleteAnalyzer.class);

    public static void analyze(DeleteStmt deleteStatement, ConnectContext session) {
        TableName tableName = deleteStatement.getTableName();
        MetaUtils.normalizationTableName(session, tableName);
        MetaUtils.getDatabase(session, tableName);
        Table table = MetaUtils.getTable(session, tableName);

        if (table instanceof MaterializedView) {
            throw new SemanticException(
                    "The data of '%s' cannot be deleted because '%s' is a materialized view," +
                            "and the data of materialized view must be consistent with the base table.",
                    tableName.getTbl(), tableName.getTbl());
        }

        if (!(table instanceof OlapTable && ((OlapTable) table).getKeysType() == KeysType.PRIMARY_KEYS)) {
            try {
                deleteStatement.analyze(new Analyzer(session.getGlobalStateMgr(), session));
            } catch (Exception e) {
                LOG.warn("Analyze DeleteStmt using old analyzer failed", e);
                throw new SemanticException("Analyze DeleteStmt using old analyzer failed: " + e.getMessage());
            }
            return;
        }

        deleteStatement.setTable(table);
        if (deleteStatement.getWherePredicate() == null) {
            throw new SemanticException("Delete must specify where clause to prevent full table delete");
        }
        if (deleteStatement.getPartitionNames() != null && deleteStatement.getPartitionNames().size() > 0) {
            throw new SemanticException("Delete for primary key table do not support specifying partitions");
        }

        SelectList selectList = new SelectList();
        for (Column col : table.getBaseSchema()) {
            SelectListItem item;
            if (col.isKey()) {
                item = new SelectListItem(new SlotRef(tableName, col.getName()), col.getName());
            } else {
                break;
            }
            selectList.addItem(item);
        }
        try {
            selectList.addItem(new SelectListItem(new IntLiteral(1, Type.TINYINT), Load.LOAD_OP_COLUMN));
        } catch (Exception e) {
            throw new SemanticException("analyze delete failed", e);
        }

        TableRelation tableRelation = new TableRelation(tableName);
        SelectRelation selectRelation =
                new SelectRelation(selectList, tableRelation, deleteStatement.getWherePredicate(), null, null);
        QueryStatement queryStatement = new QueryStatement(selectRelation);
        queryStatement.setIsExplain(deleteStatement.isExplain(), deleteStatement.getExplainLevel());
        new QueryAnalyzer(session).analyze(queryStatement);
        deleteStatement.setQueryStatement(queryStatement);
    }
}
