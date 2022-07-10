// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.AlterClause;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.CreateIndexClause;
import com.starrocks.analysis.IndexDef;
import com.starrocks.analysis.ModifyTablePropertiesClause;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRenameClause;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.common.MetaUtils;

import java.util.List;
import java.util.Map;

public class AlterTableStatementAnalyzer {
    public static void analyze(AlterTableStmt statement, ConnectContext context) {
        TableName tbl = statement.getTbl();
        MetaUtils.normalizationTableName(context, tbl);
        Table table = MetaUtils.getTable(context, tbl);
        if (table instanceof MaterializedView) {
            throw new SemanticException(
                    "The '%s' cannot be alter by 'ALTER TABLE', because '%s' is a materialized view," +
                            "you can use 'ALTER MATERIALIZED VIEW' to alter it.",
                    tbl.getTbl(), tbl.getTbl());
        }
        try {
            CatalogUtils.checkIsLakeTable(tbl.getDb(), tbl.getTbl());
        } catch (AnalysisException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_NO_TABLES_USED);
        }
        List<AlterClause> alterClauseList = statement.getOps();
        if (alterClauseList == null || alterClauseList.isEmpty()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_NO_ALTER_OPERATION);
        }
        AlterTableClauseAnalyzerVisitor alterTableClauseAnalyzerVisitor = new AlterTableClauseAnalyzerVisitor();
        for (AlterClause alterClause : alterClauseList) {
            alterTableClauseAnalyzerVisitor.analyze(alterClause, context);
        }
    }

    static class AlterTableClauseAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(AlterClause statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreateIndexClause(CreateIndexClause clause, ConnectContext context) {
            IndexDef indexDef = clause.getIndexDef();
            indexDef.analyze();
            clause.setIndex(new Index(indexDef.getIndexName(), indexDef.getColumns(),
                    indexDef.getIndexType(), indexDef.getComment()));
            return null;
        }

        @Override
        public Void visitTableRenameClause(TableRenameClause clause, ConnectContext context) {
            String newTableName = clause.getNewTableName();
            if (Strings.isNullOrEmpty(newTableName)) {
                throw new SemanticException("New Table name is not set");
            }
            try {
                FeNameFormat.checkTableName(newTableName);
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_TABLE_NAME, newTableName);
            }
            return null;
        }

        @Override
        public Void visitModifyTablePropertiesClause(ModifyTablePropertiesClause clause, ConnectContext context) {
            Map<String, String> properties = clause.getProperties();
            if (properties == null || properties.isEmpty()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Properties is not set");
            }

            boolean needTableStable = true;
            AlterOpType opType = clause.getOpType();
            if (properties.size() != 1
                    && !TableProperty.isSamePrefixProperties(properties, TableProperty.DYNAMIC_PARTITION_PROPERTY_PREFIX)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Can only set one table property at a time");
            }

            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)) {
                needTableStable = false;
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE)) {
                if (!properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE).equalsIgnoreCase("column")) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Can only change storage type to COLUMN");
                }
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_TYPE)) {
                if (!properties.get(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_TYPE).equalsIgnoreCase("hash")) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Can only change distribution type to HASH");
                }
                needTableStable = false;
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK)) {
                if (!properties.get(PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK).equalsIgnoreCase("true")) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Property " + PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK + " should be set to true");
                }
                needTableStable = false;
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BF_COLUMNS)
                    || properties.containsKey(PropertyAnalyzer.PROPERTIES_BF_FPP)) {
                // do nothing, these 2 properties will be analyzed when creating alter job
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT)) {
                if (!properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT).equalsIgnoreCase("v2")) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Property " + PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT + " should be v2");
                }
            } else if (DynamicPartitionUtil.checkDynamicPartitionPropertiesExist(properties)) {
                // do nothing, dynamic properties will be analyzed in SchemaChangeHandler.process
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
                try {
                    PropertyAnalyzer.analyzeReplicationNum(properties, false);
                } catch (AnalysisException e) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
                }
            } else if (properties.containsKey("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
                short defaultReplicationNum = 0;
                try {
                    defaultReplicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, true);
                } catch (AnalysisException e) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
                }
                properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, Short.toString(defaultReplicationNum));
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
                needTableStable = false;
                opType = AlterOpType.MODIFY_TABLE_PROPERTY_SYNC;
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX)) {
                if (!properties.get(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX).equalsIgnoreCase("true") &&
                        !properties.get(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX).equalsIgnoreCase("false")) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Property " + PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX +
                                    " must be bool type(false/true)");
                }
                needTableStable = false;
                opType = AlterOpType.MODIFY_TABLE_PROPERTY_SYNC;
            } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TABLET_TYPE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Alter tablet type not supported");
            } else {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Unknown table property: " + properties.keySet());
            }
            clause.setOpType(opType);
            clause.setNeedTableStable(needTableStable);
            return null;
        }
    }
}
