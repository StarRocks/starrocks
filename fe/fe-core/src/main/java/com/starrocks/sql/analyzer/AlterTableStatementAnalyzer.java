// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.starrocks.analysis.AlterClause;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.CreateIndexClause;
import com.starrocks.analysis.IndexDef;
import com.starrocks.analysis.ModifyPartitionClause;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRenameClause;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.common.MetaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            CatalogUtils.checkOlapTableHasStarOSPartition(tbl.getDb(), tbl.getTbl());
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

        private final static Logger logger = LoggerFactory.getLogger(AlterTableClauseAnalyzerVisitor.class);

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
        public Void visitModifyPartitionClause(ModifyPartitionClause clause, ConnectContext context) {
            final List<String> partitionNames = clause.getPartitionNames();
            final boolean needExpand = clause.isNeedExpand();
            final Map<String, String> properties = clause.getProperties();
            if (partitionNames == null || (!needExpand && partitionNames.isEmpty())) {
                throw new SemanticException("Partition names is not set or empty");
            }

            if (partitionNames.stream().anyMatch(entity -> Strings.isNullOrEmpty(entity))) {
                throw new SemanticException("there are empty partition name");
            }

            if (properties == null || properties.isEmpty()) {
                throw new SemanticException("Properties is not set");
            }

            // check properties here
            try {
                checkProperties(Maps.newHashMap(properties));
            } catch (AnalysisException e) {
                logger.error("check properties error", e);
                throw new SemanticException(e.getMessage());
            }
            return null;
        }

        // Check the following properties' legality before modifying partition.
        // 1. replication_num
        // 2. storage_medium && storage_cooldown_time
        // 3. in_memory
        // 4. tablet type
        private void checkProperties(Map<String, String> properties) throws AnalysisException {
            // 1. data property
            DataProperty newDataProperty = null;
            newDataProperty = PropertyAnalyzer.analyzeDataProperty(properties, DataProperty.DEFAULT_DATA_PROPERTY);
            Preconditions.checkNotNull(newDataProperty);

            // 2. replication num
            short newReplicationNum = (short) -1;
            newReplicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, FeConstants.default_replication_num);
            Preconditions.checkState(newReplicationNum != (short) -1);

            // 3. in memory
            PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);

            // 4. tablet type
            PropertyAnalyzer.analyzeTabletType(properties);
        }
    }
}
