// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.analysis.AlterClause;
import com.starrocks.analysis.TableRenameClause;
import com.starrocks.catalog.DataProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;

import java.util.Map;

public class AlterTableClauseAnalyzer {
    public static void analyze(AlterClause alterTableClause, ConnectContext session) {
        new AlterTableClauseAnalyzerVisitor().analyze(alterTableClause, session);
    }
    static class AlterTableClauseAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(AlterClause statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitTableRenameClause(TableRenameClause statement, ConnectContext context) {
            String newTableName = statement.getNewTableName();
            if (Strings.isNullOrEmpty(newTableName)) {
                throw new SemanticException("New Table name is not set");
            }
            try {
                FeNameFormat.checkTableName(newTableName);
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_TABLE_NAME);
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
