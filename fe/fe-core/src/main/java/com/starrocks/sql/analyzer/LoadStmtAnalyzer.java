// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.DataDescription;
import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.LoadStmt;
import com.starrocks.analysis.ResourceDesc;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.load.EtlJobType;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;

public class LoadStmtAnalyzer {

    private LoadStmtAnalyzer(){}

    public static void analyze(LoadStmt statement, ConnectContext context) {
        new LoadStmtAnalyzerVisitor().analyze(statement, context);
    }

    static class LoadStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        private static final String VERSION = "version";

        public void analyze(LoadStmt statement, ConnectContext context) {
            visit(statement, context);
        }

        @Override
        public Void visitLoadStmt(LoadStmt statement, ConnectContext context) {
            analyzeLabel(statement, context);
            analyzeDataDescriptions(statement);
            analyzeProperties(statement);
            return null;
        }

        private void analyzeLabel(LoadStmt statement, ConnectContext context) {
            LabelName label = statement.getLabel();
            String dbName = label.getDbName();
            if (Strings.isNullOrEmpty(dbName)) {
                dbName = context.getDatabase();
                if (Strings.isNullOrEmpty(dbName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                }
            }
            label.setDbName(dbName);
            try {
                FeNameFormat.checkLabel(label.getLabelName());
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
            }
        }

        private void analyzeDataDescriptions(LoadStmt statement) {
            List<DataDescription> dataDescriptions = statement.getDataDescriptions();
            BrokerDesc brokerDesc = statement.getBrokerDesc();
            ResourceDesc resourceDesc = statement.getResourceDesc();
            LabelName label = statement.getLabel();
            if (CollectionUtils.isEmpty(dataDescriptions)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "No data file in load statement.");
            }
            try {
                boolean isLoadFromTable = false;
                for (DataDescription dataDescription : dataDescriptions) {
                    if (brokerDesc == null && resourceDesc == null) {
                        dataDescription.setIsHadoopLoad(true);
                    }
                    dataDescription.analyze(label.getDbName());

                    if (dataDescription.isLoadFromTable()) {
                        isLoadFromTable = true;
                    }
                }
                if (isLoadFromTable) {
                    if (dataDescriptions.size() > 1) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                                "Only support one olap table load from one external table");
                    }
                    if (resourceDesc == null) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                                "Load from table should use Spark Load");
                    }
                }

                EtlJobType etlJobType;
                if (resourceDesc != null) {
                    resourceDesc.analyze();
                    etlJobType = resourceDesc.getEtlJobType();
                    // check resource usage privilege
                    if (!GlobalStateMgr.getCurrentState().getAuth().checkResourcePriv(ConnectContext.get(),
                            resourceDesc.getName(),
                            PrivPredicate.USAGE)) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                                "USAGE denied to user '" + ConnectContext.get().getQualifiedUser()
                                + "'@'" + ConnectContext.get().getRemoteIP()
                                + "' for resource '" + resourceDesc.getName() + "'");
                    }
                } else if (brokerDesc != null) {
                    etlJobType = EtlJobType.BROKER;
                } else {
                    // if cluster is null, use default hadoop cluster
                    // if cluster is not null, use this hadoop cluster
                    etlJobType = EtlJobType.HADOOP;
                }

                if (etlJobType == EtlJobType.SPARK) {
                    for (DataDescription dataDescription : dataDescriptions) {
                        CatalogUtils.checkIsLakeTable(label.getDbName(), dataDescription.getTableName());
                    }
                }
                statement.setEtlJobType(etlJobType);
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
            }
        }

        private void analyzeProperties(LoadStmt statement) {
            Map<String, String> properties = statement.getProperties();
            try {
                LoadStmt.checkProperties(properties);
            } catch (DdlException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
            }

            if (properties == null) {
                return;
            }
            final String versionProperty = properties.get(VERSION);
            if (versionProperty != null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Do not support VERSION property");
            }
            statement.setUser(ConnectContext.get().getQualifiedUser());
        }
    }
}