// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.RestoreStmt;
import com.starrocks.analysis.TableRef;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.common.MetaUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class RestoreAnalyzer {
    private static final String PROP_ALLOW_LOAD = "allow_load";
    private static final String PROP_REPLICATION_NUM = "replication_num";
    private static final String PROP_BACKUP_TIMESTAMP = "backup_timestamp";
    private static final String PROP_META_VERSION = "meta_version";
    private static final String PROP_STARROCKS_META_VERSION = "starrocks_meta_version";

    private static boolean allowLoad = false;
    private static int replicationNum = FeConstants.default_replication_num;
    private static String backupTimestamp = null;
    private static int metaVersion = -1;
    private static int starrocksMetaVersion = -1;

    public static void analyze(RestoreStmt restoreStmt, ConnectContext session) {
        new RestoreStmtAnalyzerVisitor().visit(restoreStmt, session);
    }

    static class RestoreStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(RestoreStmt stmt, ConnectContext session) {
            visit(stmt, session);
        }

        @Override
        public Void visitRestoreStatement(RestoreStmt restoreStmt, ConnectContext session) {
            String db = restoreStmt.getDbName();
            restoreStmt.setDb(MetaUtils.getFullDatabaseName(db, session));
            restoreStmt.setClusterName(session.getClusterName());
            List<TableRef> tblRefs = restoreStmt.getTableRefs();
            // check if alias is duplicated
            Set<String> aliasSet = Sets.newHashSet();
            for (TableRef tblRef : tblRefs) {
                aliasSet.add(tblRef.getName().getTbl());
            }

            for (TableRef tblRef : tblRefs) {
                if (tblRef.hasExplicitAlias() && !aliasSet.add(tblRef.getExplicitAlias())) {
                    throw new SemanticException("Duplicated alias name: " + tblRef.getExplicitAlias());
                }
            }

            Map<String, String> copiedProperties = Maps.newHashMap(restoreStmt.getProperties());
            // allow load
            if (copiedProperties.containsKey(PROP_ALLOW_LOAD)) {
                if (copiedProperties.get(PROP_ALLOW_LOAD).equalsIgnoreCase("true")) {
                    allowLoad = true;
                } else if (copiedProperties.get(PROP_ALLOW_LOAD).equalsIgnoreCase("false")) {
                    allowLoad = false;
                } else {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Invalid allow load value: "
                                    + copiedProperties.get(PROP_ALLOW_LOAD));
                }
                copiedProperties.remove(PROP_ALLOW_LOAD);
            }

            // replication num
            if (copiedProperties.containsKey(PROP_REPLICATION_NUM)) {
                try {
                    replicationNum = Integer.parseInt(copiedProperties.get(PROP_REPLICATION_NUM));
                } catch (NumberFormatException e) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Invalid replication num format: "
                                    + copiedProperties.get(PROP_REPLICATION_NUM));
                }
                copiedProperties.remove(PROP_REPLICATION_NUM);
            }

            // backup timestamp
            if (copiedProperties.containsKey(PROP_BACKUP_TIMESTAMP)) {
                backupTimestamp = copiedProperties.get(PROP_BACKUP_TIMESTAMP);
                copiedProperties.remove(PROP_BACKUP_TIMESTAMP);
            } else {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Missing " + PROP_BACKUP_TIMESTAMP + " property");
            }

            // meta version
            if (copiedProperties.containsKey(PROP_META_VERSION)) {
                try {
                    metaVersion = Integer.parseInt(copiedProperties.get(PROP_META_VERSION));
                } catch (NumberFormatException e) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Invalid meta version format: "
                                    + copiedProperties.get(PROP_META_VERSION));
                }
                copiedProperties.remove(PROP_META_VERSION);
            }
            if (copiedProperties.containsKey(PROP_STARROCKS_META_VERSION)) {
                try {
                    starrocksMetaVersion = Integer.parseInt(copiedProperties.get(PROP_STARROCKS_META_VERSION));
                } catch (NumberFormatException e) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Invalid meta version format: "
                                    + copiedProperties.get(PROP_STARROCKS_META_VERSION));
                }
                copiedProperties.remove(PROP_STARROCKS_META_VERSION);
            }

            if (!copiedProperties.isEmpty()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Unknown restore job properties: " + copiedProperties.keySet());
            }
            return null;
        }
    }
}

