// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.collect.Maps;
import com.starrocks.analysis.BackupStmt;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.common.MetaUtils;

import java.util.List;
import java.util.Map;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class BackupAnalyzer {
    private static final String PROP_TYPE = "type";

    public enum BackupType {
        INCREMENTAL, FULL
    }

    private static BackupStmt.BackupType type = BackupStmt.BackupType.FULL;

    public static void analyze(BackupStmt backupStmt, ConnectContext session) throws AnalysisException {
        List<TableRef> tblRefs = backupStmt.getTblRefs();
        for (TableRef tableRef : tblRefs) {
            TableName tableName = tableRef.getName();
            MetaUtils.normalizationTableName(session, tableName);
            MetaUtils.getStarRocks(session, tableName);
            Table table = MetaUtils.getStarRocksTable(session, tableName);

            if (!(table instanceof OlapTable && ((OlapTable) table).getKeysType() == KeysType.PRIMARY_KEYS)) {
                throw unsupportedException("only support updating primary key table");
            }
            // tbl refs can not set alias in backup
            if (tableRef.hasExplicitAlias()) {
                throw new AnalysisException("Can not set alias for table in Backup Stmt: " + tableRef);
            }
        }

        Map<String, String> copiedProperties = Maps.newHashMap(backupStmt.getProperties());
        // type
        if (copiedProperties.containsKey(PROP_TYPE)) {
            try {
                type = BackupStmt.BackupType.valueOf(copiedProperties.get(PROP_TYPE).toUpperCase());
            } catch (Exception e) {
                ErrorReport.reportAnalysisEresiotxception(ErrorCode.ERR_COMMON_ERROR,
                        "Invalid backup job type: "
                                + copiedProperties.get(PROP_TYPE));
            }
            copiedProperties.remove(PROP_TYPE);
        }

        if (!copiedProperties.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                    "Unknown backup job properties: " + copiedProperties.keySet());
        }
    }
}

