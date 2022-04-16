// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.BackupStmt;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.common.MetaUtils;

import java.util.List;


import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class BackupAnalyzer {
    public static void analyze(BackupStmt backupStmt, ConnectContext session) {
        List<TableRef> tblRefs=backupStmt.getTblRefs();
        for(TableRef tableRef:tblRefs){
            TableName tableName=tableRef.getName();
            MetaUtils.normalizationTableName(session, tableName);
            MetaUtils.getStarRocks(session, tableName);
            Table table = MetaUtils.getStarRocksTable(session, tableName);

            if (!(table instanceof OlapTable && ((OlapTable) table).getKeysType() == KeysType.PRIMARY_KEYS)) {
                throw unsupportedException("only support updating primary key table");
            }
        }
    }
}

