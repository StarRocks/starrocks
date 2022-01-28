// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/DropMaterializedViewStmt.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;

import java.util.List;

/**
 * DROP MATERIALIZED VIEW [ IF EXISTS ] <mv_name> IN|FROM [db_name].<table_name>
 * <p>
 * Parameters
 * IF EXISTS: Do not throw an error if the materialized view does not exist. A notice is issued in this case.
 * mv_name: The name of the materialized view to remove.
 * db_name: The name of db to which materialized view belongs.
 * table_name: The name of table to which materialized view belongs.
 */
public class DropMaterializedViewStmt extends DdlStmt {

    private final boolean ifExists;
    private final TableName dbMvName;
    private final TableName dbTblName;

    public DropMaterializedViewStmt(boolean ifExists, TableName dbMvName, TableName dbTblName) {
        this.ifExists = ifExists;
        this.dbMvName = dbMvName;
        this.dbTblName = dbTblName;
    }

    public boolean isSetIfExists() {
        return ifExists;
    }

    public String getMvName() {
        return dbMvName.getTbl();
    }

    public String getTblName() {
        if (dbTblName != null) {
            return dbTblName.getTbl();
        } else {
            return null;
        }
    }

    public String getDbName() {
        if (dbTblName != null) {
            return dbTblName.getDb();
        } else {
            return dbMvName.getDb();
        }
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (dbTblName != null && !Strings.isNullOrEmpty(dbMvName.getDb())) {
            throw new AnalysisException(
                    "Syntax drop materialized view [mv-name] from db.name mush specify database name explicitly in `from`");
        }
        if (dbTblName != null) {
            if (!Strings.isNullOrEmpty(dbMvName.getDb())) {
                throw new AnalysisException("If the database appears after the from statement, " +
                        "the materialized view should not include the database name prefix");
            }

            if (Strings.isNullOrEmpty(dbTblName.getDb())) {
                throw new AnalysisException(
                        "Syntax drop materialized view [mv-name] from db.name mush specify database name explicitly in `from`");
            }
            dbTblName.analyze(analyzer);
        }

        if (Strings.isNullOrEmpty(dbMvName.getDb())) {
            dbMvName.setDb(analyzer.getDefaultDb());
        }

        if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(ConnectContext.get(), getDbName(), PrivPredicate.DROP)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP");
        }

        dbMvName.analyze(analyzer);

        Database db = Catalog.getCurrentCatalog().getDb(dbMvName.getDb());
        if (dbTblName == null) {
            boolean hasMv = false;
            for (Table table : db.getTables()) {
                if (table.getType() == Table.TableType.OLAP) {
                    OlapTable olapTable = (OlapTable) table;
                    List<MaterializedIndex> visibleMaterializedViews = olapTable.getVisibleIndex();
                    long baseIdx = olapTable.getBaseIndexId();

                    for (MaterializedIndex mvIdx : visibleMaterializedViews) {
                        if (baseIdx == mvIdx.getId()) {
                            continue;
                        }
                        if (olapTable.getIndexNameById(mvIdx.getId()).equals(dbMvName.getTbl())) {
                            if (hasMv) {
                                throw new AnalysisException(
                                        "There are multiple materialized views called " + dbMvName.getTbl() +
                                                ". Use the syntax [drop materialized view db.mv_name on table] to drop materialized view");
                            }
                            hasMv = true;
                        }
                    }
                }
            }
            if (!hasMv && !isSetIfExists()) {
                throw new AnalysisException("The materialized " + dbMvName.getTbl() + " is not exist");
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DROP MATERIALIZED VIEW ");
        if (ifExists) {
            stringBuilder.append("IF EXISTS ");
        }
        stringBuilder.append("`").append(dbMvName).append("`");

        if (dbTblName != null) {
            stringBuilder.append(" IN `").append(dbTblName).append("`");
        }
        return stringBuilder.toString();
    }
}
