// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;

import java.util.List;

public class MaterializedViewAnalyzer {
    public static void analyzeDropMaterializedView(DropMaterializedViewStmt stmt, ConnectContext session) {
        stmt.getDbMvName().normalization(session);
        if (stmt.getDbTblName() != null) {
            stmt.getDbTblName().normalization(session);
        }

        Database db = Catalog.getCurrentCatalog().getDb(stmt.getDbName());
        if (stmt.getDbTblName() == null) {
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
                        if (olapTable.getIndexNameById(mvIdx.getId()).equals(stmt.getDbMvName().getTbl())) {
                            if (hasMv) {
                                throw new SemanticException(
                                        "There are multiple materialized views called " + stmt.getDbMvName().getTbl() +
                                                ". Use the syntax [drop materialized view db.mv_name on table] " +
                                                "to drop materialized view");
                            }
                            hasMv = true;
                        }
                    }
                }
            }
        }
        return;
    }
}