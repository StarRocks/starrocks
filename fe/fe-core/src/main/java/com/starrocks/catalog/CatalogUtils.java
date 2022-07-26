// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.catalog;

import com.google.common.collect.Sets;
import com.starrocks.analysis.SingleRangePartitionDesc;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.util.ListComparator;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class CatalogUtils {

    private static final Logger LOG = LogManager.getLogger(CatalogUtils.class);

    // check table exist
    public static void checkTableExist(Database db, String tableName) throws DdlException {
        Table table = db.getTable(tableName);
        if (table == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
        }
    }

    // check table type is OLAP
    public static void checkTableTypeOLAP(Database db, Table table) throws DdlException {
        if (table.getType() != Table.TableType.OLAP && table.getType() != Table.TableType.MATERIALIZED_VIEW) {
            throw new DdlException("Table[" + table.getName() + "] is not OLAP table");
        }
    }

    // check table state
    public static void checkTableState(OlapTable olapTable, String tableName) throws DdlException {
        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            throw new DdlException("Table[" + tableName + "]'s state is not NORMAL");
        }
    }

    public static Set<String> checkPartitionNameExistForAddPartitions(OlapTable olapTable,
                                                                      List<SingleRangePartitionDesc> singleRangePartitionDescs)
            throws DdlException {
        Set<String> existPartitionNameSet = Sets.newHashSet();
        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            String partitionName = singleRangePartitionDesc.getPartitionName();
            if (olapTable.checkPartitionNameExist(partitionName)) {
                if (singleRangePartitionDesc.isSetIfNotExists()) {
                    existPartitionNameSet.add(partitionName);
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_SAME_NAME_PARTITION, partitionName);
                }
            }
        }
        return existPartitionNameSet;
    }

    // Used to temporarily disable some command on lake table and remove later.
    public static void checkIsLakeTable(String dbName, String tableName) throws AnalysisException {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            return;
        }

        db.readLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                return;
            }
            if (table.isLakeTable()) {
                throw new AnalysisException("Unsupported operation on lake table [" + dbName + "." + tableName + "]");
            }
        } finally {
            db.readUnlock();
        }
    }

    public static void convertToMetaResult(BaseProcResult result, List<List<Comparable>> infos) {
        // order by asc
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0);
        Collections.sort(infos, comparator);

        // set result
        for (List<Comparable> info : infos) {
            List<String> row = new ArrayList<String>(info.size());
            for (Comparable comparable : info) {
                row.add(String.valueOf(comparable));
            }
            result.addRow(row);
        }
    }
}
