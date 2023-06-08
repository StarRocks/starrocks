// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.catalog;

import com.google.common.collect.Sets;
import com.starrocks.analysis.SingleRangePartitionDesc;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
<<<<<<< HEAD
=======
import com.starrocks.common.FeConstants;
import com.starrocks.common.InvalidOlapTableStateException;
>>>>>>> 6fe982a83 ([Enhancement] Detailed error message of invalid table state (#24806))
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        if (table.getType() != Table.TableType.OLAP) {
            throw new DdlException("Table[" + table.getName() + "] is not OLAP table");
        }
    }

    // check table state
    public static void checkTableState(OlapTable olapTable, String tableName) throws DdlException {
        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            throw InvalidOlapTableStateException.of(olapTable.getState(), tableName);
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

    public static boolean isUseStarOS(TStorageMedium storageMedium) {
        return storageMedium == TStorageMedium.S3;
    }

    // Used to temporarily disable some command on StarOS table and remove later.
    public static void checkOlapTableHasStarOSPartition(String dbName, String tableName) throws AnalysisException {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            return;
        }

        db.readLock();
        try {
            Table table = db.getTable(tableName);
            if (!(table instanceof OlapTable)) {
                return;
            }
            OlapTable olapTable = (OlapTable) table;
            for (Partition partition : olapTable.getPartitions()) {
                if (partition.isUseStarOS()) {
                    throw new AnalysisException("Unsupported operation because table [" + dbName + "." + tableName +
                            "] has StarOS partitions");
                }
            }
        } finally {
            db.readUnlock();
        }
    }
}
