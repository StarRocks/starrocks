// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
    public static void checkNativeTable(Database db, Table table) throws DdlException {
        if (!table.isNativeTable()) {
            throw new DdlException("Table[" + table.getName() + "] is not OLAP table or LAKE table");
        }
    }

    // check table state
    public static void checkTableState(OlapTable olapTable, String tableName) throws DdlException {
        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            throw new DdlException("Table[" + tableName + "]'s state is not NORMAL");
        }
    }

    public static Set<String> checkPartitionNameExistForAddPartitions(OlapTable olapTable,
                                                                      List<PartitionDesc> partitionDescs)
            throws DdlException {
        Set<String> existPartitionNameSet = Sets.newHashSet();
        for (PartitionDesc partitionDesc : partitionDescs) {
            String partitionName = partitionDesc.getPartitionName();
            if (olapTable.checkPartitionNameExist(partitionName)) {
                if (partitionDesc.isSetIfNotExists()) {
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

    public static void checkPartitionValuesExistForAddListPartition(OlapTable olapTable, PartitionDesc partitionDesc)
            throws DdlException {
        try {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) olapTable.getPartitionInfo();
            if (partitionDesc instanceof SingleItemListPartitionDesc) {
                listPartitionInfo.setBatchLiteralExprValues(listPartitionInfo.getIdToValues());
                List<LiteralExpr> allLiteralExprValues = Lists.newArrayList();
                listPartitionInfo.getLiteralExprValues().forEach((k, v) -> allLiteralExprValues.addAll(v));

                SingleItemListPartitionDesc singleItemListPartitionDesc = (SingleItemListPartitionDesc) partitionDesc;
                for (LiteralExpr item : singleItemListPartitionDesc.getLiteralExprValues()) {
                    for (LiteralExpr value : allLiteralExprValues) {
                        if (item.getStringValue().equals(value.getStringValue())) {
                            throw new DdlException("Duplicate partition value %s");
                        }
                    }
                }
            } else if (partitionDesc instanceof MultiItemListPartitionDesc) {
                listPartitionInfo.setBatchMultiLiteralExprValues(listPartitionInfo.getIdToMultiValues());
                List<List<LiteralExpr>> allMultiLiteralExprValues = Lists.newArrayList();
                listPartitionInfo.getMultiLiteralExprValues().forEach((k, v) -> allMultiLiteralExprValues.addAll(v));

                int partitionColSize = listPartitionInfo.getPartitionColumns().size();
                MultiItemListPartitionDesc multiItemListPartitionDesc = (MultiItemListPartitionDesc) partitionDesc;
                for (List<LiteralExpr> itemExpr : multiItemListPartitionDesc.getMultiLiteralExprValues()) {
                    for (List<LiteralExpr> valueExpr : allMultiLiteralExprValues) {
                        int duplicatedSize = 0;
                        for (int i = 0; i < itemExpr.size(); i++) {
                            String itemValue = itemExpr.get(i).getStringValue();
                            String value = valueExpr.get(i).getStringValue();
                            if (value.equals(itemValue)) {
                                duplicatedSize++;
                            }
                        }
                        if (duplicatedSize == partitionColSize) {
                            List<String> msg = itemExpr.stream()
                                    .map(value -> ("\"" + value.getStringValue() + "\""))
                                    .collect(Collectors.toList());
                            throw new DdlException("Duplicate values " +
                                    "(" + String.join(",", msg) + ") ");
                        }
                    }
                }
            }
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
    }

    public static int calBucketNumAccordingToBackends() {
        int backendNum = GlobalStateMgr.getCurrentSystemInfo().getBackendIds().size();
        // When POC, the backends is not greater than three most of the time.
        // The bucketNum will be given a small multiplier factor for small backends.
        int bucketNum = 0;
        if (backendNum <= 12) {
            bucketNum = 2 * backendNum;
        } else if (backendNum <= 24) {
            bucketNum = (int) (1.5 * backendNum);
        } else if (backendNum <= 36) {
            bucketNum = 36;
        } else {
            bucketNum = Math.min(backendNum, 48);
        }
        return bucketNum;
<<<<<<< HEAD
    }

    public static int calAvgBucketNumOfRecentPartitions(OlapTable olapTable, int recentPartitionNum,
                                                        boolean enableAutoTabletDistribution) {
        // 1. If the partition is less than recentPartitionNum, use backendNum to speculate the bucketNum
        //    Or the Config.enable_auto_tablet_distribution is disabled
        int bucketNum = 0;
        if (olapTable.getPartitions().size() < recentPartitionNum || !enableAutoTabletDistribution) {
            bucketNum = CatalogUtils.calBucketNumAccordingToBackends();
            return bucketNum;
        }

        // 2. If the partition is not imported anydata, use backendNum to speculate the bucketNum
        List<Partition> partitions = (List<Partition>) olapTable.getRecentPartitions(recentPartitionNum);
        boolean dataImported = true;
        for (Partition partition : partitions) {
            if (partition.getVisibleVersion() == 1) {
                dataImported = false;
                break;
            }
        }

        bucketNum = CatalogUtils.calBucketNumAccordingToBackends();
        if (!dataImported) {
            return bucketNum;
        }

        // 3. Use the totalSize of recentPartitions to speculate the bucketNum
        long maxDataSize = 0;
        for (Partition partition : partitions) {
            maxDataSize = Math.max(maxDataSize, partition.getDataSize());
        }
        // A tablet will be regarded using the 1GB size
        // And also the number will not be larger than the calBucketNumAccordingToBackends()
        long speculateTabletNum = (maxDataSize + FeConstants.AUTO_DISTRIBUTION_UNIT - 1) / FeConstants.AUTO_DISTRIBUTION_UNIT;
        bucketNum = (int) Math.min(bucketNum, speculateTabletNum);
        if (bucketNum == 0) {
            bucketNum = 1;
        }
        return bucketNum;
    }

    public static String addEscapeCharacter(String comment) {
        if (StringUtils.isEmpty(comment)) {
            return comment;
        }
        StringBuilder output = new StringBuilder();
        for (int i = 0; i < comment.length(); i++) {
            char c = comment.charAt(i);

            if (c == '\\' || c == '"') {
                output.append('\\');
                output.append(c);
            } else {
                output.append(c);
            }
        }
        return output.toString();
    }

=======

    }
>>>>>>> branch-2.5-mrs
}
