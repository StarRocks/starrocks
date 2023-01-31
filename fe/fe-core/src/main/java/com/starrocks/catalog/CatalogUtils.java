// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
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

    public static void checkPartitionValuesExistForReplaceListPartition(ListPartitionInfo listPartitionInfo,
                                                                        Partition partition)
            throws DdlException {
        try {
            List<Long> partitionIds = listPartitionInfo.getPartitionIds(false);
            Map<Long, List<LiteralExpr>> literalExprValues = listPartitionInfo.getLiteralExprValues();
            Map<Long, List<List<LiteralExpr>>> multiLiteralExprValues = listPartitionInfo.getMultiLiteralExprValues();

            List<LiteralExpr> literalExprs = listPartitionInfo.getLiteralExprValues().get(partition.getId());
            List<List<LiteralExpr>> multiLiteral = listPartitionInfo.getMultiLiteralExprValues().get(partition.getId());
            if (!literalExprValues.isEmpty()) {
                listPartitionInfo.setBatchLiteralExprValues(listPartitionInfo.getIdToValues());
                List<LiteralExpr> allLiteralExprValues = Lists.newArrayList();
                literalExprValues.forEach((k, v) -> {
                    if (partitionIds.contains(k)) {
                        allLiteralExprValues.addAll(v);
                    }
                });
                if (literalExprs != null) {
                    for (LiteralExpr item : literalExprs) {
                        for (LiteralExpr value : allLiteralExprValues) {
                            if (item.getStringValue().equals(value.getStringValue())) {
                                throw new DdlException("Duplicate partition value %s");
                            }
                        }
                    }
                }
            } else if (!multiLiteralExprValues.isEmpty()) {
                listPartitionInfo.setBatchMultiLiteralExprValues(listPartitionInfo.getIdToMultiValues());
                List<List<LiteralExpr>> allMultiLiteralExprValues = Lists.newArrayList();
                listPartitionInfo.getMultiLiteralExprValues().forEach((k, v) -> {
                    if (partitionIds.contains(k)) {
                        allMultiLiteralExprValues.addAll(v);
                    }
                });

                int partitionColSize = listPartitionInfo.getPartitionColumns().size();
                for (List<LiteralExpr> itemExpr : multiLiteral) {
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

    public static void checkPartitionValuesExistForAddListPartition(OlapTable olapTable, PartitionDesc partitionDesc,
                                                                    boolean isTemp)
            throws DdlException {
        try {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) olapTable.getPartitionInfo();
            List<Long> partitionIds = listPartitionInfo.getPartitionIds(isTemp);

            if (partitionDesc instanceof SingleItemListPartitionDesc) {
                listPartitionInfo.setBatchLiteralExprValues(listPartitionInfo.getIdToValues());
                List<LiteralExpr> allLiteralExprValues = Lists.newArrayList();
                listPartitionInfo.getLiteralExprValues().forEach((k, v) -> {
                    if (partitionIds.contains(k)) {
                        allLiteralExprValues.addAll(v);
                    }
                });

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
                listPartitionInfo.getMultiLiteralExprValues().forEach((k, v) -> {
                    if (partitionIds.contains(k)) {
                        allMultiLiteralExprValues.addAll(v);
                    }
                });

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
        if (backendNum <= 3) {
            bucketNum = 2 * backendNum;
        } else if (backendNum <= 6) {
            bucketNum = backendNum;
        } else if (backendNum <= 12) {
            bucketNum = 12;
        } else {
            bucketNum = Math.min(backendNum, 48);
        }
        return bucketNum;
    }
}
