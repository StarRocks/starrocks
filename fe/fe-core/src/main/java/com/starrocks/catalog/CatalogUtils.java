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

import com.google.common.collect.Sets;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.InvalidOlapTableStateException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

public class CatalogUtils {

    private static final Logger LOG = LogManager.getLogger(CatalogUtils.class);

    public static String normalizeTableName(String dbName, String tableName) {
        return dbName + "." + tableName;
    }

    // check table exist
    public static void checkTableExist(Database db, String tableName) throws DdlException {
        Table table = db.getTable(tableName);
        if (table == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
        }
    }

    // check table type is OLAP
    public static void checkNativeTable(Database db, Table table) throws DdlException {
        if (!table.isNativeTableOrMaterializedView()) {
            throw new DdlException("Table[" + table.getName() + "] is not OLAP table or LAKE table");
        }
    }

    // check table state
    public static void checkTableState(OlapTable olapTable, String tableName) throws DdlException {
        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            throw InvalidOlapTableStateException.of(olapTable.getState(), tableName);
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

    public static Set<String> getPartitionNamesFromAddPartitionClause(AddPartitionClause addPartitionClause) {
        Set<String> partitionNames = new TreeSet<>();
        PartitionDesc partitionDesc = addPartitionClause.getPartitionDesc();
        if (partitionDesc instanceof SingleItemListPartitionDesc
                || partitionDesc instanceof MultiItemListPartitionDesc
                || partitionDesc instanceof SingleRangePartitionDesc) {
            partitionNames.add(partitionDesc.getPartitionName());
        } else if (partitionDesc instanceof RangePartitionDesc) {
            for (PartitionDesc desc : ((RangePartitionDesc) partitionDesc).getSingleRangePartitionDescs()) {
                partitionNames.add(desc.getPartitionName());
            }
        } else if (partitionDesc instanceof ListPartitionDesc) {
            for (PartitionDesc desc : (((ListPartitionDesc) partitionDesc).getPartitionDescs())) {
                partitionNames.add(desc.getPartitionName());
            }
        }
        return partitionNames;
    }

    // Used to temporarily disable some command on lake table and remove later.
    public static void checkIsLakeTable(String dbName, String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            return;
        }

        Table table = db.getTable(tableName);
        if (table == null) {
            return;
        }
        if (table.isCloudNativeTable()) {
            throw new SemanticException(PARSER_ERROR_MSG.unsupportedOpWithInfo("lake table " + db + "." + tableName));
        }
    }

    private static void getLiteralByPartitionMap(ListPartitionInfo listPartitionInfo,
                                                 List<Partition> partitionList,
                                                 Set<LiteralExpr> simpleSet,
                                                 Set<Object> simpleValueSet,
                                                 Set<List<LiteralExpr>> multiSet,
                                                 Set<List<Object>> multiValueSet) {
        Map<Long, List<LiteralExpr>> listMap = listPartitionInfo.getLiteralExprValues();
        Map<Long, List<List<LiteralExpr>>> multiListMap = listPartitionInfo.getMultiLiteralExprValues();
        for (Partition partition : partitionList) {
            if (!listMap.isEmpty()) {
                List<LiteralExpr> currentPartitionValueList = listMap.get(partition.getId());
                if (currentPartitionValueList != null) {
                    simpleSet.addAll(currentPartitionValueList);
                    for (LiteralExpr literalExpr : currentPartitionValueList) {
                        simpleValueSet.add(literalExpr.getRealObjectValue());
                    }
                    continue;
                }
            }
            if (!multiListMap.isEmpty()) {
                List<List<LiteralExpr>> currentMultiplePartitionValueList = multiListMap.get(partition.getId());
                if (currentMultiplePartitionValueList != null) {
                    multiSet.addAll(currentMultiplePartitionValueList);
                    for (List<LiteralExpr> list : currentMultiplePartitionValueList) {
                        List<Object> valueList = new ArrayList<>();
                        for (LiteralExpr literalExpr : list) {
                            valueList.add(literalExpr.getRealObjectValue());
                        }
                        multiValueSet.add(valueList);
                    }
                }
            }
        }
    }

    public static void checkTempPartitionStrictMatch(List<Partition> partitionList,
                                                     List<Partition> tempPartitionList,
                                                     ListPartitionInfo listPartitionInfo) throws DdlException {
        Set<LiteralExpr> simpleSet = new HashSet<>();
        Set<List<LiteralExpr>> multiSet = new HashSet<>();
        Set<Object> simpleValueSet = new HashSet<>();
        Set<List<Object>> multiValueSet = new HashSet<>();
        getLiteralByPartitionMap(listPartitionInfo, partitionList, simpleSet, simpleValueSet, multiSet, multiValueSet);

        Set<LiteralExpr> tempSimpleSet = new HashSet<>();
        Set<List<LiteralExpr>> tempMultiSet = new HashSet<>();
        Set<Object> tempSimpleValueSet = new HashSet<>();
        Set<List<Object>> tempMultiValueSet = new HashSet<>();
        getLiteralByPartitionMap(listPartitionInfo, tempPartitionList, tempSimpleSet,
                tempSimpleValueSet, tempMultiSet, tempMultiValueSet);

        if (!simpleSet.isEmpty() && !tempSimpleSet.isEmpty()) {
            if (!simpleSet.equals(tempSimpleSet)) {
                throw new DdlException("2 list partitions are not strictly matched, "
                        + simpleValueSet + " vs " + tempSimpleValueSet);
            }
        }

        if (!multiSet.isEmpty() && !tempMultiSet.isEmpty()) {
            if (!multiSet.equals(tempMultiSet)) {
                throw new DdlException("2 list partitions are not strictly matched, "
                        + multiValueSet + " vs " + tempMultiValueSet);
            }
        }
    }

    public static void checkTempPartitionConflict(List<Partition> partitionList,
                                                  List<Partition> tempPartitionList,
                                                  ListPartitionInfo listPartitionInfo) throws DdlException {
        Map<Long, List<LiteralExpr>> listMap = listPartitionInfo.getLiteralExprValues();
        Map<Long, List<List<LiteralExpr>>> multiListMap = listPartitionInfo.getMultiLiteralExprValues();
        Map<Long, List<LiteralExpr>> newListMap = new HashMap<>(listMap);
        Map<Long, List<List<LiteralExpr>>> newMultiListMap = new HashMap<>(multiListMap);

        // Filter the partition that needs to be replaced
        partitionList.forEach(partition -> {
            newListMap.remove(partition.getId());
            newMultiListMap.remove(partition.getId());
        });

        // Filter out temporary partitions
        Set<Object> tempSet = new HashSet<>();
        Set<List<Object>> tempMultiSet = new HashSet<>();
        for (Partition partition : tempPartitionList) {
            if (listPartitionInfo.isMultiColumnPartition()) {
                tempMultiSet = multiListMap.get(partition.getId())
                        .stream()
                        .map(literalExprs -> literalExprs
                                .stream()
                                .map(LiteralExpr::getRealObjectValue)
                                .collect(Collectors.toList()))
                        .collect(Collectors.toSet());
                newMultiListMap.remove(partition.getId());
            } else {
                tempSet = listMap.get(partition.getId())
                        .stream()
                        .map(LiteralExpr::getRealObjectValue)
                        .collect(Collectors.toSet());
                newListMap.remove(partition.getId());
            }
        }

        // Check whether the remaining partition overlaps with the temporary partition
        if (!tempSet.isEmpty() && !newListMap.isEmpty()) {
            List<Object> newList = new ArrayList<>();
            for (List<LiteralExpr> baseList : newListMap.values()) {
                baseList.forEach(item -> {
                    newList.add(item.getRealObjectValue());
                });
            }
            for (Object tempSingle : tempSet) {
                if (newList.contains(tempSingle)) {
                    throw new DdlException("Range: " + tempSingle + " conflicts with existing range");
                }
            }
        }

        if (!tempMultiSet.isEmpty() && !newMultiListMap.isEmpty()) {
            List<List<Object>> newMultiList = new ArrayList<>();
            for (List<List<LiteralExpr>> baseMultiList : newMultiListMap.values()) {
                baseMultiList.forEach(itemList -> {
                    List<Object> objectList = new ArrayList<>();
                    itemList.forEach(item -> {
                        objectList.add(item.getRealObjectValue());
                    });
                    newMultiList.add(objectList);
                });
            }
            for (List<Object> tempMulti : tempMultiSet) {
                if (newMultiList.contains(tempMulti)) {
                    throw new DdlException("Range: " + tempMulti + " conflicts with existing range");
                }
            }
        }
    }

    public static void checkPartitionValuesExistForAddListPartition(OlapTable olapTable, PartitionDesc partitionDesc,
                                                                    boolean isTemp)
            throws DdlException {
        try {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) olapTable.getPartitionInfo();
            Set<Long> partitionIds = Sets.newHashSet(listPartitionInfo.getPartitionIds(isTemp));

            if (partitionDesc instanceof SingleItemListPartitionDesc) {
                Set<LiteralExpr> existingValues = listPartitionInfo.getValuesSet(partitionIds);
                SingleItemListPartitionDesc singleItemListPartitionDesc = (SingleItemListPartitionDesc) partitionDesc;
                for (LiteralExpr item : singleItemListPartitionDesc.getLiteralExprValues()) {
                    if (existingValues.contains(item)) {
                        throw new DdlException("Duplicate partition value " + item.getStringValue());
                    }
                }
            } else if (partitionDesc instanceof MultiItemListPartitionDesc) {
                int partitionColSize = listPartitionInfo.getPartitionColumnsSize();
                MultiItemListPartitionDesc multiItemListPartitionDesc = (MultiItemListPartitionDesc) partitionDesc;
                checkItemValuesValid(partitionColSize, partitionIds, listPartitionInfo.getMultiLiteralExprValues(),
                        multiItemListPartitionDesc);
            }
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
    }

    private static void checkItemValuesValid(int partitionColSize, Set<Long> partitionIds,
                                             Map<Long, List<List<LiteralExpr>>> idToMultiLiteralExprValues,
                                             MultiItemListPartitionDesc multiItemListPartitionDesc)
            throws AnalysisException, DdlException {
        List<Map<LiteralExpr, Set<Long>>> valueToIdIndexList = new ArrayList<>();
        for (int i = 0; i < partitionColSize; ++i) {
            valueToIdIndexList.add(new HashMap<>());
        }

        for (Long partitionId : partitionIds) {
            List<List<LiteralExpr>> multiValues = idToMultiLiteralExprValues.get(partitionId);
            if (multiValues == null) {
                // Because of the shadow partition, there may be a situation where multiValues==null
                continue;
            }

            for (int columnIdx = 0; columnIdx < partitionColSize; ++columnIdx) {
                List<LiteralExpr> col = new ArrayList<>();
                for (List<LiteralExpr> multiValue : multiValues) {
                    LiteralExpr v = multiValue.get(columnIdx);
                    col.add(v);
                }

                Map<LiteralExpr, Set<Long>> v2i = valueToIdIndexList.get(columnIdx);
                for (LiteralExpr value : col) {
                    if (v2i.containsKey(value)) {
                        v2i.get(value).add(partitionId);
                    } else {
                        Set<Long> s = new HashSet<>();
                        s.add(partitionId);
                        v2i.put(value, s);
                    }
                }
            }
        }

        for (List<LiteralExpr> values : multiItemListPartitionDesc.getMultiLiteralExprValues()) {
            boolean isValid = false;
            Set<Long> pSet = null;
            for (int i = 0; i < values.size(); ++i) {
                Map<LiteralExpr, Set<Long>> valueToPartitionIdIndex = valueToIdIndexList.get(i);
                if (!valueToPartitionIdIndex.containsKey(values.get(i))) {
                    isValid = true;
                    break;
                } else {
                    if (pSet == null) {
                        pSet = new HashSet<>(valueToPartitionIdIndex.get(values.get(i)));
                    } else {
                        pSet.retainAll(valueToPartitionIdIndex.get(values.get(i)));
                    }
                }
                if (pSet.isEmpty()) {
                    isValid = true;
                    break;
                }
            }
            if (!isValid) {
                List<String> multiValues = values.stream().map(LiteralExpr::getStringValue)
                        .collect(Collectors.toList());
                throw new DdlException("Duplicate values " + "(" + String.join(",", multiValues) + ") ");
            }
        }
    }

    public static int divisibleBucketNum(int backendNum) {
        while (backendNum > 7) {
            if (backendNum % 2 == 0) {
                backendNum = backendNum / 2;
            } else if (backendNum % 3 == 0) {
                backendNum = backendNum / 3;
            } else if (backendNum % 5 == 0) {
                backendNum = backendNum / 5;
            } else if (backendNum % 7 == 0) {
                backendNum = backendNum / 7;
            } else {
                backendNum = backendNum / 2;
            }
        }

        return backendNum;
    }

    public static int calPhysicalPartitionBucketNum() {
        int backendNum = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds().size();

        if (RunMode.isSharedDataMode()) {
            backendNum = backendNum + GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAliveComputeNodeNumber();
        }

        return divisibleBucketNum(backendNum);
    }

    public static int calBucketNumAccordingToBackends() {
        int backendNum = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds().size();

        if (RunMode.isSharedDataMode()) {
            backendNum = backendNum + GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAliveComputeNodeNumber();
        }

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
    }

    public static int calAvgBucketNumOfRecentPartitions(OlapTable olapTable, int recentPartitionNum,
                                                        boolean enableAutoTabletDistribution) {
        // 1. If the partition is less than recentPartitionNum, use backendNum to speculate the bucketNum
        //    Or the Config.enable_auto_tablet_distribution is disabled
        int bucketNum = 0;
        if (olapTable.getPartitions().size() < recentPartitionNum || !enableAutoTabletDistribution) {
            bucketNum = CatalogUtils.calBucketNumAccordingToBackends();
            // If table is not partitioned, the bucketNum should be at least DEFAULT_UNPARTITIONED_TABLE_BUCKET_NUM
            if (!olapTable.getPartitionInfo().isPartitioned()) {
                bucketNum = bucketNum > FeConstants.DEFAULT_UNPARTITIONED_TABLE_BUCKET_NUM ?
                        bucketNum : FeConstants.DEFAULT_UNPARTITIONED_TABLE_BUCKET_NUM;
            }
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
        // speculateTabletNum may be not accurate, so we need to take the max value of bucketNum and speculateTabletNum
        bucketNum = (int) Math.max(bucketNum, speculateTabletNum);
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

}
