// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ShowDataStmt.java

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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class ShowDataStmt extends ShowStmt {
    private static final ShowResultSetMetaData SHOW_TABLE_DATA_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Size", ScalarType.createVarchar(30)))
                    .addColumn(new Column("ReplicaCount", ScalarType.createVarchar(20)))
                    .build();

    private static final ShowResultSetMetaData SHOW_INDEX_DATA_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("IndexName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Size", ScalarType.createVarchar(30)))
                    .addColumn(new Column("ReplicaCount", ScalarType.createVarchar(20)))
                    .addColumn(new Column("RowCount", ScalarType.createVarchar(20)))
                    .build();

    private String dbName;
    private String tableName;

    List<List<String>> totalRows;

    public ShowDataStmt(String dbName, String tableName) {
        this.dbName = dbName;
        this.tableName = tableName;

        this.totalRows = new LinkedList<List<String>>();
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            dbName = ClusterNamespace.getFullName(dbName);
        }

        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        db.readLock();
        try {
            if (tableName == null) {
                long totalSize = 0;
                long totalReplicaCount = 0;

                // sort by table name
                List<Table> tables = db.getTables();
                SortedSet<Table> sortedTables = new TreeSet<>(new Comparator<Table>() {
                    @Override
                    public int compare(Table t1, Table t2) {
                        return t1.getName().compareTo(t2.getName());
                    }
                });

                for (Table table : tables) {
                    if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ConnectContext.get(), dbName,
                            table.getName(),
                            PrivPredicate.SHOW)) {
                        continue;
                    }
                    sortedTables.add(table);
                }

                for (Table table : sortedTables) {
                    if (!table.isNativeTable()) {
                        continue;
                    }

                    OlapTable olapTable = (OlapTable) table;
                    long tableSize = olapTable.getDataSize();
                    long replicaCount = olapTable.getReplicaCount();

                    Pair<Double, String> tableSizePair = DebugUtil.getByteUint(tableSize);
                    String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(tableSizePair.first) + " "
                            + tableSizePair.second;

                    List<String> row = Arrays.asList(table.getName(), readableSize, String.valueOf(replicaCount));
                    totalRows.add(row);

                    totalSize += tableSize;
                    totalReplicaCount += replicaCount;
                } // end for tables

                Pair<Double, String> totalSizePair = DebugUtil.getByteUint(totalSize);
                String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalSizePair.first) + " "
                        + totalSizePair.second;
                List<String> total = Arrays.asList("Total", readableSize, String.valueOf(totalReplicaCount));
                totalRows.add(total);

                // quota
                long quota = db.getDataQuota();
                long replicaQuota = db.getReplicaQuota();
                Pair<Double, String> quotaPair = DebugUtil.getByteUint(quota);
                String readableQuota = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(quotaPair.first) + " "
                        + quotaPair.second;

                List<String> quotaRow = Arrays.asList("Quota", readableQuota, String.valueOf(replicaQuota));
                totalRows.add(quotaRow);

                // left
                long left = Math.max(0, quota - totalSize);
                long replicaCountLeft = Math.max(0, replicaQuota - totalReplicaCount);
                Pair<Double, String> leftPair = DebugUtil.getByteUint(left);
                String readableLeft = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(leftPair.first) + " "
                        + leftPair.second;
                List<String> leftRow = Arrays.asList("Left", readableLeft, String.valueOf(replicaCountLeft));
                totalRows.add(leftRow);
            } else {
                if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ConnectContext.get(), dbName,
                        tableName,
                        PrivPredicate.SHOW)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW DATA",
                            ConnectContext.get().getQualifiedUser(),
                            ConnectContext.get().getRemoteIP(),
                            tableName);
                }

                Table table = db.getTable(tableName);
                if (table == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
                }

                if (!table.isNativeTable()) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NOT_OLAP_TABLE, tableName);
                }

                OlapTable olapTable = (OlapTable) table;
                int i = 0;
                long totalSize = 0;
                long totalReplicaCount = 0;

                // sort by index name
                Map<String, Long> indexNames = olapTable.getIndexNameToId();
                Map<String, Long> sortedIndexNames = new TreeMap<String, Long>();
                for (Map.Entry<String, Long> entry : indexNames.entrySet()) {
                    sortedIndexNames.put(entry.getKey(), entry.getValue());
                }

                for (Long indexId : sortedIndexNames.values()) {
                    long indexSize = 0;
                    long indexReplicaCount = 0;
                    long indexRowCount = 0;
                    for (Partition partition : olapTable.getAllPartitions()) {
                        MaterializedIndex mIndex = partition.getIndex(indexId);
                        indexSize += mIndex.getDataSize();
                        indexReplicaCount += mIndex.getReplicaCount();
                        indexRowCount += mIndex.getRowCount();
                    }

                    Pair<Double, String> indexSizePair = DebugUtil.getByteUint(indexSize);
                    String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(indexSizePair.first) + " "
                            + indexSizePair.second;

                    List<String> row = null;
                    if (i == 0) {
                        row = Arrays.asList(tableName,
                                olapTable.getIndexNameById(indexId),
                                readableSize, String.valueOf(indexReplicaCount),
                                String.valueOf(indexRowCount));
                    } else {
                        row = Arrays.asList("",
                                olapTable.getIndexNameById(indexId),
                                readableSize, String.valueOf(indexReplicaCount),
                                String.valueOf(indexRowCount));
                    }

                    totalSize += indexSize;
                    totalReplicaCount += indexReplicaCount;
                    totalRows.add(row);

                    i++;
                } // end for indices

                Pair<Double, String> totalSizePair = DebugUtil.getByteUint(totalSize);
                String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalSizePair.first) + " "
                        + totalSizePair.second;
                List<String> row = Arrays.asList("", "Total", readableSize, String.valueOf(totalReplicaCount), "");
                totalRows.add(row);
            }
        } finally {
            db.readUnlock();
        }
    }

    public boolean hasTable() {
        return this.tableName != null;
    }

    public List<List<String>> getResultRows() throws AnalysisException {
        return totalRows;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        if (tableName != null) {
            return SHOW_INDEX_DATA_META_DATA;
        } else {
            return SHOW_TABLE_DATA_META_DATA;
        }
    }

    @Override
    public String toSql() {
        StringBuilder builder = new StringBuilder();
        builder.append("SHOW DATA");
        if (dbName == null) {
            return builder.toString();
        }

        builder.append(" FROM `").append(dbName).append("`");
        if (tableName == null) {
            return builder.toString();
        }
        builder.append(".`").append(tableName).append("`");
        return builder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowDataStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}

