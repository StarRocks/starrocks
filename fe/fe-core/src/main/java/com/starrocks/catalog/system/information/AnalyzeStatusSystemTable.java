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

package com.starrocks.catalog.system.information;

import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ShowAnalyzeStatusStmt;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.NativeAnalyzeStatus;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.columns.TableNamePredicate;
import com.starrocks.thrift.TAnalyzeStatusItem;
import com.starrocks.thrift.TAnalyzeStatusReq;
import com.starrocks.thrift.TAnalyzeStatusRes;
import com.starrocks.thrift.TSchemaTableType;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * | Id | Database| Table| Columns| Type| Schedule | Status  | StartTime| EndTime| Properties | Reason |
 */
public class AnalyzeStatusSystemTable extends SystemTable {

    private static final String NAME = "analyze_status";
    private static final List<Column> COLUMNS;

    static {
        COLUMNS = Lists.newArrayList(ShowAnalyzeStatusStmt.META_DATA.getColumns());
        COLUMNS.add(1, new Column("Catalog", createNameType()));
    }

    public AnalyzeStatusSystemTable() {
        super(SystemId.ANALYZE_STATUS, NAME, TableType.SCHEMA, COLUMNS, TSchemaTableType.SCH_ANALYZE_STATUS);
    }

    public static SystemTable create() {
        return new AnalyzeStatusSystemTable();
    }

    public static TAnalyzeStatusRes query(TAnalyzeStatusReq req) {
        TAnalyzeStatusRes res = new TAnalyzeStatusRes();
        List<TAnalyzeStatusItem> itemList = Lists.newArrayList();
        res.setItems(itemList);

        Optional<Long> dbId = Optional.empty();
        if (StringUtils.isNotEmpty(req.getTable_database())) {
            Optional<Database> db =
                    GlobalStateMgr.getCurrentState().getLocalMetastore().mayGetDb(req.getTable_database());
            dbId = db.map(Database::getId);
        }

        TableName tableName = new TableName(req.getTable_catalog(), req.getTable_database(), req.getTable_name());
        TableNamePredicate predicate = new TableNamePredicate(tableName);
        Collection<AnalyzeStatus> analyzeList =
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAnalyzeStatusMap().values();

        for (AnalyzeStatus analyze : analyzeList) {
            if (dbId.isPresent() && analyze instanceof NativeAnalyzeStatus) {
                NativeAnalyzeStatus status = (NativeAnalyzeStatus) analyze;
                if (dbId.get() != status.getDbId()) {
                    continue;
                }
            }
            TAnalyzeStatusItem item = new TAnalyzeStatusItem();
            itemList.add(item);
            item.setCatalog_name("");
            item.setDatabase_name("");
            item.setTable_name("");
            Table table = null;
            try {
                item.setId(String.valueOf(analyze.getId()));
                item.setCatalog_name(analyze.getCatalogName());
                item.setDatabase_name(analyze.getDbName());
                item.setTable_name(analyze.getTableName());
                TableName name = new TableName(item.getCatalog_name(), item.getDatabase_name(), item.getTable_name());
                table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(
                        new ConnectContext(), analyze.getCatalogName(), analyze.getDbName(), analyze.getTableName());
                if (!predicate.test(name) || table == null) {
                    continue;
                }
            } catch (MetaNotFoundException ignored) {
            }

            String columnStr = "ALL";
            List<String> columns = analyze.getColumns();
            long totalCollectColumnsSize = StatisticUtils.getCollectibleColumns(table).size();
            if (null != columns && !columns.isEmpty() && (columns.size() != totalCollectColumnsSize)) {
                columnStr = String.join(",", columns);
            }
            item.setColumns(columnStr);

            item.setType(analyze.getType().name());
            item.setSchedule(analyze.getScheduleType().name());
            item.setStatus(analyze.getStatus().name());
            item.setStart_time(DateUtils.formatDateTimeUnix(analyze.getStartTime()));
            item.setEnd_time(DateUtils.formatDateTimeUnix(analyze.getEndTime()));
            item.setProperties(analyze.getProperties() == null ? "{}" : analyze.getProperties().toString());
            item.setReason(analyze.getReason());
        }

        return res;
    }
}
