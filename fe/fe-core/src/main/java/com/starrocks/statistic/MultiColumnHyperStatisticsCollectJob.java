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

package com.starrocks.statistic;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.qe.OriginStatement;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.ValuesRelation;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.statistic.StatsConstants.MULTI_COLUMN_STATISTICS_TABLE_NAME;

public class MultiColumnHyperStatisticsCollectJob extends HyperStatisticsCollectJob {
    public MultiColumnHyperStatisticsCollectJob(Database db, Table table, List<Long> partitionIdList, List<String> columnNames,
                                                List<Type> columnTypes, StatsConstants.AnalyzeType type,
                                                StatsConstants.ScheduleType scheduleType, Map<String, String> properties,
                                                List<StatsConstants.StatisticsType> statsTypes,
                                                List<List<String>> columnGroups) {
        super(db, table, partitionIdList, columnNames, columnTypes, type, scheduleType, properties, statsTypes, columnGroups);
    }

    public StatementBase createInsertStmt() {
        List<String> targetColumnNames = StatisticUtils.buildStatsColumnDef(MULTI_COLUMN_STATISTICS_TABLE_NAME).stream()
                .map(ColumnDef::getName)
                .collect(Collectors.toList());

        String sql = "INSERT INTO _statistics_.multi_column_statistics(" + String.join(", ", targetColumnNames) +
                ") values " + String.join(", ", sqlBuffer) + ";";
        QueryStatement qs = new QueryStatement(new ValuesRelation(rowsBuffer, targetColumnNames));
        InsertStmt insert = new InsertStmt(new TableName("_statistics_", "multi_column_statistics"), qs);
        insert.setTargetColumnNames(targetColumnNames);
        insert.setOrigStmt(new OriginStatement(sql, 0));
        return insert;
    }
}
