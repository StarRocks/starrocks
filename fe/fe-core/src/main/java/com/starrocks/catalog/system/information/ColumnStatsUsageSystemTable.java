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

import com.google.api.client.util.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.statistic.columns.ColumnFullId;
import com.starrocks.statistic.columns.ColumnUsage;
import com.starrocks.statistic.columns.PredicateColumnsMgr;
import com.starrocks.thrift.TColumnStatsUsage;
import com.starrocks.thrift.TColumnStatsUsageReq;
import com.starrocks.thrift.TColumnStatsUsageRes;
import com.starrocks.thrift.TSchemaTableType;
import org.apache.commons.lang3.NotImplementedException;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The column_stats_usage is used to track the usage of column statistics
 */
public class ColumnStatsUsageSystemTable extends SystemTable {

    private static final String NAME = "column_stats_usage";

    public ColumnStatsUsageSystemTable() {

        super(SystemId.COLUMN_STATS_USAGE, NAME, TableType.SCHEMA,
                builder()
                        .column("TABLE_CATALOG", SystemTable.createNameType())
                        .column("TABLE_DATABASE", SystemTable.createNameType())
                        .column("TABLE_NAME", SystemTable.createNameType())
                        .column("COLUMN_NAME", SystemTable.createNameType())

                        .column("USAGE", Type.STRING, "use case of this column stats")
                        .column("LAST_USED", Type.DATETIME, "last time when using this column stats")

                        .column("CREATED", Type.DATETIME, "create time of this column stats")
                        .build(),
                TSchemaTableType.SCH_COLUMN_STATS_USAGE);
    }

    public static SystemTable create() {
        return new ColumnStatsUsageSystemTable();
    }

    @Override
    public boolean supportFeEvaluation() {
        return FeConstants.runningUnitTest;
    }

    @Override
    public List<List<ScalarOperator>> evaluate(ScalarOperator predicate) {
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        TableName tableName = new TableName();
        for (ScalarOperator conjunct : conjuncts) {
            BinaryPredicateOperator binary =
                    (com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator) conjunct;
            ColumnRefOperator columnRef = binary.getChild(0).cast();
            String name = columnRef.getName();
            ConstantOperator value = binary.getChild(1).cast();
            switch (name.toUpperCase()) {
                case "TABLE_CATALOG":
                    tableName.setCatalog(value.toString());
                case "TABLE_DATABASE":
                    tableName.setDb(value.toString());
                case "TABLE_NAME":
                    tableName.setTbl(value.toString());
                    break;
                default:
                    throw new NotImplementedException("not supported");
            }
        }

        return PredicateColumnsMgr.getInstance().query(tableName)
                .stream()
                .map(ColumnStatsUsageSystemTable::columnUsageToScalar)
                .collect(Collectors.toList());
    }

    public static TColumnStatsUsageRes query(TColumnStatsUsageReq req) {
        TableName tableName = new TableName(req.getTable_catalog(), req.getTable_database(), req.getTable_name());
        List<ColumnUsage> columnStatsUsages = PredicateColumnsMgr.getInstance().query(tableName);
        TColumnStatsUsageRes res = new TColumnStatsUsageRes();
        res.setItems(columnStatsUsages.stream()
                .map(ColumnStatsUsageSystemTable::columnUsageToThrift)
                .collect(Collectors.toList()));
        return res;
    }

    private static List<ScalarOperator> columnUsageToScalar(ColumnUsage columnUsage) {
        ColumnFullId columnFullId = columnUsage.getColumnFullId();
        Optional<Pair<TableName, ColumnId>> names = columnFullId.toNames();
        List<ScalarOperator> result = Lists.newArrayList();
        result.add(ConstantOperator.createVarchar(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME));
        result.add(ConstantOperator.createVarchar(names.map(x -> x.first.getDb()).orElse(null)));
        result.add(ConstantOperator.createVarchar(names.map(x -> x.first.getTbl()).orElse(null)));
        result.add(ConstantOperator.createVarchar(names.map(x -> x.second.getId()).orElse(null)));
        result.add(ConstantOperator.createVarchar(columnUsage.getUseCaseString()));
        result.add(ConstantOperator.createDatetime(columnUsage.getLastUsed()));
        result.add(ConstantOperator.createDatetime(columnUsage.getCreated()));
        return result;
    }

    private static TColumnStatsUsage columnUsageToThrift(ColumnUsage columnUsage) {
        TColumnStatsUsage thriftUsage = new TColumnStatsUsage();
        ColumnFullId columnFullId = columnUsage.getColumnFullId();
        Optional<Pair<TableName, ColumnId>> names = columnFullId.toNames();
        thriftUsage.setTable_catalog(
                names.map(x -> x.first.getCatalog()).orElse(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME));
        thriftUsage.setTable_database(names.map(x -> x.first.getDb()).orElse(null));
        thriftUsage.setTable_name(names.map(x -> x.first.getTbl()).orElse(null));
        thriftUsage.setColumn_name(names.map(x -> x.second.getId()).orElse(null));
        thriftUsage.setUsage(columnUsage.getUseCaseString());
        thriftUsage.setLast_used(TimeUtils.toEpochSeconds(columnUsage.getLastUsed()));
        thriftUsage.setCreated((TimeUtils.toEpochSeconds(columnUsage.getCreated())));
        return thriftUsage;
    }


}
